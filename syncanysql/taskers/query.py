# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os
import copy
import traceback
import uuid
from syncany.logger import get_logger
from syncany.filters.filter import Filter
from syncany.taskers.core import CoreTasker
from syncany.hook import Hooker
from syncany.taskers.tasker import _thread_local
from syncany.main import TaskerYieldNext, warp_database_logging


class ReduceHooker(Hooker):
    def __init__(self, executor, session_config, manager, arguments, tasker, config, batch, aggregate):
        self.executor = executor
        self.session_config = session_config
        self.manager = manager
        self.arguments = arguments
        self.tasker = tasker
        self.config = config
        self.batch = batch
        self.aggregate = aggregate
        self.count = 0

    def outputed(self, tasker, datas):
        self.count += len(datas)
        if self.count >= self.batch:
            self.count = 0
            self.tasker.run_reduce(self.executor, self.session_config, self.manager, self.arguments, False)

    def finaled(self, tasker, e=None):
        if e is not None:
            return
        self.tasker.run_reduce(self.executor, self.session_config, self.manager, self.arguments, False)


class QueryTasker(object):
    def __init__(self, config):
        self.config = config
        self.reduce_config = None
        self.tasker = None
        self.dependency_taskers = []
        self.arguments = None
        self.tasker_generator = None

    def start(self, executor, session_config, manager, arguments):
        dependency_taskers = []
        for dependency_config in self.config.get("dependencys", []):
            kn, knl = (dependency_config["name"] + "@"), len(dependency_config["name"] + "@")
            dependency_arguments = {}
            for key, value in arguments.items():
                if key[:knl] != kn:
                    continue
                dependency_arguments[key[knl:]] = value
                dependency_arguments.pop(key, None)
            dependency_tasker = QueryTasker(dependency_config)
            dependency_tasker.start(executor, session_config, manager, dependency_arguments)
            dependency_taskers.append(dependency_tasker)

        limit, batch, aggregate = int(arguments.get("@limit", 0)), int(arguments.get("@batch", 0)), self.config.pop("aggregate", None)
        require_reduce, reduce_intercept = False, False
        if self.config.get("intercepts"):
            if aggregate and aggregate.get("reduces") and aggregate.get("having_columns"):
                if [having_column for having_column in aggregate["having_columns"] if having_column in aggregate["reduces"]]:
                    require_reduce, reduce_intercept = True, True
            if (batch > 0 or limit > 0):
                require_reduce = True
        if (aggregate and aggregate.get("key") and aggregate.get("reduces")) and (batch > 0 or limit > 0):
            require_reduce = True
        if require_reduce and not arguments.get("@streaming"):
            if limit > 0 and batch <= 0:
                batch = max(*(int(arguments.get(key, 0)) for key in ("@limit", "@batch", "@join_batch", "@insert_batch")))
                arguments["@batch"] = batch
            if not aggregate:
                aggregate = {"key": "", "reduces": [], "having_columns": set([])}
            self.compile_reduce_config(aggregate)
            if reduce_intercept:
                self.reduce_config["intercepts"] = self.config.pop("intercepts")
        elif 0 < limit < batch:
            arguments["@batch"] = limit
        tasker = CoreTasker(self.config, manager)
        if "#" not in tasker.config["name"]:
            tasker.config["name"] = tasker.config["name"] + "#select"
        if self.reduce_config and "_aggregate_key_" in self.reduce_config["schema"]:
            tasker.add_hooker(ReduceHooker(executor, session_config, manager, arguments,
                                           self, copy.deepcopy(self.config), batch, aggregate))
        arguments = self.compile_tasker(arguments, tasker)
        self.tasker, self.dependency_taskers, self.arguments = tasker, dependency_taskers, arguments
        return [self]

    def run(self, executor, session_config, manager):
        if not self.tasker_generator:
            self.tasker_generator = self.run_tasker(executor, session_config, manager, self.tasker, self.dependency_taskers)
        else:
            _thread_local.current_tasker = self.tasker
        while True:
            try:
                value = self.tasker_generator.send(None)
                if isinstance(value, TaskerYieldNext):
                    executor.add_runner(self)
                    return 0
            except StopIteration as e:
                exit_code = e.value
                if exit_code is not None and exit_code != 0:
                    return exit_code
                break
        if self.reduce_config:
            return self.run_reduce(executor, session_config, manager, self.arguments, True)
        return exit_code

    def run_yield(self, executor, session_config, manager, tasker, dependency_taskers):
        tasker_generator = tasker.run_yield()
        dependency_tasker_generators = [(dependency_tasker, self.run_tasker(executor, session_config, manager,
                                                                            dependency_tasker.tasker, dependency_tasker.dependency_taskers))
                                        for dependency_tasker in dependency_taskers]
        while True:
            for dependency_tasker, dependency_tasker_generator in tuple(dependency_tasker_generators):
                try:
                    _thread_local.current_tasker = dependency_tasker.tasker
                    dependency_tasker_generator.send(None)
                except StopIteration as e:
                    exit_code = e.value
                    if exit_code is not None and exit_code != 0:
                        return exit_code
                    if dependency_tasker.reduce_config:
                        dependency_tasker.run_reduce(executor, session_config, manager, dependency_tasker.arguments, True)
                    dependency_tasker_generators.remove((dependency_tasker, dependency_tasker_generator))

            try:
                _thread_local.current_tasker = self.tasker
                tasker_generator.send(None)
            except StopIteration:
                break
            yield TaskerYieldNext()

    def terminate(self):
        if self.tasker:
            self.tasker.terminate()

    def compile_reduce_config(self, aggregate):
        subquery_name = "__subquery_" + str(uuid.uuid1().int) + "_reduce"
        config = copy.deepcopy(self.config)
        config.update({
            "input": "&.--." + subquery_name + "::" + self.config["output"].split("::")[-1].split(" ")[0],
            "output": self.config["output"],
            "querys": [],
            "caches": [],
            "imports": {},
            "sources": {},
            "defines": {},
            "variables": {},
            "intercepts": [],
            "schema": {},
            "orders": [],
            "pipelines": [],
            "options": {},
            "dependencys": [],
            "states": [],
        })
        for key, column in self.config["schema"].items():
            if key in aggregate["reduces"]:
                config["schema"][key] = ["#aggregate", "$._aggregate_key_", aggregate["reduces"][key]]
            else:
                config["schema"][key] = "$." + key
        if aggregate["key"]:
            config["schema"]["_aggregate_key_"] = "$._aggregate_key_"
            self.config["schema"]["_aggregate_key_"] = aggregate["key"]
        self.config["output"] = "&.--." + subquery_name + "::" + self.config["output"].split("::")[-1].split(" ")[0] + " use I"
        self.reduce_config = config

    def run_reduce(self, executor, session_config, manager, arguments, final_reduce=False):
        config, arguments = copy.deepcopy(self.reduce_config), copy.deepcopy(arguments)
        if final_reduce:
            config["schema"].pop("_aggregate_key_", None)
            for key, column in config["schema"].items():
                config["schema"][key] = "$." + key
            config["name"] = config["name"] + "#select@final_reduce"
        else:
            config["output"] = config["input"] + " use DI"
            config["name"] = config["name"] + "#select@reduce"
            config["intercepts"] = []
        tasker = CoreTasker(config, manager)
        arguments["@primary_order"] = False
        arguments["@batch"] = 0
        arguments["@limit"] = 0
        self.compile_tasker(arguments, tasker)
        tasker_generator = self.run_tasker(executor, session_config, manager, tasker, [])
        while True:
            try:
                tasker_generator.send(None)
            except StopIteration as e:
                exit_code = e.value
                if exit_code is not None and exit_code != 0:
                    return exit_code
                break
        return 0

    def compile_tasker(self, arguments, tasker):
        tasker_arguments = tasker.load()

        compile_arguments = {}
        for argument in tasker_arguments:
            if "default" not in argument:
                continue
            if "type" not in argument or not isinstance(argument["type"], Filter):
                compile_arguments[argument["name"]] = argument["default"]
                continue
            if "nargs" in argument and "action" in argument and isinstance(argument["default"], list):
                compile_arguments[argument["name"]] = [argument["type"].filter(v) for v in argument["default"]]
            else:
                compile_arguments[argument["name"]] = argument["type"].filter(argument["default"])
        compile_arguments.update({key.lower(): value for key, value in os.environ.items()})
        compile_arguments.update(arguments)

        tasker.compile(compile_arguments)
        if "@verbose" in compile_arguments and compile_arguments["@verbose"]:
            warp_database_logging(tasker)
        return compile_arguments

    def run_tasker(self, executor, session_config, manager, tasker, dependency_taskers):
        try:
            tasker_generator = self.run_yield(executor, session_config, manager, tasker, dependency_taskers)
            while True:
                try:
                    value = tasker_generator.send(None)
                    if isinstance(value, TaskerYieldNext):
                        yield value
                except StopIteration as e:
                    exit_code = e.value
                    if exit_code is not None and exit_code != 0:
                        return exit_code
                    break
        except SystemError:
            tasker.close(False, "signal terminaled")
            get_logger().error("signal exited")
            return 130
        except KeyboardInterrupt:
            tasker.close(False, "user terminaled")
            get_logger().error("Crtl+C exited")
            return 130
        except Exception as e:
            tasker.close(False, "Error: " + repr(e), traceback.format_exc())
            get_logger().error("tasker %s error: %s\n%s", tasker.name, e, traceback.format_exc())
            return 1
        else:
            tasker.close()
        return 0
