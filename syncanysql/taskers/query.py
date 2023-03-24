# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os
import copy
import traceback
import uuid
from syncany.logger import get_logger
from syncany.taskers.core import CoreTasker
from syncany.hook import Hooker
from syncany.taskers.tasker import _thread_local
from syncany.main import TaskerYieldNext, warp_database_logging


DEAULT_AGGREGATE = {"key": None, "values": {}, "calculates": {}, "reduces": {}, "having_columns": set([]),
                    "distincts": [], "distinct_aggregates": set([])}


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
        if self.batch > 0 and self.count >= self.batch:
            self.count = 0
            self.tasker.run_reduce(self.executor, self.session_config, self.manager, self.arguments, False)
        else:
            limit = tasker.arguments["@limit"] if "@limit" in tasker.arguments and tasker.arguments["@limit"] > 0 else 0
            if 0 < limit <= tasker.status["store_count"]:
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
        self.updaters = []
        self.temporary_memory_collections = set([])

    def start(self, executor, session_config, manager, arguments):
        dependency_taskers, aggregate = [], self.config.pop("aggregate", None)
        if aggregate and aggregate.get("distincts"):
            self.config, distinct_config = self.compile_distinct_config(aggregate), self.config
            distinct_config["name"] = distinct_config["name"] + "#select@distinct"
            distinct_tasker = QueryTasker(distinct_config)
            distinct_tasker.start(executor, session_config, manager, copy.deepcopy(arguments))
            dependency_taskers.append(distinct_tasker)
            arguments["@limit"], arguments["@batch"] = 0, 0
            arguments["@primary_order"] = False

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

        limit, batch = int(arguments.get("@limit", 0)), int(arguments.get("@batch", 0))
        require_reduce, reduce_intercept, sorted_limit = False, False, len(self.config.get("pipelines", [])) == 2
        if self.config.get("intercepts"):
            if aggregate and aggregate.get("reduces") and aggregate.get("having_columns"):
                if [having_column for having_column in aggregate["having_columns"] if having_column in aggregate["reduces"]]:
                    require_reduce, reduce_intercept = True, True
            if (batch > 0 or limit > 0 or sorted_limit):
                require_reduce = True
        if (aggregate and aggregate.get("reduces")) and (batch > 0 or limit > 0 or sorted_limit):
            require_reduce = True
        if require_reduce and not arguments.get("@streaming"):
            if limit > 0 and batch <= 0:
                batch = max(*(int(arguments.get(key, 0)) for key in ("@limit", "@batch", "@join_batch", "@insert_batch")))
                arguments["@batch"] = batch
            if not aggregate:
                aggregate = copy.deepcopy(DEAULT_AGGREGATE)
            self.compile_reduce_config(aggregate)
            if reduce_intercept:
                self.reduce_config["intercepts"] = self.config.pop("intercepts", [])
            self.reduce_config["pipelines"] = self.config.pop("pipelines", [])
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
        try:
            self.execute_updater(executor, session_config, manager)
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
        finally:
            names = self.get_temporary_memory_collections()
            if names:
                executor.clear_temporary_memory_collection(names)

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
        if not self.tasker:
            return
        self.tasker.terminate()
        self.tasker = None

    def compile_distinct_config(self, aggregate):
        subquery_name = "__subquery_" + str(uuid.uuid1().int) + "_distinct"
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

        group_column = ["@aggregate_key"] if not aggregate["key"] else aggregate["key"][:]
        group_column.extend(aggregate["distincts"])
        distinct_aggregate = copy.deepcopy(DEAULT_AGGREGATE)
        for key, column in tuple(self.config["schema"].items()):
            if key in aggregate["distinct_aggregates"]:
                config["schema"][key] = ["#make", {
                    "key": "$._aggregate_distinct_key_",
                    "value": "$." + key
                }, aggregate["calculates"][key]]
                self.config["schema"][key] = aggregate["values"][key]
            elif key in aggregate["values"]:
                self.config["schema"][key] = ["#make", {
                    "key": group_column,
                    "value": aggregate["values"][key]
                }, aggregate["calculates"][key]]
                distinct_aggregate["values"][key] = aggregate["values"][key]
                distinct_aggregate["calculates"][key] = aggregate["calculates"][key]
                distinct_aggregate["reduces"][key] = aggregate["reduces"][key]
                config["schema"][key] = ["#aggregate", "$._aggregate_distinct_key_", aggregate["reduces"][key]]
            else:
                config["schema"][key] = "$." + key

        if aggregate["key"]:
            self.config["schema"]["_aggregate_distinct_key_"] = aggregate["key"]
        self.config["schema"]["_aggregate_distinct_aggregate_key_"] = ["#aggregate", group_column, ["#const", 0]]
        distinct_aggregate["key"] = group_column
        distinct_aggregate["values"]["_aggregate_distinct_aggregate_key_"] = ["#const", 0]
        distinct_aggregate["calculates"]["_aggregate_distinct_aggregate_key_"] = ["#const", 0]
        distinct_aggregate["reduces"]["_aggregate_distinct_aggregate_key_"] = ["#const", 0]
        self.config["aggregate"] = distinct_aggregate
        if [having_column for having_column in aggregate["having_columns"] if having_column in aggregate["reduces"]]:
            config["intercepts"] = self.config.pop("intercepts", [])
        else:
            distinct_aggregate["having_columns"] = aggregate["having_columns"]
        config["pipelines"] = self.config.pop("pipelines", [])
        self.config["output"] = "&.--." + subquery_name + "::" + self.config["output"].split("::")[-1].split(" ")[0] + " use I"
        return config

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
            config["pipelines"] = []
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
        if not final_reduce:
            self.tasker.status["store_count"] = tasker.status["store_count"]
        return 0

    def compile_tasker(self, arguments, tasker):
        tasker.load()
        compile_arguments = {}
        compile_arguments.update({key.lower(): value for key, value in os.environ.items()})
        compile_arguments.update(arguments)

        tasker.compile(compile_arguments)
        if "@verbose" in compile_arguments and compile_arguments["@verbose"]:
            warp_database_logging(tasker)

        if hasattr(tasker.outputer, "name"):
            if tasker.outputer.name.startswith("--.__subquery_") or tasker.outputer.name.startswith("--.__unionquery_"):
                self.temporary_memory_collections.add(tasker.outputer.name)

        loader_or_outputers = [tasker.loader, tasker.outputer] + list(tasker.join_loaders.values())
        for loader_or_outputer in loader_or_outputers:
            if not hasattr(loader_or_outputer, "name"):
                continue
            info = loader_or_outputer.name.split(".")
            if len(info) <= 1:
                continue
            db, table_name = info[0], info[1]
            if not table_name.startswith("#{env_variable__@") or table_name[-1] != "}":
                continue
            if not loader_or_outputer.name.startswith(db + "."):
                continue

            def env_variable_setter(obj, prefix, key):
                def _(executor, session_config, manager):
                    obj.name = prefix + str(executor.env_variables.get_value(key))
                return _
            self.updaters.append(env_variable_setter(loader_or_outputer, db + ".", table_name[16:-1]))
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

    def execute_updater(self, executor, session_config, manager):
        for updater in self.updaters:
            updater(executor, session_config, manager)

        for dependency_tasker in self.dependency_taskers:
            dependency_tasker.execute_updater(executor, session_config, manager)

    def get_temporary_memory_collections(self):
        names = list(self.temporary_memory_collections)
        for dependency_tasker in self.dependency_taskers:
            names.extend(dependency_tasker.get_temporary_memory_collections())
        return names
