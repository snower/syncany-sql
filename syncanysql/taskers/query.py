# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os
import copy
import traceback
from syncany.logger import get_logger
from syncany.filters.filter import Filter
from syncany.taskers.core import CoreTasker
from syncany.hook import Hooker
from syncany.main import show_tasker, show_dependency_tasker, compile_dependency, run_dependency, warp_database_logging


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
            self.tasker.run_aggregate_reduce(self.executor, self.session_config,
                                             self.manager, self.arguments, False)

    def finaled(self, tasker, e=None):
        self.tasker.run_aggregate_reduce(self.executor, self.session_config,
                                         self.manager, self.arguments, False)


class QueryTasker(object):
    def __init__(self, config):
        self.config = config
        self.aggregate_config = None
        self.tasker = None
        self.dependency_taskers = None
        self.arguments = None

    def start(self, executor, session_config, manager, arguments):
        batch, aggregate = int(self.config.get("@batch", 0)), self.config.pop("aggregate", None)
        if aggregate and aggregate["key"] and aggregate["reduces"] and batch > 0:
            self.compile_aggregate_config(aggregate)
        tasker = CoreTasker(self.config, manager)
        if self.aggregate_config:
            tasker.add_hooker(ReduceHooker(executor, session_config, manager, arguments,
                                           self, copy.deepcopy(self.config), batch, aggregate))
        tasker_arguments = tasker.load()

        dependency_taskers = []
        for filename in tasker.get_dependencys():
            if isinstance(filename, list) and len(filename) == 2:
                filename, dependency_arguments = filename[0], (filename[1] if isinstance(filename[1], dict) else {})
            else:
                dependency_arguments = {}
            dependency_taskers.append(self.load_core_task_dependency(tasker, filename, dependency_arguments))

        run_arguments = {}
        for argument in tasker_arguments:
            if "default" not in argument:
                continue
            if "type" not in argument or not isinstance(argument["type"], Filter):
                run_arguments[argument["name"]] = argument["default"]
                continue
            if "nargs" in argument and "action" in argument and isinstance(argument["default"], list):
                run_arguments[argument["name"]] = [argument["type"].filter(v) for v in argument["default"]]
            else:
                run_arguments[argument["name"]] = argument["type"].filter(argument["default"])
        run_arguments.update(arguments)
        self.compile_core_task(run_arguments, tasker, dependency_taskers)
        self.tasker, self.dependency_taskers, self.arguments = tasker, dependency_taskers, run_arguments
        return [self]

    def run(self, executor, session_config, manager):
        exit_code = self.run_core_task(self.arguments, self.tasker, self.dependency_taskers)
        if exit_code is not None and exit_code != 0:
            return exit_code
        if self.aggregate_config:
            return self.run_aggregate_reduce(executor, session_config, manager, self.arguments, True)
        return exit_code

    def terminate(self):
        if self.tasker:
            self.tasker.terminate()

    def compile_aggregate_config(self, aggregate):
        subquery_name = "__subquery_" + str(id(self)) + "_reduce"
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
        config["schema"]["_aggregate_key_"] = "$._aggregate_key_"
        config["name"] = self.config["name"] + "#reduce"
        self.config["schema"]["_aggregate_key_"] = aggregate["key"]
        self.config["output"] = "&.--." + subquery_name + "::" + self.config["output"].split("::")[-1].split(" ")[0] + " use I"
        self.aggregate_config = config

    def run_aggregate_reduce(self, executor, session_config, manager, arguments, final_reduce=False):
        config = copy.deepcopy(self.aggregate_config)
        if final_reduce:
            config["schema"].pop("_aggregate_key_", None)
            for key, column in config["schema"].items():
                config["schema"][key] = "$." + key
            config["name"] = config["name"] + "_final"
        else:
            config["output"] = config["input"] + " use DI"
        tasker = CoreTasker(config, manager)
        tasker_arguments = tasker.load()
        run_arguments = {}
        for argument in tasker_arguments:
            if "default" not in argument:
                continue
            if "type" not in argument or not isinstance(argument["type"], Filter):
                run_arguments[argument["name"]] = argument["default"]
                continue
            if "nargs" in argument and "action" in argument and isinstance(argument["default"], list):
                run_arguments[argument["name"]] = [argument["type"].filter(v) for v in argument["default"]]
            else:
                run_arguments[argument["name"]] = argument["type"].filter(argument["default"])
        run_arguments.update(arguments)
        run_arguments["@batch"] = 0
        run_arguments["@limit"] = 0
        self.compile_core_task(run_arguments, tasker, [])
        return self.run_core_task(run_arguments, tasker, [])

    def load_core_task_dependency(self, parent, filename, parent_arguments):
        tasker = CoreTasker(filename, parent.manager, parent)
        arguments = tasker.load()
        for argument in arguments:
            if "default" not in argument:
                continue
            if "type" not in argument or not isinstance(argument["type"], Filter):
                parent_arguments[argument["name"]] = argument["default"]
                continue
            if "nargs" in argument and "action" in argument and isinstance(argument["default"], list):
                parent_arguments[argument["name"]] = [argument["type"].filter(v) for v in argument["default"]]
            else:
                parent_arguments[argument["name"]] = argument["type"].filter(argument["default"])
        setattr(tasker, "parent_arguments", parent_arguments)

        dependency_taskers = []
        for filename in tasker.get_dependencys():
            if isinstance(filename, list) and len(filename) == 2:
                filename, dependency_arguments = filename[0], (filename[1] if isinstance(filename[1], dict) else {})
            else:
                dependency_arguments = {}
            dependency_taskers.append(self.load_core_task_dependency(tasker, filename, dependency_arguments))
        return (tasker, dependency_taskers)

    def compile_core_task(self, arguments, tasker, dependency_taskers):
        self.tasker = tasker
        tasker_arguments = {key.lower(): value for key, value in os.environ.items()}
        tasker_arguments.update(arguments)

        tasker.compile(tasker_arguments)
        for dependency_tasker in dependency_taskers:
            compile_dependency(tasker_arguments, *dependency_tasker)

        if "@show" in tasker_arguments and tasker_arguments["@show"]:
            for dependency_tasker in dependency_taskers:
                show_dependency_tasker(*dependency_tasker)
            show_tasker(tasker)
            return 0
        if "@verbose" in tasker_arguments and tasker_arguments["@verbose"]:
            warp_database_logging(tasker)

    def run_core_task(self, arguments, tasker, dependency_taskers):
        self.tasker = tasker
        try:
            for dependency_tasker in dependency_taskers:
                run_dependency(*dependency_tasker)
            tasker.run()
        except SystemError:
            tasker.close(False, "signal terminaled")
            get_logger().error("signal exited")
            return 130
        except KeyboardInterrupt:
            tasker.close(False, "user terminaled")
            get_logger().error("Crtl+C exited")
            return 130
        except Exception as e:
            if "@show" in arguments and arguments["@show"]:
                for dependency_tasker in dependency_taskers:
                    show_dependency_tasker(*dependency_tasker)
                show_tasker(tasker)
            else:
                tasker.close(False, "Error: " + repr(e), traceback.format_exc())
            get_logger().error("%s\n%s", e, traceback.format_exc())
            return 1
        else:
            if "@show" in arguments and arguments["@show"]:
                return 0
            tasker.close()
        finally:
            self.tasker = None
        return 0