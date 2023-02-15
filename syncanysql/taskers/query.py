# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os
import traceback
from syncany.logger import get_logger
from syncany.filters.filter import Filter
from syncany.taskers.core import CoreTasker
from syncany.main import show_tasker, show_dependency_tasker, compile_dependency, run_dependency, warp_database_logging

class QueryTasker(object):
    def __init__(self, config):
        self.config = config

    def run(self, executor, session_config, manager, arguments):
        tasker = CoreTasker(self.config, manager)
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
        return self.run_core_task(run_arguments, tasker_arguments, tasker, dependency_taskers)

    def terminate(self):
        pass

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

    def run_core_task(self, run_arguments, arguments, tasker, dependency_taskers):
        try:
            arguments = {key.lower(): value for key, value in os.environ.items()}
            arguments.update(run_arguments)

            tasker.compile(arguments)
            for dependency_tasker in dependency_taskers:
                compile_dependency(arguments, *dependency_tasker)

            if "@show" in arguments and arguments["@show"]:
                for dependency_tasker in dependency_taskers:
                    show_dependency_tasker(*dependency_tasker)
                show_tasker(tasker)
                return 0
            if "@verbose" in arguments and arguments["@verbose"]:
                warp_database_logging(tasker)

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
        return 0