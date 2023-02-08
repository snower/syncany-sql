# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os
import traceback
from syncany.logger import get_logger
from syncany.filters.filter import Filter
from syncany.taskers.core import CoreTasker
from syncany.main import show_tasker, show_dependency_tasker, compile_dependency, run_dependency, warp_database_logging
from .compiler import Compiler

class Tasker(object):
    def __init__(self, manager, config):
        self.manager = manager
        self.config = config

    def run(self, name, sqls):
        for sql in sqls:
            self.run_one(name, sql)

    def run_one(self, name, sql):
        compiler = Compiler(sql)
        compile_config, compile_arguments = compiler.compile()
        config = {}
        config.update(self.config.get())
        config.update(compile_config)
        config["name"] = name

        tasker = CoreTasker(config, self.manager)
        arguments = tasker.load()
        tasker.config_logging()

        dependency_taskers = []
        for filename in tasker.get_dependencys():
            if isinstance(filename, list) and len(filename) == 2:
                filename, dependency_arguments = filename[0], (filename[1] if isinstance(filename[1], dict) else {})
            else:
                dependency_arguments = {}
            dependency_taskers.append(self.load_core_task_dependency(tasker, filename, dependency_arguments))

        run_arguments = {argument["name"]: argument["type"].filter(argument["default"]) if "type" in argument and isinstance(argument["type"], Filter) else argument["default"]
                         for argument in arguments if "default" in argument}
        run_arguments.update(compile_arguments)
        return self.run_core_task(run_arguments, arguments, tasker, dependency_taskers)

    def terminate(self):
        pass

    def load_core_task_dependency(self, parent, filename, parent_arguments):
        tasker = CoreTasker(filename, parent.manager, parent)
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