# -*- coding: utf-8 -*-
# 2023/2/15
# create by: snower

import time
import traceback
from syncany.logger import get_logger
from syncany.filters.filter import Filter
from syncany.taskers.core import CoreTasker
from syncany.main import show_tasker, warp_database_logging

class DeleteTasker(object):
    def __init__(self, config):
        self.config = config

    def run(self, executor, session_config, manager, arguments):
        tasker = CoreTasker(self.config, manager)
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

        try:
            tasker.compile(run_arguments)
            if "@show" in run_arguments and run_arguments["@show"]:
                show_tasker(tasker)
                return 0
            if "@verbose" in run_arguments and run_arguments["@verbose"]:
                warp_database_logging(tasker)
            get_logger().info("%s start %s -> %s", tasker.name, tasker.config_filename, tasker.config.get("name"))
            tasker.outputer.store([])
            tasker.print_stored_statistics(tasker.outputer, tasker.status["statistics"]["outputer"])
            tasker.status["execute_time"] = (time.time() - tasker.status.start_time) * 1000
            get_logger().info("%s finish %s %s %.2fms", tasker.name, tasker.config_filename, tasker.config.get("name"),
                              tasker.status["execute_time"])
        except SystemError:
            tasker.close(False, "signal terminaled")
            get_logger().error("signal exited")
            return 130
        except KeyboardInterrupt:
            tasker.close(False, "user terminaled")
            get_logger().error("Crtl+C exited")
            return 130
        except Exception as e:
            if "@show" in run_arguments and run_arguments["@show"]:
                show_tasker(tasker)
            else:
                tasker.close(False, "Error: " + repr(e), traceback.format_exc())
            get_logger().error("%s\n%s", e, traceback.format_exc())
            return 1
        else:
            if "@show" in run_arguments and run_arguments["@show"]:
                return 0
            tasker.close()
        return 0

    def terminate(self):
        pass