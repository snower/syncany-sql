# -*- coding: utf-8 -*-
# 2023/2/14
# create by: snower

import datetime
from syncany.main import beautify_print

class ExplainTasker(object):
    def __init__(self, tasker):
        self.tasker = tasker

    def run(self, session_config, manager, arguments):
        for key in list(arguments.keys()):
            if key.endswith("@limit"):
                arguments[key] = 1
            elif key.endswith("@verbose"):
                arguments[key] = True
        beautify_print("%s tasker %s compiled config:" % (datetime.datetime.now(), self.tasker.config["name"]))
        beautify_print(self.tasker.config)
        print()
        return self.tasker.run(session_config, manager, arguments)

    def terminate(self):
        return self.tasker.terminate()