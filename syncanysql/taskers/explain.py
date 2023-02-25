# -*- coding: utf-8 -*-
# 2023/2/14
# create by: snower

import copy
import datetime
from syncany.main import beautify_print

class ExplainTasker(object):
    def __init__(self, tasker):
        self.config = None
        self.tasker = tasker

    def start(self, executor, session_config, manager, arguments):
        self.config = copy.deepcopy(self.tasker.config)
        beautify_print("%s tasker %s compiled config:" % (datetime.datetime.now(), self.config["name"]))
        beautify_print(self.config)
        print()

        for key in list(arguments.keys()):
            if key.endswith("@limit"):
                arguments[key] = 1
            elif key.endswith("@verbose"):
                arguments[key] = True
        self.tasker.config["output"] = "&.-.&1::" + self.tasker.config["output"].split("::")[-1].split(" ")[0]
        self.tasker.config["name"] = self.tasker.config["name"] + "#explain"
        self.tasker.start(executor, session_config, manager, arguments)
        return [self]

    def run(self, executor, session_config, manager):
        try:
            return self.tasker.run(executor, session_config, manager)
        finally:
            self.config, self.tasker = None, None

    def terminate(self):
        if self.tasker:
            return self.tasker.terminate()