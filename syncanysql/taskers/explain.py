# -*- coding: utf-8 -*-
# 2023/2/14
# create by: snower

import copy
import datetime
from syncany.main import beautify_print


class ExplainTasker(object):
    def __init__(self, tasker, sql=None):
        self.config = None
        self.tasker = tasker
        self.sql = sql

    def start(self, name, executor, session_config, manager, arguments):
        self.config = copy.deepcopy(self.tasker.config)
        if self.sql:
            beautify_print("%s tasker %s execute sql:\n%s" % (datetime.datetime.now(), self.config["name"], self.sql))
            print()
        beautify_print("%s tasker %s compiled config:" % (datetime.datetime.now(), self.config["name"]))
        beautify_print(self.config)
        print()

        for key in list(arguments.keys()):
            if key.endswith("@verbose"):
                arguments[key] = True
        self.tasker.start(name, executor, session_config, manager, arguments)
        return [self]

    def run(self, executor, session_config, manager):
        try:
            return self.tasker.run(executor, session_config, manager)
        finally:
            self.config, self.tasker = None, None

    def terminate(self):
        if not self.tasker:
            return
        self.tasker.terminate()
        self.tasker = None
