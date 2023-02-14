# -*- coding: utf-8 -*-
# 2023/2/13
# create by: snower

import copy
from .compiler import Compiler

class Executor(object):
    def __init__(self, manager, session_config):
        self.manager = manager
        self.session_config = session_config
        self.tasker = None

    def run(self, name, sqls):
        for sql in sqls:
            exit_code = self.run_one(name, sql)
            if exit_code is not None and exit_code != 0:
                return exit_code
        return 0

    def run_one(self, name, sql):
        config = copy.deepcopy(self.session_config.get())
        config["name"] = name
        compiler = Compiler(config)
        arguments = {"@verbose": config.get("@verbose", False), "@timeout": config.get("@timeout", 0), "@limit": config.get("@limit", 100)}
        self.tasker = compiler.compile(sql, arguments)
        return self.tasker.run(self.session_config, self.manager, arguments)

    def terminate(self):
        if not self.tasker:
            return
        self.tasker.terminate()