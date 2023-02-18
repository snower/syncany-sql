# -*- coding: utf-8 -*-
# 2023/2/15
# create by: snower

from .query import QueryTasker


class DeleteTasker(object):
    def __init__(self, config):
        self.tasker = QueryTasker(config)

    def start(self, executor, session_config, manager, arguments):
        self.tasker.config["name"] = self.tasker.config["name"] + "#delete"
        return self.tasker.start(executor, session_config, manager, arguments)

    def run(self, executor, session_config, manager):
        try:
            return self.tasker.run(executor, session_config, manager)
        finally:
            self.tasker = None

    def terminate(self):
        if self.tasker:
            self.tasker.terminate()