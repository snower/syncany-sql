# -*- coding: utf-8 -*-
# 2023/2/15
# create by: snower

from syncany.hook import Hooker
from .query import QueryTasker


class StreamingFollowHooker(Hooker):
    def __init__(self, manager, tasker):
        self.manager = manager
        self.tasker = tasker

    def loaded(self, tasker, datas):
        tasker.loader.set_streaming(False)
        return datas

    def outputed(self, tasker, datas):
        tasker.loader.set_streaming(True if tasker.outputer.is_streaming() else False)


class DeleteTasker(object):
    def __init__(self, config):
        self.tasker = QueryTasker(config)

    def start(self, executor, session_config, manager, arguments):
        arguments["@limit"] = 0
        arguments["@batch"] = 0
        self.tasker.config["name"] = self.tasker.config["name"] + "#delete"
        taskers = self.tasker.start(executor, session_config, manager, arguments)
        self.tasker.tasker.add_hooker(StreamingFollowHooker(manager, self))
        return taskers

    def run(self, executor, session_config, manager):
        try:
            return self.tasker.run(executor, session_config, manager)
        finally:
            self.tasker = None

    def terminate(self):
        if not self.tasker:
            return
        self.tasker.terminate()
        self.tasker = None
