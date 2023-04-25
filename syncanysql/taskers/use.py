# -*- coding: utf-8 -*-
# 2023/2/25
# create by: snower

import os


class UseCommandTasker(object):
    def __init__(self, config):
        self.config = config

    def start(self, name, executor, session_config, manager, arguments):
        use_info = [s.strip() for s in self.config["use"].split(" as ")]
        if not use_info[0] or isinstance(use_info[0], (bool, int, float, list, tuple, set, dict)):
            return []
        if isinstance(use_info[0], str):
            __import__(use_info[0], globals(), locals(), [use_info[0].rpartition(".")[-1]])
        if len(use_info) >= 2:
            if os.path.exists(use_info[0]):
                session_config.set("sources." + use_info[1], use_info[0])
            else:
                session_config.set("imports." + use_info[1], use_info[0])
        else:
            if os.path.exists(use_info[0]):
                session_config.set("sources." + use_info[0].split(".")[-1], use_info[0])
            else:
                session_config.set("imports." + use_info[0].split(".")[-1], use_info[0])
        return []

    def run(self, executor, session_config, manager):
        return 0

    def terminate(self):
        pass
