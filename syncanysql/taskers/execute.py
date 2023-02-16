# -*- coding: utf-8 -*-
# 2023/2/15
# create by: snower

import time
from syncany.logger import get_logger
from ..parser import FileParser

class ExecuteTasker(object):
    def __init__(self, config):
        self.config = config

    def run(self, executor, session_config, manager, arguments):
        start_time = time.time()
        get_logger().info("execute file %s", self.config["filename"])
        try:
            file_parser = FileParser(self.config["filename"])
            sqls = file_parser.load()
            return executor.run(self.config["filename"], sqls)
        finally:
            get_logger().info("execute file %s finish %.2fms", self.config["filename"], (time.time() - start_time) * 1000)

    def terminate(self):
        pass