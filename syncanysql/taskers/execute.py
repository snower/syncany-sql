# -*- coding: utf-8 -*-
# 2023/2/15
# create by: snower

import os
import time
from syncany.logger import get_logger
from ..parser import FileParser

class ExecuteTasker(object):
    def __init__(self, config):
        self.config = config
        self.executor = None

    def start(self, executor, session_config, manager, arguments):
        if self.executor != executor:
            from ..executor import Executor
            self.executor = Executor(manager, session_config.session(), executor)

        if self.config["filename"].endswith("sql") or self.config["filename"].endswith("sqlx"):
            start_time = time.time()
            get_logger().info("execute file %s", self.config["filename"])
            try:
                file_parser = FileParser(self.config["filename"])
                sqls = file_parser.load()
                self.executor.run(self.config["filename"], sqls)
            finally:
                session_config.merge()
                get_logger().info("execute file %s finish %.2fms", self.config["filename"], (time.time() - start_time) * 1000)
        else:
            os.system(self.config["filename"])
        return []

    def run(self, executor, session_config, manager):
        return self.executor.execute()

    def terminate(self):
        if not self.executor:
            self.executor.terminate()