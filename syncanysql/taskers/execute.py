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

    def start(self, name, executor, session_config, manager, arguments):
        if self.config["filename"].endswith("sql") or self.config["filename"].endswith("sqlx"):
            if self.executor != executor:
                from ..executor import Executor
                self.executor = Executor(manager, session_config.session(), executor)

            start_time = time.time()
            get_logger().info("execute file %s", self.config["filename"])
            try:
                file_parser = FileParser(self.config["filename"])
                sqls = file_parser.load()
                with self.executor as executor:
                    executor.run(self.config["filename"], sqls)
            finally:
                session_config.merge()
                get_logger().info("execute file %s finish %.2fms", self.config["filename"], (time.time() - start_time) * 1000)
        else:
            os.system(self.config["filename"])
        return [self]

    def run(self, executor, session_config, manager):
        if not self.executor:
            return
        with self.executor as executor:
            return executor.execute()

    def terminate(self):
        if not self.executor:
            return
        self.executor.terminate()
        self.executor = None
