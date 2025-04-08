# -*- coding: utf-8 -*-
# 2023/2/15
# create by: snower

import os
import time
import json
from syncany.logger import get_logger
from ..parser import FileParser


class ExecuteTasker(object):
    def __init__(self, config):
        self.config = config
        self.executor = None

    def start(self, name, executor, session_config, manager, arguments):
        start_time = time.time()
        try:
            if self.config["filename"].endswith(".sql") or self.config["filename"].endswith(".sqlx") or self.config["filename"].endswith(".prql"):
                if self.executor is None:
                    from ..executor import Executor
                    self.executor = Executor(manager, session_config.session(), executor)

                get_logger().info("execute file %s", self.config["filename"])
                try:
                    file_parser = FileParser(self.config["filename"])
                    sqls = file_parser.load()
                    with self.executor as executor:
                        executor.run(self.config["filename"], sqls)
                finally:
                    session_config.merged_config = None
            elif self.config["filename"].endswith(".json"):
                if self.executor is None:
                    from ..executor import Executor
                    self.executor = Executor(manager, session_config.session(), executor)

                from .query import QueryTasker
                with open(self.config["filename"], 'r', encoding=os.environ.get("SYNCANYENCODING", "utf-8")) as fp:
                    tasker = QueryTasker(json.loads(fp.read()))
                with self.executor as executor:
                    arguments = {"@verbose": arguments.get("@verbose", False), "@timeout": arguments.get("@timeout", 0),
                                 "@limit": self.executor.env_variables.get("@limit", 0), "@batch": self.executor.env_variables.get("@batch", 0),
                                 "@streaming": arguments.get("@streaming", False), "@recovery": arguments.get("@recovery", False),
                                 "@join_batch": arguments.get("@join_batch", 10000), "@insert_batch": arguments.get("@insert_batch", 0),
                                 "@use_input": self.executor.env_variables.get("@use_input", None), "@use_output": self.executor.env_variables.get("@use_output", None),
                                 "@use_output_type": self.executor.env_variables.get("@use_output_type", None), "@primary_order": False}
                    name = "execute_" + "".join([c if c.isalpha() or c.isdigit() else '_' for c in self.config["filename"]])
                    executor.runners.extend(tasker.start(name, executor, executor.session_config, executor.manager, arguments))
        finally:
            get_logger().info("execute %s start %.2fms", self.config["filename"], (time.time() - start_time) * 1000)
        return [self]

    def run(self, executor, session_config, manager):
        start_time = time.time()
        try:
            if not self.executor:
                try:
                    os.system(self.config["filename"])
                except Exception as e:
                    get_logger().warning("execute system command error: %s", str(e))
                return
            with self.executor as executor:
                return executor.execute()
        finally:
            get_logger().info("execute %s run %.2fms", self.config["filename"], (time.time() - start_time) * 1000)

    def terminate(self):
        if not self.executor:
            return
        self.executor.terminate()
        self.executor = None
