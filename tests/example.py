# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

import sys
import os
from unittest import TestCase
from syncanysql import ScriptEngine, Executor
from syncany.database.memory import MemoryDBCollection, MemoryDBFactory
from syncanysql.parser import FileParser


class ExampleTestCase(TestCase):
    script_engine = None
    example_name = None
    execute_results = {}

    def get_executor(self):
        if self.script_engine is None:
            self.__class__.script_engine = ScriptEngine()
        if self.script_engine.executor is None:
            self.script_engine.setup()
        return Executor(self.script_engine.manager, self.script_engine.executor.session_config.session(),
                        self.script_engine.executor)


    def execute(self, filename):
        fileParser = FileParser(os.path.join("examples", self.example_name, filename))
        sqls = fileParser.load()
        for sql in sqls:
            if sql.sql[:6].lower() != "select" or "into" in sql.sql:
                continue
            sql.sql = "INSERT INTO `__test__%s_%s` %s" % (self.__class__.__name__, sql.lineno, sql.sql)

        cwd = os.getcwd()
        os.chdir(os.path.join("examples", self.example_name))
        sys.path.insert(0, os.path.abspath(os.getcwd()))
        try:
            with self.get_executor() as executor:
                executor.run("execute[%s]" % self.__class__.__name__, sqls)
                executor.execute()

            self.execute_results = {}
            for config_key, factory in self.script_engine.manager.database_manager.factorys.items():
                if not isinstance(factory, MemoryDBFactory):
                    continue
                for driver in factory.drivers:
                    if not isinstance(driver.instance, MemoryDBCollection):
                        continue
                    for key in list(driver.instance.keys()):
                        if key.startswith("--.__test__"):
                            self.execute_results[key[3:]] = driver.instance[key]
                        driver.instance.remove(key)
            self.script_engine.manager.database_manager.states.clear()
        finally:
            sys.path.pop(0)
            os.chdir(cwd)

    def assert_data(self, lineno, checker, error_msg):
        execute_result = self.execute_results.get("__test__%s_%s" % (self.__class__.__name__, lineno))
        assert execute_result is not None, error_msg
        if callable(checker):
            self.assertTrue(checker(execute_result), error_msg)
        else:
            self.assertEqual(execute_result, checker, error_msg)

    def assert_value(self, lineno, key, checker, error_msg, index=0):
        execute_result = self.execute_results.get("__test__%s_%s" % (self.__class__.__name__, lineno))
        assert execute_result is not None, error_msg
        assert isinstance(execute_result, list) and len(execute_result) > index, error_msg
        assert key in execute_result[index], error_msg
        if callable(checker):
            self.assertTrue(checker(execute_result[index][key]), error_msg)
        else:
            self.assertEqual(execute_result[index][key], checker, error_msg)
