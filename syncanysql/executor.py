# -*- coding: utf-8 -*-
# 2023/2/13
# create by: snower

import os
import sys
import re
import copy
from syncany.database.memory import MemoryDBFactory, MemoryDBDriver
from .compiler import Compiler

ENV_VARIABLE_RE = re.compile("(\$\{\w+?(:.*?){0,1}\})", re.DOTALL | re.M)
RAW_SQL_RE = re.compile("(\(\s*?\/\*\s*?raw\(([\w\.]+?)\)\s*?\*\/(.*?)\/\*\s*?endraw\s*?\*\/\s*\))", re.DOTALL | re.M)

class Executor(object):
    def __init__(self, manager, session_config):
        self.manager = manager
        self.session_config = session_config
        self.tasker = None
        self.env_variables = None

    def parse_env_variables(self):
        self.env_variables = {}
        self.env_variables.update(os.environ)
        for arg in sys.argv:
            if arg[:2] != "--":
                continue
            arg_info = arg[2:].split("=")
            arg_value = arg_info[1] if len(arg_info) >= 2 else True
            if isinstance(arg_value, str) and arg_value and arg_value[0] in ('"', "'"):
                arg_value = arg_value[1:-1]
            self.env_variables[arg_info[0]] = arg_value

    def compile_env_variable(self, sql):
        if self.env_variables is None:
            self.parse_env_variables()
        variables = ENV_VARIABLE_RE.findall(sql)
        for variable, default_value in variables:
            variable_name = variable[2:-1].split(":")[0]
            variable_value = self.env_variables[variable_name] if variable_name in self.env_variables else default_value[1:]
            if isinstance(variable_value, str):
                variable_value = "true" if variable_value is True else ("false" if variable_value is False else
                                                                        ("null" if variable_value is None else str(variable_value)))
            sql = sql.replace(variable, variable_value)
        return sql

    def run(self, name, sqls):
        for sql in sqls:
            sql = self.compile_env_variable(sql)
            raw_sqls = RAW_SQL_RE.findall(sql)
            for raw, raw_name, raw_sql in raw_sqls:
                raw_name_info = raw_name.split(".")
                if len(raw_name_info) != 2:
                    continue
                raw_sql = "set @config.databases." + raw_name_info[0] + ".virtual_views." + raw_name_info[1] + "='''\n" + raw_sql.strip() + "\n'''"
                exit_code = self.run_one(name, raw_sql)
                if exit_code is not None and exit_code != 0:
                    return exit_code
                sql = sql.replace(raw, raw_name)
            exit_code = self.run_one(name, sql)
            if exit_code is not None and exit_code != 0:
                return exit_code
        return 0

    def run_one(self, name, sql):
        config = copy.deepcopy(self.session_config.get())
        config["name"] = name
        compiler = Compiler(config)
        arguments = {"@verbose": config.get("@verbose", False), "@timeout": config.get("@timeout", 0), "@limit": config.get("@limit", 100),
                     "@batch": config.get("@batch", 0), "@recovery": config.get("@recovery", False), "@join_batch": config.get("@join_batch", 1000),
                     "@insert_batch": config.get("@insert_batch", 0)}
        self.tasker = compiler.compile(sql, arguments)
        try:
            return self.tasker.run(self.session_config, self.manager, arguments)
        finally:
            for factory in self.manager.database_manager.factorys.values():
                if not isinstance(factory, MemoryDBFactory):
                    continue
                for driver in factory.drivers:
                    if not isinstance(driver.driver, MemoryDBDriver):
                        continue
                    for key in list(driver.driver.keys()):
                        if "__subquery_" in key or "__unionquery_" in key:
                            driver.driver.pop(key)

    def terminate(self):
        if not self.tasker:
            return
        self.tasker.terminate()