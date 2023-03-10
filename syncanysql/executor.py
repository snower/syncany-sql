# -*- coding: utf-8 -*-
# 2023/2/13
# create by: snower

import os
import sys
import re
import copy
from collections import deque
from syncany.filters import StringFilter
from syncany.calculaters import find_calculater
from syncany.database.memory import MemoryDBFactory, MemoryDBCollection
from .errors import SyncanySqlCompileException
from .utils import parse_value
from .compiler import Compiler

ENV_VARIABLE_RE = re.compile("(\$\{\w+?(:.*?){0,1}\})", re.DOTALL | re.M)
RAW_SQL_RE = re.compile("(\(\s*?\/\*\s*?raw\(([\w\.]+?)\)\s*?\*\/(.*?)\/\*\s*?endraw\s*?\*\/\s*\))", re.DOTALL | re.M)
FUNC_RE = re.compile("^(\w+?)\(((.+),{0,1})*\)$", re.DOTALL)


class EnvVariables(dict):
    def __init__(self, parent=None, *args, **kwargs):
        super(EnvVariables, self).__init__(*args, **kwargs)

        self.parent = parent

    @classmethod
    def build_global(cls):
        env_variables = cls()
        env_variables.update({key.lower(): value for key, value in os.environ.items()})
        env_variables["home"] = os.path.expanduser('~')
        env_variables["syncany_home"] = os.path.join(os.path.expanduser('~'), ".syncany")
        env_variables["cwd"] = os.getcwd()
        env_variables["platform"] = sys.platform
        for arg in sys.argv:
            if arg[:2] != "--":
                continue
            arg_info = arg[2:].split("=")
            arg_value = arg_info[1] if len(arg_info) >= 2 else True
            if isinstance(arg_value, str) and arg_value and arg_value[0] in ('"', "'"):
                arg_value = arg_value[1:-1]
            env_variables[arg_info[0]] = arg_value
        return env_variables

    def get_value(self, key):
        if key in self:
            return self[key]
        if self.parent is None:
            raise KeyError("%s unknown" % key)
        return self.parent.get_value(key)

    def get(self, key, default=None):
        if key in self:
            return self[key]
        if self.parent is None:
            return default
        return self.parent.get(key, default)


class Executor(object):
    global_env_variables = None

    def __init__(self, manager, session_config, parent_executor=None):
        self.manager = manager
        self.session_config = session_config
        self.parent_executor = parent_executor
        self.runners = deque()
        self.tasker = None
        if self.global_env_variables is None:
            self.__class__.global_env_variables = EnvVariables.build_global()
        self.env_variables = parent_executor.env_variables if parent_executor else EnvVariables(self.global_env_variables)

    def compile_variable(self, sql):
        variables = ENV_VARIABLE_RE.findall(sql)
        for variable, default_value in variables:
            variable_name = variable[2:-1].split(":")[0]
            try:
                variable_value = self.env_variables.get_value(variable_name.lower())
            except KeyError:
                try:
                    groups = FUNC_RE.match(default_value[1:].strip()).groups()
                    calculater_args = []
                    if groups[1]:
                        for arg in groups[1].split(","):
                            calculater_args.append(parse_value(arg))
                    variable_value = find_calculater(groups[0].split("__")[0])(groups[0].replace("__", "::"),
                                                                               *tuple(calculater_args)).calculate()
                    variable_value = StringFilter().filter(variable_value)
                except Exception as e:
                    variable_value = default_value[1:]
            if isinstance(variable_value, str):
                variable_value = "true" if variable_value is True else ("false" if variable_value is False else
                                                                        ("null" if variable_value is None else str(variable_value)))
            sql = sql.replace(variable, variable_value)
        return sql

    def run(self, name, sqls):
        for sql in sqls:
            lineno = sql.lineno
            sql = self.compile_variable(str(sql))
            raw_sqls = RAW_SQL_RE.findall(sql)
            for raw, raw_name, raw_sql in raw_sqls:
                raw_name_info = raw_name.split(".")
                if len(raw_name_info) != 2:
                    raise SyncanySqlCompileException("raw sql name error: %s", raw)
                raw_sql = "set @config.databases." + raw_name_info[0] + ".virtual_views." + raw_name_info[1] + "='''\n" + raw_sql.strip() + "\n'''"
                self.compile(name + "(" + str(lineno) + ")#set_virtual_view", raw_sql)
                sql = sql.replace(raw, raw_name)
            self.compile(name + "(" + str(lineno) + ")", sql)

    def compile(self, name, sql):
        config = copy.deepcopy(self.session_config.get())
        config["name"] = name
        compiler = Compiler(config, self.env_variables)
        arguments = {"@verbose": self.env_variables.get("@verbose", False), "@timeout": self.env_variables.get("@timeout", 0),
                     "@limit": self.env_variables.get("@limit", 0), "@batch": self.env_variables.get("@batch", 0),
                     "@streaming": self.env_variables.get("@streaming", False), "@recovery": self.env_variables.get("@recovery", False),
                     "@join_batch": self.env_variables.get("@join_batch", 1000), "@insert_batch": self.env_variables.get("@insert_batch", 0)}
        tasker = compiler.compile(sql, arguments)
        self.runners.extend(tasker.start(self, self.session_config, self.manager, arguments))

    def execute(self):
        while self.runners:
            self.tasker = self.runners.popleft()
            try:
                exit_code = self.tasker.run(self, self.session_config, self.manager)
                if exit_code is not None and exit_code != 0:
                    return exit_code
            finally:
                self.tasker = None
                for config_key, factory in self.manager.database_manager.factorys.items():
                    if not isinstance(factory, MemoryDBFactory):
                        continue
                    for driver in factory.drivers:
                        if not isinstance(driver.instance, MemoryDBCollection):
                            continue
                        for key in list(driver.instance.keys()):
                            if self.runners and self.manager.database_manager.get_state(config_key + "::" + key, "is_streaming"):
                                continue
                            if "__subquery_" in key or "__unionquery_" in key:
                                driver.instance.remove(key)
        return 0

    def terminate(self):
        if not self.tasker:
            return
        self.tasker.terminate()

    def add_runner(self, tasker):
        self.runners.append(tasker)