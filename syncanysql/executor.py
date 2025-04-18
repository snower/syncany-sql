# -*- coding: utf-8 -*-
# 2023/2/13
# create by: snower

import datetime
import os
import sys
import re
import threading
from collections import deque
from syncany.filters import StringFilter
from syncany.calculaters import find_calculater
from syncany.database import find_database
from syncany.taskers.config import ConfigReader, register_reader
from syncany.taskers.core import CoreTasker
from .errors import SyncanySqlCompileException
from .utils import SequenceTypes, parse_value
from .compiler import Compiler

ENV_VARIABLE_RE = re.compile(r"(\$\{[@\w]+?(:.*?)?\})", re.DOTALL | re.M)
RAW_SQL_RE = re.compile(r"(\/\*\s*raw\(([\w\.]+?)\)\s*(\*\/\s*\()?\s*(.*?)\s*(\)\s*\/\*)?\s*endraw\s*\*\/)", re.DOTALL | re.M)
FUNC_RE = re.compile(r"^(\w+?)\(((.+),?)*\)$", re.DOTALL)


@register_reader("context")
class ContextConfigReader(ConfigReader):
    def read(self):
        executor = Executor.current()
        if not executor:
            return "dict", {}
        names = self.name[10:].split("/")
        if len(names) >= 2 and names[0] == "session" and names[1] == "config":
            if len(names) >= 3:
                return "dict", {
                    names[2]: executor.session_config.get().get(names[2], CoreTasker.DEFAULT_CONFIG.get(names[2]))
                }
            return "dict", executor.session_config.get_tasker_extend_config()
        return "dict", {}


class EnvVariables(dict):
    def __init__(self, parent=None, *args, **kwargs):
        super(EnvVariables, self).__init__(*args, **kwargs)

        self.parent = parent

    @classmethod
    def build_global(cls, session_config):
        env_variables = cls()
        env_variables.update({key.lower(): value for key, value in os.environ.items()})
        env_variables["home"] = os.path.expanduser('~')
        env_variables["syncany_home"] = session_config.get_home()
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
        if key and key[0] == "@" and key[1:] in self:
            return self[key[1:]]
        if self.parent is None:
            raise KeyError("%s unknown" % key)
        return self.parent.get_value(key)

    def get(self, key, default=None):
        if key in self:
            return self[key]
        if key and key[0] == "@" and key[1:] in self:
            return self[key[1:]]
        if self.parent is None:
            return default
        return self.parent.get(key, default)


class Executor(object):
    _thread_local = threading.local()
    global_env_variables = None
    tasker_index = 0

    @classmethod
    def current(cls):
        return cls._thread_local.current_executor

    def __init__(self, manager, session_config, parent_executor=None):
        self.manager = manager
        self.session_config = session_config
        self.parent_executor = parent_executor
        self.runners = deque()
        self.tasker = None
        if self.global_env_variables is None:
            self.__class__.global_env_variables = EnvVariables.build_global(session_config)
        self.env_variables = EnvVariables(parent_executor.env_variables if parent_executor
                                          else self.global_env_variables)

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
                    calculater = find_calculater(groups[0].split("__")[0]).instance(groups[0].replace("__", "::"))
                    variable_value = calculater.calculate(*tuple(calculater_args))
                    variable_value = StringFilter().filter(variable_value)
                except Exception as e:
                    variable_value = default_value[1:]

            def format_variable_value(value):
                if value is True:
                    return "true"
                if value is False:
                    return "false"
                if value is None:
                    return "null"
                if isinstance(value, datetime.date):
                    if isinstance(value, datetime.datetime):
                        return value.strftime(self.session_config.get().get("datetime_format", "%Y-%m-%d %H:%M:%S"))
                    return value.strftime(self.session_config.get().get("date_format", "%Y-%m-%d"))
                if isinstance(value, datetime.time):
                    return value.strftime(self.session_config.get().get("time_format", "%H:%M:%S"))
                if isinstance(value, SequenceTypes):
                    return "(" + ",".join([format_variable_value(v) for v in value]) + ")"
                return str(value)
            sql = sql.replace(variable, format_variable_value(variable_value))
        return sql

    def run(self, name, sqls):
        self._thread_local.current_executor = self
        for sql in sqls:
            lineno = sql.lineno
            sql = self.compile_variable(str(sql))
            raw_sqls = RAW_SQL_RE.findall(sql)
            for raw, raw_name, _, raw_sql, _ in raw_sqls:
                raw_name_info = raw_name.split(".")
                if len(raw_name_info) != 2:
                    raise SyncanySqlCompileException("raw sql name error: %s", raw)
                raw_sql = "set @config.databases." + raw_name_info[0] + ".virtual_views." \
                          + raw_name_info[1] + "='''\n" + raw_sql.strip() + "\n'''"
                self.compile(name + "(" + str(lineno) + ")#set_virtual_view", raw_sql)
                sql = sql.replace(raw, raw_name)
            self.compile(name + "(" + str(lineno) + ")", sql)

    def compile(self, name, sql):
        self._thread_local.current_executor = self
        compiler = Compiler(self.session_config, self.env_variables, name)
        arguments = {"@verbose": self.env_variables.get("@verbose", False), "@timeout": self.env_variables.get("@timeout", 0),
                     "@limit": self.env_variables.get("@limit", 0), "@batch": self.env_variables.get("@batch", 0),
                     "@streaming": self.env_variables.get("@streaming", False), "@recovery": self.env_variables.get("@recovery", False),
                     "@join_batch": self.env_variables.get("@join_batch", 10000), "@insert_batch": self.env_variables.get("@insert_batch", 0),
                     "@use_input": self.env_variables.get("@use_input", None), "@use_output": self.env_variables.get("@use_output", None),
                     "@use_output_type": self.env_variables.get("@use_output_type", None), "@primary_order": False}
        tasker = compiler.compile(sql, arguments)
        self.runners.extend(tasker.start(name, self, self.session_config, self.manager, arguments))

    def execute(self):
        self._thread_local.current_executor = self
        while self.runners:
            self.tasker = self.runners.popleft()
            try:
                self.tasker.run(self, self.session_config, self.manager)
            finally:
                self.tasker = None

    def terminate(self):
        if not self.tasker:
            return
        self.tasker.terminate()

    def add_runner(self, tasker):
        self.runners.append(tasker)

    def clear_temporary_memory_collection(self, names):
        try:
            database_config = dict(**[database for database in self.session_config.get()["databases"]
                                      if database["name"] == "--"][0])
        except (TypeError, KeyError, IndexError):
            return
        database_cls = find_database(database_config.pop("driver"))
        if not database_cls:
            return
        database = database_cls(self.manager.database_manager, database_config).build()
        try:
            for name in names:
                if self.runners and database.is_streaming(name):
                    continue
                delete = database.delete(name, ["id"])
                delete.commit()
        finally:
            database.close()

    def __enter__(self):
        self._thread_local.current_executor = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._thread_local.current_executor = self.parent_executor

    def distribute_tasker_index(self):
        if self.parent_executor is not None:
            return self.parent_executor.distribute_tasker_index()
        self.tasker_index += 1
        return self.tasker_index

    def get_tasker_index(self):
        if self.parent_executor is not None:
            return self.parent_executor.get_tasker_index()
        return self.tasker_index
