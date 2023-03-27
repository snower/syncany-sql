# -*- coding: utf-8 -*-
# 23/02/07
# create by: snower

from .version import version, version_info

import threading
from syncany.loaders import Loader, register_loader
from syncany.outputers import Outputer, register_outputer
from syncany.valuers import Valuer, register_valuer
from syncany.filters import Filter, register_filter
from syncany.database import DataBase, register_database
from syncany.calculaters import Calculater, TypeFormatCalculater, TypingCalculater, MathematicalCalculater, \
    TransformCalculater, register_calculater
from syncany.taskers.config import Parser, ConfigReader, register_parser, register_reader
from syncany.taskers.tasker import current_tasker
from syncany.taskers.manager import TaskerManager
from syncany.database.database import DatabaseManager
from syncany.database.memory import MemoryDBFactory, MemoryDBCollection
from .version import version, version_info
from .parser import SqlParser, SqlSegment
from .config import GlobalConfig
from .executor import Executor
from .calculaters import AggregateCalculater, StateAggregateCalculater


class ExecuterError(Exception):
    def __init__(self, exit_code, *args):
        super(ExecuterError, self).__init__(*args)

        self.exit_code = exit_code


class ExecuterContext(object):
    _execute_index = 0
    _thread_local = threading.local()

    @classmethod
    def current(cls):
        return cls._thread_local.current_executer_context

    def __init__(self, engine, executor):
        self.engine = engine
        self.executor = executor

    def get_variable(self, name, default=None):
        if name and name[0] != "@":
            name = "@" + name
        return self.executor.env_variables.get(name, default)

    def set_variable(self, name, value):
        if name and name[0] != "@":
            name = "@" + name
        self.executor.env_variables[name] = value

    def use(self, name, func_or_module):
        if "imports" not in self.executor.session_config:
            self.executor.session_config.config["imports"] = {}
        self.executor.session_config.config["imports"][name] = func_or_module

    def execute(self, sql):
        self.__class__._thread_local.current_executer_context = self
        try:
            sql_parser = SqlParser(sql)
            sqls = sql_parser.split()
            self.__class__._execute_index += 1
            self.executor.run("execute[%d]" % self._execute_index, sqls)
            exit_code = self.executor.execute()
            if exit_code is not None and exit_code != 0:
                raise ExecuterError(exit_code)
            return 0
        finally:
            self.__class__._thread_local.current_executer_context = None

    def get_memory_datas(self, name, db=None):
        return self.engine.get_memory_datas(name, db)

    def pop_memory_datas(self, name, db=None):
        return self.engine.pop_memory_datas(name, db)

    def terminal(self):
        if not self.executor:
            return
        self.executor.terminate()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class ScriptEngine(object):
    default_instance = None

    @classmethod
    def instance(cls, config=None):
        if cls.default_instance is None:
            cls.default_instance = ScriptEngine(config)
        return cls.default_instance

    def __init__(self, config=None):
        self.config = GlobalConfig()
        self.config.load(config)
        self.manager = None
        self.executor = None

    def setup(self):
        if self.manager is not None:
            return
        init_execute_files = self.config.load()
        self.config.config_logging()
        self.manager = TaskerManager(DatabaseManager())
        self.executor = Executor(self.manager, self.config.session())
        if init_execute_files:
            self.executor.run("init", [SqlSegment("execute `%s`" % init_execute_files[i], i + 1) for i in
                                       range(len(init_execute_files))])
            exit_code = self.executor.execute()
            if exit_code is not None and exit_code != 0:
                raise ExecuterError(exit_code)
        return 0

    def get_variable(self, name, default=None):
        if self.executor is None:
            self.setup()
        if name and name[0] != "@":
            name = "@" + name
        return self.executor.env_variables.get(name, default)

    def set_variable(self, name, value):
        if self.executor is None:
            self.setup()
        if name and name[0] != "@":
            name = "@" + name
        self.executor.env_variables[name] = value

    def use(self, name, func_or_module):
        if self.executor is None:
            self.setup()
        if "imports" not in self.executor.session_config.config:
            self.executor.session_config["imports"] = {}
        self.executor.session_config.config["imports"][name] = func_or_module

    def context(self):
        if self.executor is None:
            self.setup()
        return ExecuterContext(self, Executor(self.manager, self.executor.session_config.session(), self.executor))

    def execute(self, sql):
        return ExecuterContext(self, self.executor).execute(sql)

    def get_memory_datas(self, name, db=None):
        if self.manager is None:
            return []
        collection_key = ("--." + name) if not db else (db + "." + name)
        for config_key, factory in self.manager.database_manager.factorys.items():
            if not isinstance(factory, MemoryDBFactory):
                continue
            for driver in factory.drivers:
                if not isinstance(driver.instance, MemoryDBCollection):
                    continue
                for key in list(driver.instance.keys()):
                    if collection_key == key:
                        return driver.instance[key]
        return []

    def pop_memory_datas(self, name, db=None):
        if self.manager is None:
            return []
        collection_key = ("--." + name) if not db else (db + "." + name)
        for config_key, factory in self.manager.database_manager.factorys.items():
            if not isinstance(factory, MemoryDBFactory):
                continue
            for driver in factory.drivers:
                if not isinstance(driver.instance, MemoryDBCollection):
                    continue
                for key in list(driver.instance.keys()):
                    if collection_key == key:
                        collection_datas = driver.instance[key]
                        driver.instance.remove(key)
                        return collection_datas
        return []

    def terminal(self):
        executer_context = ExecuterContext.current()
        if not executer_context:
            if not self.executor:
                return
            self.executor.terminate()
            return
        executer_context.terminate()

    def close(self):
        if self.manager:
            self.manager.close()
            self.manager = None
        self.executor = None
        self.config = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()


def instance(config=None):
    return ScriptEngine.instance(config)


def get_variable(name, default=None):
    return ScriptEngine.instance().get_variable(name, default)


def set_variable(name, value):
    ScriptEngine.instance().set_variable(name, value)


def use(name, func_or_module):
    ScriptEngine.instance().use(name, func_or_module)


def execute(sql):
    return ScriptEngine.instance().execute(sql)


def get_memory_datas(name, db=None):
    return ScriptEngine.instance().get_memory_datas(name, db)


def pop_memory_datas(name, db=None):
    return ScriptEngine.instance().pop_memory_datas(name, db)


def terminal():
    return ScriptEngine.instance().terminal()


def close():
    if ScriptEngine.default_instance is None:
        return
    ScriptEngine.default_instance.close()
    ScriptEngine.default_instance = None
