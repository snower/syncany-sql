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
from syncany.database import DatabaseManager, find_database
from syncany.errors import DatabaseUnknownException
from .version import version, version_info
from .parser import SqlParser, SqlSegment
from .config import GlobalConfig
from .executor import Executor
from .calculaters import GenerateCalculater, AggregateCalculater, StateAggregateCalculater, \
    WindowAggregateCalculater, WindowStateAggregateCalculater


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
        if "imports" not in self.executor.session_config.config:
            self.executor.session_config.config["imports"] = {}
        self.executor.session_config.config["imports"][name] = func_or_module

    def execute(self, sql):
        sql_parser = SqlParser(sql)
        sqls = sql_parser.split()
        self.__class__._execute_index += 1
        with self.executor as executor:
            executor.run("execute[%d]" % self._execute_index, sqls)
            exit_code = executor.execute()
            if exit_code is not None and exit_code != 0:
                raise ExecuterError(exit_code)
        return 0

    def get_database(self, db=None):
        return self.engine.get_database(db)

    def get_memory_datas(self, name):
        return self.engine.get_memory_datas(name)

    def pop_memory_datas(self, name):
        return self.engine.pop_memory_datas(name)

    def push_memory_datas(self, name, datas):
        return self.engine.push_datas(name, datas)

    def terminal(self):
        if not self.executor:
            return
        self.executor.terminate()

    def __enter__(self):
        self._thread_local.current_executer_context = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._thread_local.current_executer_context = None


class ScriptEngine(object):
    default_instance = None

    @classmethod
    def instance(cls, config=None):
        if cls.default_instance is None:
            cls.default_instance = ScriptEngine(config)
        return cls.default_instance

    @classmethod
    def current_context(cls):
        return ExecuterContext.current()

    def __init__(self, config=None):
        self.config = GlobalConfig()
        self.config.load(config)
        self.manager = None
        self.databases = {}
        self.executor = None

    def setup(self):
        if self.manager is not None:
            return
        init_execute_files = self.config.load()
        self.config.config_logging()
        self.config.load_extensions()
        self.manager = TaskerManager(DatabaseManager())
        self.executor = Executor(self.manager, self.config.session())
        if init_execute_files:
            self.executor.run("init", [SqlSegment("execute `%s`" % init_execute_files[i], i + 1) for i in
                                       range(len(init_execute_files))])
            with self.executor as executor:
                exit_code = executor.execute()
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
        if self.executor is None:
            self.setup()
        return ExecuterContext(self, self.executor).execute(sql)

    def get_database(self, db=None):
        if self.manager is None:
            self.setup()
        db = db if db else "--"
        if db in self.databases:
            return self.databases[db]
        try:
            database_config = dict(**[database for database in self.executor.session_config.get()["databases"]
                                      if database["name"] == db][0])
        except Exception:
            raise DatabaseUnknownException("%s is unknown" % db)
        database_cls = find_database(database_config.pop("driver"))
        if not database_cls:
            raise DatabaseUnknownException(database_config["name"] + " is unknown")
        self.databases[db] = database_cls(self.manager.database_manager, database_config).build()
        return self.databases[db]

    def get_memory_datas(self, name):
        if self.manager is None:
            return []
        database = self.get_database("--")
        query = database.query("--." + name, ["id"])
        return query.commit()

    def pop_memory_datas(self, name):
        if self.manager is None:
            return []
        database = self.get_database("--")
        query = database.query("--." + name, ["id"])
        datas = query.commit()
        delete = database.delete("--." + name, ["id"])
        delete.commit()
        return datas

    def push_memory_datas(self, name, datas):
        database = self.get_database("--")
        insert = database.insert("--." + name, ["id"], datas=datas if isinstance(datas, list) else [datas])
        insert.commit()

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
        if self.databases:
            for name, database in self.databases.items():
                database.close()
            self.databases = {}
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


def get_database(db=None):
    return ScriptEngine.instance().get_database(db)


def get_memory_datas(name):
    return ScriptEngine.instance().get_memory_datas(name)


def pop_memory_datas(name):
    return ScriptEngine.instance().pop_memory_datas(name)


def push_memory_datas(name, datas):
    return ScriptEngine.instance().push_datas(name, datas)


def terminal():
    return ScriptEngine.instance().terminal()


def close():
    if ScriptEngine.default_instance is None:
        return
    ScriptEngine.default_instance.close()
    ScriptEngine.default_instance = None
