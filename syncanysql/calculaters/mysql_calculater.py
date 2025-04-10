# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

import types
import traceback
from syncany.logger import get_logger
from syncany.calculaters.calculater import Calculater
from syncany.calculaters.import_calculater import parse_final_filter


class MysqlCalculater(Calculater):
    funcs = None

    @classmethod
    def find_func(cls, name):
        if cls.funcs is None:
            from . import mysql_funcs
            cls.funcs = mysql_funcs.funcs
        return cls.funcs.get(name)

    def __init__(self, *args, **kwargs):
        super(MysqlCalculater, self).__init__(*args, **kwargs)

        func = self.find_func(self.name[7:])
        if func is not None and isinstance(func, types.FunctionType):
            try:
                argcount, varnames_count = func.__code__.co_argcount, len(func.__code__.co_varnames)
                if argcount == varnames_count:
                    if argcount == 0:
                        self.calculate = self.calculate0
                    elif argcount == 1:
                        self.calculate = self.calculate1
                    elif argcount == 2:
                        self.calculate = self.calculate2
                    elif argcount == 3:
                        self.calculate = self.calculate3
                    elif argcount == 4:
                        self.calculate = self.calculate4
            except Exception as e:
                pass
        self.func  = func

    def calculate(self, *args):
        try:
            return self.func(*args)
        except Exception as e:
            if isinstance(e, (ValueError, KeyError)):
                return None
            get_logger().warning("mysql calculater execute %s(%s) error: %s\n%s", self.name[7:], args, e,
                                 traceback.format_exc())
            return None

    def calculate0(self):
        try:
            return self.func()
        except Exception as e:
            if isinstance(e, (ValueError, KeyError)):
                return None
            get_logger().warning("mysql calculater execute %s(%s) error: %s\n%s", self.name[7:], tuple(), e,
                                 traceback.format_exc())
            return None

    def calculate1(self, args0=None):
        try:
            return self.func(args0)
        except Exception as e:
            if isinstance(e, (ValueError, KeyError)):
                return None
            get_logger().warning("mysql calculater execute %s(%s) error: %s\n%s", self.name[7:], (args0,), e,
                                 traceback.format_exc())
            return None

    def calculate2(self, args0=None, args1=None):
        try:
            return self.func(args0, args1)
        except Exception as e:
            if isinstance(e, (ValueError, KeyError)):
                return None
            get_logger().warning("mysql calculater execute %s(%s) error: %s\n%s", self.name[7:], (args0, args1), e,
                                 traceback.format_exc())
            return None

    def calculate3(self, args0=None, args1=None, args2=None):
        try:
            return self.func(args0, args1, args2)
        except Exception as e:
            if isinstance(e, (ValueError, KeyError)):
                return None
            get_logger().warning("mysql calculater execute %s(%s) error: %s\n%s", self.name[7:], (args0, args1, args2), e,
                                 traceback.format_exc())
            return None

    def calculate4(self, args0=None, args1=None, args2=None, args3=None):
        try:
            return self.func(args0, args1, args2, args3)
        except Exception as e:
            if isinstance(e, (ValueError, KeyError)):
                return None
            get_logger().warning("mysql calculater execute %s(%s) error: %s\n%s", self.name[7:], (args0, args1, args2, args3), e,
                                 traceback.format_exc())
            return None

    def get_final_filter(self):
        if hasattr(self.func, "get_final_filter"):
            return self.func.get_final_filter()
        return parse_final_filter(self.func)

    def is_realtime_calculater(self):
        if self.name[7:] in {"currenttimestamp", "curdate", "currentdate", "curtime", "currenttime",
                             "sysdate", "unix_timestamp", "utc_date", "utc_time", "utc_timestamp"}:
            return True
        return False
