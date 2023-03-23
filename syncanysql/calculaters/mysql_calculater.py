# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

import traceback
from syncany.logger import get_logger
from syncany.calculaters.calculater import Calculater
from . import mysql_funcs


class MysqlCalculater(Calculater):
    funcs = mysql_funcs.funcs

    def calculate(self, *args):
        func_name = self.name[7:]
        try:
            if len(args) == 1 and isinstance(args[0], list) and args[0] and isinstance(args[0][0], dict):
                try:
                    self.funcs[func_name](*tuple(args[0]))
                except TypeError:
                    pass
            return self.funcs[func_name](*args)
        except Exception as e:
            get_logger().warning("mysql calculater execute %s(%s) error: %s\n%s", func_name, args, e,
                                 traceback.format_exc())
            return None