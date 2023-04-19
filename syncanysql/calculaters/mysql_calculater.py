# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

import traceback
from syncany.logger import get_logger
from syncany.calculaters.calculater import Calculater
from . import mysql_funcs


class MysqlCalculater(Calculater):
    funcs = mysql_funcs.funcs

    def __init__(self, *args, **kwargs):
        super(MysqlCalculater, self).__init__(*args, **kwargs)

        self.func = self.funcs.get(self.name[7:])

    def calculate(self, *args):
        try:
            if len(args) == 1 and isinstance(args[0], list) and args[0] and isinstance(args[0][0], dict):
                try:
                    self.func(*tuple(args[0]))
                except TypeError:
                    pass
            return self.func(*args)
        except Exception as e:
            get_logger().warning("mysql calculater execute %s(%s) error: %s\n%s", self.name[7:], args, e,
                                 traceback.format_exc())
            return None