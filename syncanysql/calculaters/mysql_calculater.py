# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

from syncany.calculaters.calculater import Calculater
from . import mysql_funcs


class MysqlCalculater(Calculater):
    funcs = mysql_funcs.funcs

    def calculate(self):
        func_name = self.name[7:]
        try:
            return self.funcs[func_name](*tuple(self.args))
        except:
            return None