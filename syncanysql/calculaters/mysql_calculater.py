# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

import traceback
from syncany.logger import get_logger
from syncany.calculaters.calculater import Calculater


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

        self.func = self.find_func(self.name[7:])

    def calculate(self, *args):
        try:
            return self.func(*args)
        except (ValueError, KeyError) as e:
            return None
        except Exception as e:
            get_logger().warning("mysql calculater execute %s(%s) error: %s\n%s", self.name[7:], args, e,
                                 traceback.format_exc())
            return None
