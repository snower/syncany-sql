# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

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

    def get_final_filter(self):
        if hasattr(self.func, "get_final_filter"):
            return self.func.get_final_filter()
        return parse_final_filter(self.func)

    def is_realtime_calculater(self):
        if self.name[7:] in {"currenttimestamp", "curdate", "currentdate", "curtime", "currenttime",
                             "sysdate", "unix_timestamp", "utc_date", "utc_time", "utc_timestamp"}:
            return True
        return False
