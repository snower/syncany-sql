# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

from syncany.calculaters import CALCULATERS, Calculater, TypeFormatCalculater, TypingCalculater, MathematicalCalculater
from syncany.calculaters import register_calculater, find_calculater, CalculaterUnknownException
from .mysql_calculater import MysqlCalculater
from .aggregate_calculater import *

SQL_CALCULATERS = {
    "mysql": MysqlCalculater,
    "aggregate_key": AggregateKeyCalculater,
    "aggregate_count": AggregateCountCalculater,
    "aggregate_sum": AggregateSumCalculater,
    "aggregate_max": AggregateMaxCalculater,
    "aggregate_min": AggregateMinCalculater,
    "aggregate_avg": AggregateAvgCalculater,
}
CALCULATERS.update(SQL_CALCULATERS)


def is_mysql_func(name):
    if MysqlCalculater.funcs is None:
        MysqlCalculater.find_func(name)
    return name in MysqlCalculater.funcs


def find_aggregate_calculater(name):
    calculater = find_calculater(name)
    if not issubclass(calculater, AggregateCalculater):
        raise CalculaterUnknownException("%s is unknown calculater" % name)
    return calculater

