# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

from syncany.calculaters import register_calculater
from .mysql_calculater import MysqlCalculater
from .aggregate_calculater import *


def is_mysql_func(name):
    return name in MysqlCalculater.funcs


def register_sql_calculaters():
    register_calculater("mysql", MysqlCalculater)
    register_calculater("aggregate_key", AggregateKeyCalculater)
    register_calculater("aggregate_add", AggregateAddCalculater)
    register_calculater("aggregate_max", AggregateMaxCalculater)
    register_calculater("aggregate_min", AggregateMinCalculater)

