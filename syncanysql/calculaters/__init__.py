# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

import types
from syncany.calculaters import CALCULATERS, Calculater, TypeFormatCalculater, TypingCalculater, MathematicalCalculater
from syncany.calculaters import register_calculater, find_calculater, CalculaterUnknownException
from syncany.calculaters.import_calculater import ImportCalculater
from .env_variable_calculater import CurrentEnvVariableCalculater
from .row_calculater import *
from .mysql_calculater import MysqlCalculater
from .generate_calculater import *
from .aggregate_calculater import *
from .window_calculater import *

SQL_CALCULATERS = {
    "current_env_variable": CurrentEnvVariableCalculater,
    "row_index": RowIndexCalculater,
    "row_last": RowLastCalculater,
    "mysql": MysqlCalculater,
    "aggregate_key": AggregateKeyCalculater,
    "aggregate_count": AggregateCountCalculater,
    "aggregate_distinct_count": AggregateDistinctCountCalculater,
    "aggregate_sum": AggregateSumCalculater,
    "aggregate_max": AggregateMaxCalculater,
    "aggregate_min": AggregateMinCalculater,
    "aggregate_avg": AggregateAvgCalculater,
    "yield_array": GenerateYieldArrayCalculater,
    "group_concat": AggregateGroupConcatCalculater,
    "group_array": AggregateGroupArrayCalculater,
    "group_uniq_array": AggregateGroupUniqArrayCalculater,
    "group_bit_and": AggregateGroupBitAndCalculater,
    "group_bit_or": AggregateGroupBitOrCalculater,
    "group_bit_xor": AggregateGroupBitXorCalculater,
    "row_number": WindowStateAggregateRowNumberCalculater,
    "rank": WindowStateAggregateRankCalculater,
    "dense_rank": WindowStateAggregateDenseRankCalculater,
    "percent_rank": WindowStateAggregatePercentRankCalculater,
    "cume_dist": WindowStateAggregateCumeDistCalculater,
    "lag": WindowStateAggregateLagCalculater,
    "lead": WindowStateAggregateLeadCalculater,
}
CALCULATERS.update(SQL_CALCULATERS)


def is_mysql_func(name):
    if MysqlCalculater.funcs is None:
        MysqlCalculater.find_func(name)
    return name in MysqlCalculater.funcs


def find_generate_calculater(name):
    calculater = find_calculater(name)
    if issubclass(calculater, ImportCalculater):
        try:
            import_calculater = ImportCalculater(name)
            if isinstance(import_calculater.module_or_func, (types.FunctionType, types.LambdaType)):
                if import_calculater.module_or_func.__code__.co_flags & 0x20 != 0:
                    return calculater
        except:
            pass
        raise CalculaterUnknownException("%s is unknown generate calculater" % name)
    if not issubclass(calculater, GenerateCalculater):
        raise CalculaterUnknownException("%s is unknown generate calculater" % name)
    return calculater


def find_aggregate_calculater(name):
    calculater = find_calculater(name)
    if not issubclass(calculater, AggregateCalculater):
        raise CalculaterUnknownException("%s is unknown aggregate calculater" % name)
    return calculater


def find_window_aggregate_calculater(name):
    calculater = find_calculater(name)
    if not issubclass(calculater, WindowAggregateCalculater):
        raise CalculaterUnknownException("%s is unknown window aggregate calculater" % name)
    return calculater