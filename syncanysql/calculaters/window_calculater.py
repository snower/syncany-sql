# -*- coding: utf-8 -*-
# 2023/6/1
# create by: snower

from syncany.calculaters.calculater import Calculater
from ..errors import SyncanySqlExecutorException


class WindowAggregateCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(WindowAggregateCalculater, self).__init__(*args, **kwargs)

        if self.name.endswith("::aggregate"):
            self.calculate = self.aggregate
        elif self.name.endswith("::order_aggregate"):
            self.calculate = self.order_aggregate

    def aggregate(self, state_value, data_value, context):
        raise SyncanySqlExecutorException("window caculate require order by")

    def order_aggregate(self, state_value, data_value, context):
        return context.current_index + 1

    def calculate(self, *args):
        if self.name.endswith("::aggregate"):
            return self.aggregate(*args)
        if self.name.endswith("::order_aggregate"):
            return self.order_aggregate(*args)
        return None


class WindowStateAggregateCalculater(WindowAggregateCalculater):
    def __init__(self, *args, **kwargs):
        super(WindowStateAggregateCalculater, self).__init__(*args, **kwargs)

        if self.name.endswith("::final_value"):
            self.calculate = self.final_value

    def final_value(self, state_value):
        return state_value

    def calculate(self, *args):
        if self.name.endswith("::final_value"):
            return self.final_value(*args)
        return super(WindowStateAggregateCalculater, self).calculate(*args)


class WindowStateAggregateRowNumberCalculater(WindowAggregateCalculater):
    def order_aggregate(self, state_value, data_value, context):
        return context.current_index + 1


class WindowStateAggregateRankCalculater(WindowStateAggregateCalculater):
    def order_aggregate(self, state_value, data_value, context):
        order_value = context.order_value
        if state_value is None:
            return {"order_value": order_value, "rank": 1, "row_number": 1}
        state_value["row_number"] += 1
        if order_value != state_value["order_value"]:
            state_value["rank"] += 1
        return state_value

    def final_value(self, state_value):
        if state_value is None:
            return 1
        return state_value["rank"]


class WindowStateAggregateDenseRankCalculater(WindowStateAggregateCalculater):
    def order_aggregate(self, state_value, data_value, context):
        order_value = context.order_value
        if state_value is None:
            return {"order_value": order_value, "rank": 1}
        if order_value != state_value["order_value"]:
            state_value["rank"] += 1
        return state_value

    def final_value(self, state_value):
        if state_value is None:
            return 1
        return state_value["rank"]


class WindowStateAggregatePercentRankCalculater(WindowStateAggregateCalculater):
    def order_aggregate(self, state_value, data_value, context):
        order_value = context.order_value
        if state_value is None:
            return {"order_value": order_value, "rank": 1, "row_number": 1}
        state_value["row_number"] += 1
        if order_value != state_value["order_value"]:
            state_value["rank"] += 1
        return state_value

    def final_value(self, state_value):
        if state_value is None:
            return 1
        if state_value["row_number"] <= 1:
            return 0
        return state_value["rank"] - 1 / state_value["row_number"] - 1


class WindowStateAggregateCumeDistCalculater(WindowAggregateCalculater):
    def order_aggregate(self, state_value, data_value, context):
        if not context.datas:
            return 0
        return context.current_index + 1 / len(context.datas) + 1