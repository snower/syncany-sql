# -*- coding: utf-8 -*-
# 2023/4/25
# create by: snower

from syncanysql.calculaters import StateAggregateCalculater, WindowStateAggregateCalculater, register_calculater


@register_calculater("window_aggregate_unique")
class UniqueSetWindowAggregateCalculater(StateAggregateCalculater):
    def aggregate(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return {data_value}
        state_value.add(data_value)
        return state_value

    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return state_value | data_value

    def final_value(self, state_value):
        return state_value


@register_calculater("window_aggregate_join")
class JoinArrayWindowAggregateCalculater(WindowStateAggregateCalculater):
    def aggregate(self, state_value, data_value, context):
        if data_value is None:
            return state_value
        if state_value is None:
            return [str(data_value)]
        state_value.append(str(data_value))
        return state_value

    def order_aggregate(self, state_value, data_value, context):
        if data_value is None:
            return state_value
        if state_value is None:
            return [str(data_value)]
        state_value.append(str(data_value))
        return state_value

    def final_value(self, state_value):
        if not state_value:
            return ""
        return ",".join(state_value)
