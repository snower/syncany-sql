# -*- coding: utf-8 -*-
# 2023/6/1
# create by: snower

from syncany.calculaters.calculater import Calculater
from syncany.filters import IntFilter, FloatFilter
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


class WindowAggregateRowNumberCalculater(WindowAggregateCalculater):
    def order_aggregate(self, state_value, data_value, context):
        return context.current_index + 1

    def get_final_filter(self):
        return IntFilter.default()


class WindowAggregateRankCalculater(WindowStateAggregateCalculater):
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

    def get_final_filter(self):
        return IntFilter.default()


class WindowAggregateDenseRankCalculater(WindowStateAggregateCalculater):
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

    def get_final_filter(self):
        return IntFilter.default()


class WindowAggregatePercentRankCalculater(WindowStateAggregateCalculater):
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

    def get_final_filter(self):
        return FloatFilter.default()


class WindowAggregateFirstValueCalculater(WindowAggregateCalculater):
    def order_aggregate(self, state_value, data_value, context):
        valuer_id = context.partition_calculater.calculate_valuer.valuer_id
        for partition_calculater in context.datas[0][2]:
            if partition_calculater.calculate_valuer.valuer_id == valuer_id:
                return partition_calculater.value
        return None


class WindowAggregateLastValueCalculater(WindowAggregateCalculater):
    def order_aggregate(self, state_value, data_value, context):
        valuer_id = context.partition_calculater.calculate_valuer.valuer_id
        for partition_calculater in context.datas[-1][2]:
            if partition_calculater.calculate_valuer.valuer_id == valuer_id:
                return partition_calculater.value
        return None


class WindowAggregateNthValueCalculater(WindowAggregateCalculater):
    def order_aggregate(self, state_value, data_value, context):
        if isinstance(data_value, list):
            offset_index = int(data_value[1]) if len(data_value) >= 2 else 1
            if offset_index > 0:
                offset_index -= 1
            if (offset_index < 0 and -offset_index > len(context.datas)) or offset_index >= len(context.datas):
                return data_value[2] if len(data_value) >= 3 else None
            valuer_id = context.partition_calculater.calculate_valuer.valuer_id
            for partition_calculater in context.datas[offset_index][2]:
                if partition_calculater.calculate_valuer.valuer_id == valuer_id:
                    return partition_calculater.value[0]
            return None

        valuer_id = context.partition_calculater.calculate_valuer.valuer_id
        for partition_calculater in context.datas[0][2]:
            if partition_calculater.calculate_valuer.valuer_id == valuer_id:
                return partition_calculater.value
        return None


class WindowAggregateNtileCalculater(WindowAggregateCalculater):
    def order_aggregate(self, state_value, data_value, context):
        if isinstance(data_value, list):
            n = int(data_value[0]) if data_value else 1
        else:
            n = int(data_value)
        if n <= 0:
            return None
        return int(context.current_index / (len(context.datas) / n)) + 1


class WindowAggregateLagCalculater(WindowAggregateCalculater):
    def order_aggregate(self, state_value, data_value, context):
        if isinstance(data_value, list):
            offset = int(data_value[1]) if len(data_value) >= 2 else 1
            offset_index = context.current_index - offset
            if offset_index < 0 or offset_index >= len(context.datas):
                return data_value[2] if len(data_value) >= 3 else None
            valuer_id = context.partition_calculater.calculate_valuer.valuer_id
            for partition_calculater in context.datas[offset_index][2]:
                if partition_calculater.calculate_valuer.valuer_id == valuer_id:
                    return partition_calculater.value[0]
            return None

        offset_index = context.current_index - 1
        if offset_index < 0 or offset_index >= len(context.datas):
            return None
        valuer_id = context.partition_calculater.calculate_valuer.valuer_id
        for partition_calculater in context.datas[offset_index][2]:
            if partition_calculater.calculate_valuer.valuer_id == valuer_id:
                return partition_calculater.value
        return None


class WindowAggregateLeadCalculater(WindowAggregateCalculater):
    def order_aggregate(self, state_value, data_value, context):
        if isinstance(data_value, list):
            offset = int(data_value[1]) if len(data_value) >= 2 else 1
            offset_index = context.current_index + offset
            if offset_index < 0 or offset_index >= len(context.datas):
                return data_value[2] if len(data_value) >= 3 else None
            valuer_id = context.partition_calculater.calculate_valuer.valuer_id
            for partition_calculater in context.datas[offset_index][2]:
                if partition_calculater.calculate_valuer.valuer_id == valuer_id:
                    return partition_calculater.value[0]
            return None

        offset_index = context.current_index + 1
        if offset_index < 0 or offset_index >= len(context.datas):
            return None
        valuer_id = context.partition_calculater.calculate_valuer.valuer_id
        for partition_calculater in context.datas[offset_index][2]:
            if partition_calculater.calculate_valuer.valuer_id == valuer_id:
                return partition_calculater.value
        return None


class WindowAggregateCumeDistCalculater(WindowAggregateCalculater):
    def order_aggregate(self, state_value, data_value, context):
        if not context.datas:
            return 0
        return context.current_index + 1 / len(context.datas) + 1

    def get_final_filter(self):
        return FloatFilter.default()
