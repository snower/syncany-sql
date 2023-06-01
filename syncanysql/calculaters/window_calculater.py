# -*- coding: utf-8 -*-
# 2023/6/1
# create by: snower

from syncany.calculaters.calculater import Calculater


class WindowAggregateCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(WindowAggregateCalculater, self).__init__(*args, **kwargs)

        if self.name.endswith("::aggregate"):
            self.calculate = self.aggregate
        elif self.name.endswith("::order_aggregate"):
            self.calculate = self.order_aggregate

    def aggregate(self, state_value, data_value, datas):
        return len(datas)

    def order_aggregate(self, state_value, data_value, datas, current_index=None, start_index=None, end_index=None):
        return current_index + 1

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


class WindowAggregateCountCalculater(WindowAggregateCalculater):
    def aggregate(self, state_value, data_value, datas):
        return len(datas)

    def order_aggregate(self, state_value, data_value, datas, current_index=None, start_index=None, end_index=None):
        return current_index + 1


class WindowAggregateDistinctCountCalculater(WindowStateAggregateCalculater):
    def aggregate(self, state_value, data_value, datas):
        if data_value is None:
            return state_value
        if state_value is None:
            try:
                return {data_value}
            except:
                if isinstance(data_value, list):
                    try:
                        return {tuple(data_value)}
                    except:
                        return {str(data_value)}
                return {str(data_value)}
        try:
            state_value.add(data_value)
        except:
            if isinstance(data_value, list):
                try:
                    state_value.add(tuple(data_value))
                except:
                    state_value.add(str(data_value))
            else:
                state_value.add(str(data_value))
        return state_value

    def order_aggregate(self, state_value, data_value, datas, current_index=None, start_index=None, end_index=None):
        return self.aggregate(state_value, data_value, datas)

    def final_value(self, state_value):
        if state_value is None:
            return 0
        return len(state_value)