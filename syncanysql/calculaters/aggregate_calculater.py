# -*- coding: utf-8 -*-
# 2023/3/21
# create by: snower

from syncany.calculaters.calculater import Calculater


class AggregateKeyCalculater(Calculater):
    def calculate(self, *args):
        if len(args) == 1:
            return args[0]
        return args


class AggregateCalculater(Calculater):
    def __init__(self, *args, **kwargs):
        super(AggregateCalculater, self).__init__(*args, **kwargs)
        
        if self.name.endswith("::aggregate"):
            self.calculate = self.aggregate
        elif self.name.endswith("::reduce"):
            self.calculate = self.reduce
        
    def aggregate(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return state_value + data_value
    
    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return state_value + data_value
    
    def calculate(self, *args):
        if self.name.endswith("::aggregate"):
            return self.aggregate(*args)
        if self.name.endswith("::reduce"):
            return self.reduce(*args)
        return None
    
    
class StateAggregateCalculater(AggregateCalculater):
    def __init__(self, *args, **kwargs):
        super(StateAggregateCalculater, self).__init__(*args, **kwargs)

        if self.name.endswith("::final_value"):
            self.calculate = self.final_value
            
    def final_value(self, state_value):
        return state_value
    
    def calculate(self, *args):
        if self.name.endswith("::final_value"):
            return self.reduce(*args)
        return super(StateAggregateCalculater, self).calculate(*args)


class AggregateCountCalculater(AggregateCalculater):
    def aggregate(self, state_value, data_value):
        if data_value is None:
            return state_value
        try:
            return state_value + 1
        except:
            if state_value is None:
                return 1
            try:
                return int(state_value) + 1
            except:
                return state_value


class AggregateSumCalculater(AggregateCalculater):
    def aggregate(self, state_value, data_value):
        try:
            return state_value + data_value
        except:
            if data_value is None:
                return state_value
            if state_value is None:
                return data_value
            try:
                if isinstance(state_value, (int, float)):
                    return state_value + type(state_value)(data_value)
                try:
                    return int(state_value) + int(data_value)
                except:
                    return float(state_value) + float(data_value)
            except:
                return state_value


class AggregateMaxCalculater(AggregateCalculater):
    def aggregate(self, state_value, data_value):
        try:
            return max(state_value, data_value)
        except:
            if data_value is None:
                return state_value
            if state_value is None:
                return data_value
            try:
                return max(state_value, type(state_value)(data_value))
            except:
                return state_value

    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return max(state_value, data_value)


class AggregateMinCalculater(AggregateCalculater):
    def aggregate(self, state_value, data_value):
        try:
            return min(state_value, data_value)
        except:
            if data_value is None:
                return state_value
            if state_value is None:
                return data_value
            try:
                return min(state_value, type(state_value)(data_value))
            except:
                return state_value

    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return min(state_value, data_value)


class AggregateAvgCalculater(StateAggregateCalculater):
    def aggregate(self, state_value, data_value):
        try:
            state_value["count_value"] += 1
            state_value["sum_value"] += data_value
            return state_value
        except:
            if data_value is None:
                return state_value
            if state_value is None:
                if data_value is None:
                    return None
                return {"count_value": 1, "sum_value": data_value}
            try:
                if isinstance(state_value["sum_value"], (int, float)):
                    state_value["count_value"] += 1
                    state_value["sum_value"] += type(state_value["sum_value"])(data_value)
                    return state_value
                try:
                    return {"count_value": state_value["count_value"] + 1,
                            "sum_value": int(state_value["sum_value"]) + int(data_value)}
                except:
                    return {"count_value": state_value["count_value"] + 1,
                            "sum_value": float(state_value["sum_value"]) + float(data_value)}
            except:
                return state_value

    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return {"count_value": state_value["count_value"] + data_value["count_value"],
                "sum_value": state_value["sum_value"] + data_value["sum_value"]}

    def final_value(self, state_value):
        if state_value is None:
            return 0
        return state_value["sum_value"] / state_value["count_value"]
