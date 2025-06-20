# -*- coding: utf-8 -*-
# 2023/3/21
# create by: snower

import datetime
import json
from syncany.calculaters.calculater import Calculater
from syncany.filters import IntFilter, FloatFilter, ArrayFilter, StringFilter
from ..utils import NumberDecimalTypes, ensure_number, ensure_int


class AggregateKeyCalculater(Calculater):
    def calculate(self, *args):
        if len(args) == 1:
            return tuple(args[0]) if isinstance(args[0], list) else args[0]
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
            return self.final_value(*args)
        return super(StateAggregateCalculater, self).calculate(*args)


class AggregateCountCalculater(AggregateCalculater):
    def aggregate(self, state_value, data_value):
        if data_value is None:
            return state_value or 0
        try:
            return state_value + 1
        except:
            if state_value is None:
                return 1
            try:
                return int(state_value) + 1
            except:
                return state_value

    def get_final_filter(self):
        return IntFilter.default()

class AggregateDistinctCountCalculater(StateAggregateCalculater):
    def aggregate(self, state_value, data_value):
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

    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return state_value | data_value

    def final_value(self, state_value):
        if state_value is None:
            return 0
        return len(state_value)

    def get_final_filter(self):
        return IntFilter.default()


class AggregateSumCalculater(AggregateCalculater):
    def aggregate(self, state_value, data_value):
        try:
            if isinstance(data_value, NumberDecimalTypes):
                return state_value + data_value
            return state_value + ensure_number(data_value)
        except:
            if data_value is None:
                return state_value or 0
            if state_value is None:
                if isinstance(data_value, NumberDecimalTypes):
                    return data_value
                return ensure_number(data_value)
            return state_value

    def get_final_filter(self):
        return FloatFilter.default()


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
            if isinstance(data_value, NumberDecimalTypes):
                state_value["sum_value"] += data_value
            else:
                state_value["sum_value"] += ensure_number(data_value)
            return state_value
        except:
            if data_value is None:
                return state_value
            if state_value is None:
                if data_value is None:
                    return None
                if isinstance(data_value, NumberDecimalTypes):
                    return {"count_value": 1, "sum_value": data_value}
                return {"count_value": 1, "sum_value": ensure_number(data_value)}
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

    def get_final_filter(self):
        return FloatFilter.default()


class AggregateGroupConcatCalculater(StateAggregateCalculater):
    def aggregate(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return [str(data_value)]
        state_value.append(str(data_value))
        return state_value

    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return state_value + data_value

    def final_value(self, state_value):
        if not state_value:
            return ""
        return ",".join(state_value)


class AggregateGroupArrayCalculater(StateAggregateCalculater):
    def aggregate(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return [data_value]
        state_value.append(data_value)
        return state_value

    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return state_value + data_value

    def final_value(self, state_value):
        if state_value is None:
            return []
        return state_value

    def get_final_filter(self):
        return FloatFilter.default()


class AggregateGroupUniqArrayCalculater(StateAggregateCalculater):
    def aggregate(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            try:
                return {data_value}
            except Exception as e:
                if isinstance(data_value, list):
                    return {tuple(data_value)}
                raise e
        try:
            state_value.add(data_value)
        except Exception as e:
            if isinstance(data_value, list):
                state_value.add(tuple(data_value))
            else:
                raise e
        return state_value

    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return state_value | data_value

    def final_value(self, state_value):
        if state_value is None:
            return []
        return list(state_value)

    def get_final_filter(self):
        return ArrayFilter.default()


class AggregateGroupBitAndCalculater(AggregateCalculater):
    def aggregate(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        if isinstance(data_value, int):
            return state_value & data_value
        return state_value & ensure_int(data_value)

    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return state_value & data_value

    def get_final_filter(self):
        return IntFilter.default()


class AggregateGroupBitOrCalculater(AggregateCalculater):
    def aggregate(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        if isinstance(data_value, int):
            return state_value | data_value
        return state_value | ensure_int(data_value)

    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return state_value | data_value

    def get_final_filter(self):
        return IntFilter.default()


class AggregateGroupBitXorCalculater(AggregateCalculater):
    def aggregate(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        if isinstance(data_value, int):
            return state_value ^ data_value
        return state_value ^ ensure_int(data_value)

    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return state_value ^ data_value

    def get_final_filter(self):
        return IntFilter.default()


class AggregateJsonArrayaggCalculater(StateAggregateCalculater):
    def format_value(self, value):
        if isinstance(value, datetime.date):
            if isinstance(value, datetime.datetime):
                return value.strftime("%Y-%m-%d %H:%M:%S")
            return value.strftime("%Y-%m-%d")
        if isinstance(value, datetime.time):
            return value.strftime("%H:%M:%S")
        return str(value)

    def aggregate(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return [data_value]
        state_value.append(data_value)
        return state_value

    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return state_value + data_value

    def final_value(self, state_value):
        if state_value is None:
            return None
        return json.dumps(state_value, ensure_ascii=False, default=self.format_value)

    def get_final_filter(self):
        return StringFilter.default()


class AggregateJsonObjectaggCalculater(StateAggregateCalculater):
    def format_value(self, value):
        if isinstance(value, datetime.date):
            if isinstance(value, datetime.datetime):
                return value.strftime("%Y-%m-%d %H:%M:%S")
            return value.strftime("%Y-%m-%d")
        if isinstance(value, datetime.time):
            return value.strftime("%H:%M:%S")
        return str(value)

    def aggregate(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            state_value = {}
        if isinstance(data_value, list):
            if len(data_value) >= 2:
                state_value[self.format_value(data_value[0])] = data_value[1]
            elif data_value:
                state_value[self.format_value(data_value[0])] = None
        else:
            state_value[self.format_value(data_value)] = None
        return state_value

    def reduce(self, state_value, data_value):
        if data_value is None:
            return state_value
        if state_value is None:
            return data_value
        return state_value.update(data_value)

    def final_value(self, state_value):
        if state_value is None:
            return None
        return json.dumps(state_value, ensure_ascii=False, default=self.format_value)

    def get_final_filter(self):
        return StringFilter.default()


class AggregateAnyValueCalculater(StateAggregateCalculater):
    def aggregate(self, state_value, data_value):
        if state_value is None:
            return {"value": data_value}
        return state_value

    def reduce(self, state_value, data_value):
        if state_value is None:
            return data_value
        return state_value

    def final_value(self, state_value):
        if state_value is None:
            return None
        return state_value["value"]