# -*- coding: utf-8 -*-
# 2023/3/21
# create by: snower

from syncany.calculaters.calculater import Calculater


class AggregateKeyCalculater(Calculater):
    def calculate(self, *args):
        if len(args) == 1:
            return args[0]
        return args


class AggregateAddCalculater(Calculater):
    def calculate(self, left_value, right_value):
        try:
            return left_value + right_value
        except:
            if right_value is None:
                return left_value
            if left_value is None:
                return right_value
            try:
                if isinstance(left_value, (int, float)):
                    return left_value + type(left_value)(right_value)
                try:
                    return int(left_value) + int(right_value)
                except:
                    return float(left_value) + float(right_value)
            except:
                return left_value


class AggregateMaxCalculater(Calculater):
    def calculate(self, left_value, right_value):
        try:
            return max(left_value, right_value)
        except:
            if right_value is None:
                return left_value
            if left_value is None:
                return right_value
            try:
                return max(left_value, type(left_value)(right_value))
            except:
                return left_value


class AggregateMinCalculater(Calculater):
    def calculate(self, left_value, right_value):
        try:
            return min(left_value, right_value)
        except:
            if right_value is None:
                return left_value
            if left_value is None:
                return right_value
            try:
                return min(left_value, type(left_value)(right_value))
            except:
                return left_value
