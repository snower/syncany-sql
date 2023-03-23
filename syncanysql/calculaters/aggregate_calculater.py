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
        if left_value is None:
            return right_value
        return left_value + right_value


class AggregateMaxCalculater(Calculater):
    def calculate(self, left_value, right_value):
        if left_value is None:
            return right_value
        return max(left_value, right_value)


class AggregateMinCalculater(Calculater):
    def calculate(self, left_value, right_value):
        if left_value is None:
            return right_value
        return min(left_value, right_value)
