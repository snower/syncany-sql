# -*- coding: utf-8 -*-
# 2023/5/30
# create by: snower

from syncany.calculaters.calculater import Calculater


class GenerateCalculater(Calculater):
    pass


class GenerateYieldArrayCalculater(GenerateCalculater):
    def calculate(self, values):
        if isinstance(values, list):
            for value in values:
                yield value
        else:
            yield values
