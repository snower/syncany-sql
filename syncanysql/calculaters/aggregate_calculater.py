# -*- coding: utf-8 -*-
# 2023/3/21
# create by: snower

from syncany.calculaters.calculater import Calculater


class AggregateKeyCalculater(Calculater):
    def calculate(self):
        if len(self.args) == 1:
            return self.args[0]
        return self.args
