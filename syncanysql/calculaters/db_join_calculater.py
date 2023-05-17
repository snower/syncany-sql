# -*- coding: utf-8 -*-
# 2023/5/17
# create by: snower

from syncany.calculaters.calculater import Calculater


class DBJoinValueCalculater(Calculater):
    def calculate(self, values):
        if values:
            if isinstance(values[0], list) and len(values) == 1:
                return values[0][0]
            return values[0]
        return None
