# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

from syncanysql import Calculater, register_calculater

@register_calculater("ext_sum_func")
class SumCalculater(Calculater):
    def calculate(self, a, b):
        return a + b
