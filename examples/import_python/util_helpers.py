# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import time
from syncanysql import Calculater, register_calculater

@register_calculater("util_helpers_sum")
class SumCalculater(Calculater):
    def calculate(self, a, b):
        return a + b

def sum(a, b):
    return a + b

LoadTime = time.time()

class A(object):
    def sum(self, a, b):
        return a + b