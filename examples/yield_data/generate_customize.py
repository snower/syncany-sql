# -*- coding: utf-8 -*-
# 2023/5/30
# create by: snower

from syncanysql.calculaters import GenerateCalculater, register_calculater


@register_calculater("range_count")
class RangeGenerateCalculater(GenerateCalculater):
    def calculate(self, count):
        for i in range(count):
            yield i


def range_count(count):
    for i in range(count):
        yield i