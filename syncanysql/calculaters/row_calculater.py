# -*- coding: utf-8 -*-
# 2024/2/2
# create by: snower

from syncany.calculaters.calculater import Calculater


class RowIndexCalculater(Calculater):
    @classmethod
    def instance(cls, name):
        return RowIndexCalculater(name)

    def __init__(self, *args, **kwargs):
        super(RowIndexCalculater, self).__init__(*args, **kwargs)

        self.row_index = 0

    def calculate(self, *args):
        self.row_index += 1
        return self.row_index


class RowLastCalculater(Calculater):
    @classmethod
    def instance(cls, name):
        return RowLastCalculater(name)

    def __init__(self, *args, **kwargs):
        super(RowLastCalculater, self).__init__(*args, **kwargs)

        self.row_last = None

    def calculate(self, *args):
        row_last, self.row_last = self.row_last, (args[0] if len(args) == 1 else args)
        return row_last
