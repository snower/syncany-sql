# -*- coding: utf-8 -*-
# 2023/4/24
# create by: snower

from .example import ExampleTestCase


class LoopExampleTestCase(ExampleTestCase):
    example_name = "loop"

    def test_loop(self):
        self.execute("loop.sql")

        self.assert_data(15, [{'n': 1}, {'n': 2}, {'n': 3}, {'n': 4}], "data error")