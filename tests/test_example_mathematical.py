# -*- coding: utf-8 -*-
# 2023/4/24
# create by: snower

from .example import ExampleTestCase


class MathematicalExampleTestCase(ExampleTestCase):
    example_name = "mathematical"

    def test_mathematical(self):
        self.execute("mathematical.sql")

        self.assert_data(2, [{"a": 3, "b": 1, "c": 2, "d": 2}], "data error")
        self.assert_data(4, [{"a": 0, "b": 3, "c": 3, "d": -3}], "data error")
        self.assert_data(6, [{"a": 3, "b": 2022, "c": 222}], "data error")
        self.assert_data(8, [{"a": None, "b": 2, "c": 1}], "data error")
        self.assert_data(10, [{"a": 10115201050403, "b": 10115201, "c": 50403}], "data error")
        self.assert_data(12, [{"a": [2, 3, 4], "b": [4, 8, 12]}], "data error")
        self.assert_data(14, [{"a": 5, "b": 3}], "data error")
