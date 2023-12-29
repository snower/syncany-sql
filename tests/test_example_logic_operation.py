# -*- coding: utf-8 -*-
# 2023/9/27
# create by: snower

from .example import ExampleTestCase


class LogicOperationExampleTestCase(ExampleTestCase):
    example_name = "logic_operation"

    def test_logic_operation(self):
        self.execute("logic_operation.sql")

        self.assert_data(2, [{'a': 1, 'b': 1, 'c': 0, 'd': 1, 'f': 1}], "data error")

        self.assert_data(4, [{'a': 1, 'b': 0}], "data error")

        self.assert_data(6, [{'a': 1, 'b': 1, 'c': 0, 'd': 1}], "data error")

        self.assert_data(8, [{'a': 0, 'b': 0, 'c': 1, 'd': 0, 'f': 0}], "data error")

        self.assert_data(10, [{'a': 0, 'b': 1, 'c': 0, 'd': 0}], "data error")

        self.assert_data(12, [{'a': 1, 'b': 1, 'c': 1, 'd': 1, 'e': 1, 'f': 0}], "data error")

        self.assert_data(14, [{'a': 0, 'b': 0, 'c': 0, 'd': 0, 'e': 0, 'f': 1}], "data error")

        self.assert_data(16, [{'a': 1, 'b': 0, 'c': 1, 'd': 0}], "data error")

        self.assert_data(18, [{'a': 0, 'b': 1, 'c': 0, 'd': 1}], "data error")

        self.assert_data(20, [{'a': 1, 'b': 'B'}, {'a': 1, 'b': 'C'}], "data error")

        self.assert_data(22, [{'a': 36.2, 'b': 1, 'c': 'C'}], "data error")