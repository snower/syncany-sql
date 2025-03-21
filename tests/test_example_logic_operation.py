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

        self.assert_data(20, [{'a': 1, 'b': 'B'}], "data error")

        self.assert_data(22, [{'a': 36.2, 'b': 1, 'c': 'C'}], "data error")

        self.assert_data(24, [{'order_id': 2, 'uid': 1, 'goods_id': 1, 'amount': 0.6}], "data error")

        self.assert_data(26, [{'order_id': 2, 'uid': 1, 'goods_id': 1, 'amount': 0.6, 'status': 0}], "data error")

        self.assert_data(28, [{'a': 0, 'b': 1, 'c': 4, 'd': 'a X c', 'e': 'abc'}], "data error")

        self.assert_data(30, [{'a': 1, 'b': 0, 'c': 12, 'd': 'abc X ghi', 'e': 'def'}], "data error")