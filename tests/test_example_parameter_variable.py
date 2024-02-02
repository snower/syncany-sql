# -*- coding: utf-8 -*-
# 2023/4/25
# create by: snower

import os
from .example import ExampleTestCase


class ParameterVariableExampleTestCase(ExampleTestCase):
    example_name = "parameter_variable"

    def test_parameter_variable(self):
        self.execute("parameter_variable.sql")

        self.assert_data(8, [{'a': 1, 'b': 2, 'c': os.environ.get("PATH", "")}], "data error")

        self.assert_data(9, [{'a': 3, 'b': 4.0, 'c': len(os.environ.get("PATH", "").encode("utf-8"))}], "data error")

        self.assert_data(22, [
            {'a': 2, 'b': [1, 3], 'c': {'a': 1, 'b': 'abc'}, 'd': [{'a': 1, 'b': 'abc'}, {'a': 3, 'b': 'efg'}]}],
                         "data error")

    def test_parameter_assign(self):
        self.execute("parameter_assign.sql")

        self.assert_data(4, [{'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 4}, {'a': 4, 'b': 5, 'c': 6, 'd': 7, 'e': 7},
                             {'a': 7, 'b': 8, 'c': 9, 'd': 10, 'e': 10}, {'a': 10, 'b': 11, 'c': 12, 'd': 13, 'e': 13},
                             {'a': 13, 'b': 14, 'c': 15, 'd': 16, 'e': 16}], "data error")

        self.assert_data(6, [{'a': 1, 'b': None, 'c': 1, 'd': 17}, {'a': 2, 'b': 1, 'c': 2, 'd': 18},
                             {'a': 3, 'b': 2, 'c': 3, 'd': 19}, {'a': 4, 'b': 3, 'c': 4, 'd': 20},
                             {'a': 5, 'b': 4, 'c': 5, 'd': 21}], "data error")

        self.assert_data(8, [{'a': 1, 'b': None, 'c': 1, 'd': 22}, {'a': 2, 'b': 1, 'c': 2, 'd': 23},
                             {'a': 3, 'b': 2, 'c': 3, 'd': 24}, {'a': 4, 'b': 3, 'c': 4, 'd': 25},
                             {'a': 5, 'b': 4, 'c': 5, 'd': 26}], "data error")

        self.assert_data(14, [
            {'uid': 1, 'cnt': 2, 'amount': 55, 'start_at': '2023-01-12 10:11:12', 'end_at': '2023-07-12 22:11:12'},
            {'uid': 1, 'cnt': 1, 'amount': 322, 'start_at': '2023-07-23 12:11:12', 'end_at': '2023-10-12 15:11:12'},
            {'uid': 2, 'cnt': 2, 'amount': 3450, 'start_at': '2023-03-05 00:11:12', 'end_at': '2023-08-12 15:11:12'}],
                         "data error")