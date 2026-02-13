# -*- coding: utf-8 -*-
# 2025/4/3
# create by: snower

from .example import ExampleTestCase


class PyEvalExampleTestCase(ExampleTestCase):
    example_name = "pyeval"

    def test_pyeval(self):
        self.execute("pyeval.sql")

        self.assert_data(2, [{'a': 3, 'b': '0,1,2,3', 'c': 3, 'd': '123'}],
                         "data error")

        self.assert_data(6, [{'a': 1}], "data error")

        self.assert_data(8, [{'v': 0}, {'v': 2}], "data error")