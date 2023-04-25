# -*- coding: utf-8 -*-
# 2023/4/25
# create by: snower

import os
from .example import ExampleTestCase


class ParameterVariableExampleTestCase(ExampleTestCase):
    example_name = "parameter_variable"

    def test_type_annotation(self):
        self.execute("parameter_variable.sql")

        self.assert_data(8, [{'a': 1, 'b': 2, 'c': os.environ.get("PATH", "")}], "data error")

        self.assert_data(9, [{'a': 3, 'b': 4.0, 'c': 1154}], "data error")

        self.assert_data(22, [
            {'a': 2, 'b': [1, 3], 'c': {'a': 1, 'b': 'abc'}, 'd': [{'a': 1, 'b': 'abc'}, {'a': 3, 'b': 'efg'}]}],
                         "data error")