# -*- coding: utf-8 -*-
# 2023/4/25
# create by: snower

from .example import ExampleTestCase


class YieldDataExampleTestCase(ExampleTestCase):
    example_name = "yield_data"

    def test_type_annotation(self):
        self.execute("yield_data.sql")

        self.assert_data(5, [{'a': 'a', 'b': 1}, {'a': 'b', 'b': 2}, {'a': 'c', 'b': 3}], "data error")

        self.assert_data(11, [{'a': '青菜', 'b': '青菜'}, {'a': '白菜', 'b': '白菜'}, {'a': '青菜', 'b': '青菜'}], "data error")

        self.assert_data(13, [{'a': 1}, {'a': 2}, {'a': 3}], "data error")

        self.assert_data(17, [{'RANGE_COUNT(3)': 0, 'GENERATE_CUSTOMIZE$RANGE_COUNT(4)': 0},
                              {'RANGE_COUNT(3)': 1, 'GENERATE_CUSTOMIZE$RANGE_COUNT(4)': 1},
                              {'RANGE_COUNT(3)': 2, 'GENERATE_CUSTOMIZE$RANGE_COUNT(4)': 2},
                              {'RANGE_COUNT(3)': None, 'GENERATE_CUSTOMIZE$RANGE_COUNT(4)': 3}], "data error")
