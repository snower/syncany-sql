# -*- coding: utf-8 -*-
# 2023/4/24
# create by: snower

from .example import ExampleTestCase


class StringsExampleTestCase(ExampleTestCase):
    example_name = "strings"

    def test_strings(self):
        self.execute("strings.sql")

        self.assert_data(2, [{"a": "abc", "b": "ab", "c": "abc", "d": "ABC"}], "data error")
        self.assert_data(4, [{"a": "a b c", "b": "aaa", "c": "cba", "d": -1}], "data error")
        self.assert_data(6, [{"a": 1, "b": 1, "c": 1}], "data error")
        self.assert_data(8, [{"a": None, "b": 'a 2023-04-02 10:08:06 2023-04-02 10:08:06 1 0 1 1.23'}], "data error")
