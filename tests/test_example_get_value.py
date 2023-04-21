# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class GetValueExampleTestCase(ExampleTestCase):
    example_name = "get_value"

    def test_get_value(self):
        self.execute("get_value.sql")
