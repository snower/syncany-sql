# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class JsonExampleTestCase(ExampleTestCase):
    example_name = "json"

    def test_json(self):
        self.execute("json.sql")
