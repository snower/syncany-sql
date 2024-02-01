# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

import uuid
from bson.objectid import ObjectId
from .example import ExampleTestCase


class FunctionsExampleTestCase(ExampleTestCase):
    example_name = "functions"

    def test_generate(self):
        self.execute("generate.sql")

        self.assert_value(2, "a", lambda value: isinstance(value, ObjectId), "data error")
        self.assert_value(2, "b", ObjectId('65bb4211eda4fed2e199073e'), "data error")
        self.assert_value(2, "c", lambda value: isinstance(value, uuid.UUID), "data error")
        self.assert_value(2, "d", uuid.UUID('54aa0a5c-b54f-4628-8391-3756007d5fc3'), "data error")
        self.assert_value(2, "e", lambda value: isinstance(value, int), "data error")
        self.assert_value(2, "f", lambda value: isinstance(value, int), "data error")

        self.assert_value(4, "a", lambda value: isinstance(value, float), "data error")
        self.assert_value(4, "b", lambda value: isinstance(value, int), "data error")
        self.assert_value(4, "c", lambda value: isinstance(value, str), "data error")
        self.assert_value(4, "d", lambda value: isinstance(value, str), "data error")
        self.assert_value(4, "e", lambda value: isinstance(value, str), "data error")
        self.assert_value(4, "f", lambda value: isinstance(value, str), "data error")
        self.assert_value(4, "g", lambda value: isinstance(value, str), "data error")
        self.assert_value(4, "h", lambda value: isinstance(value, bytes), "data error")

        self.assert_value(6, "a", lambda value: isinstance(value, int), "data error")
