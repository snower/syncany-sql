# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

import datetime
from .example import ExampleTestCase


class DatetimeExampleTestCase(ExampleTestCase):
    example_name = "datetime"

    def test_datetime(self):
        self.execute("datetime.sql")

        self.assert_value(2, "NOW()", lambda value: isinstance(value, datetime.datetime), "value error")

        self.assert_value(4, "NOW(0)", lambda value: isinstance(value, datetime.datetime), "value error")
        self.assert_value(4, "NOW('-1d')", lambda value: isinstance(value, datetime.datetime), "value error")
        self.assert_value(4, "NOW('+3d')", lambda value: isinstance(value, datetime.datetime), "value error")
        self.assert_value(4, "NOW('-3d', 0)", lambda value: isinstance(value, datetime.datetime), "value error")
        self.assert_value(4, "NOW('-3d', 0, 10, 11)", lambda value: isinstance(value, datetime.datetime), "value error")