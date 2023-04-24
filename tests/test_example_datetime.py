# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

import datetime
from .example import ExampleTestCase


class DatetimeExampleTestCase(ExampleTestCase):
    example_name = "datetime"

    def test_datetime(self):
        self.execute("datetime.sql")

        self.assert_value(2, "NOW()", lambda value: isinstance(value, datetime.datetime), "data error")

        self.assert_value(4, "NOW(0)", lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(4, "NOW('-1d')", lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(4, "NOW('+3d')", lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(4, "NOW('-3d', 0)", lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(4, "NOW('-3d', 0, 10, 11)", lambda value: isinstance(value, datetime.datetime), "data error")

        self.assert_value(6, "DATE_ADD(NOW(), 1, '')", lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(6, "ADDDATE(NOW(), INTERVAL '20' DAY)", lambda value: isinstance(value, datetime.datetime),
                          "data error")
        self.assert_value(6, "DATE_SUB(NOW(), INTERVAL '10' DAY)", lambda value: isinstance(value, datetime.datetime),
                          "data error")
        self.assert_value(6, "SUBDATE(NOW(), INTERVAL '7' MONTH)", lambda value: isinstance(value, datetime.datetime),
                          "data error")

        self.assert_value(8, "ADDTIME(NOW(), '10:00')", lambda value: isinstance(value, datetime.datetime),
                          "data error")
        self.assert_value(8, "SUBTIME(NOW(), '1 10:00')", lambda value: isinstance(value, datetime.datetime),
                          "data error")

        self.assert_value(10, "DATE_FORMAT(DATETIME('2023-04-24 17:07:08'), '%Y-%m-%d %H:%M:%S')",
                          '2023-04-24 17:07:08', "data error")
        self.assert_value(10, "TIME_FORMAT(DATETIME('2023-04-24 17:07:08'), '%H:%M:%S')", '17:07:08', "data error")
        self.assert_value(10, "TIME_TO_SEC('10:11:00')", 36660, "data error")
        self.assert_value(10, "SEC_TO_TIME(234)", '00:03:54', "data error")

        self.assert_value(12, "CURDATE()", lambda value: isinstance(value, datetime.date), "data error")
        self.assert_value(12, "CURRENT_DATE", lambda value: isinstance(value, datetime.date), "data error")
        self.assert_value(12, "CURRENT_TIME()", lambda value: isinstance(value, datetime.time), "data error")
        self.assert_value(12, "CURTIME()", lambda value: isinstance(value, datetime.time), "data error")

        self.assert_value(14, "FROM_UNIXTIME(1677833819)", lambda value: isinstance(value, datetime.datetime),
                          "data error")
        self.assert_value(14, "UNIX_TIMESTAMP()", lambda value: isinstance(value, int), "data error")
        self.assert_value(14, "UNIX_TIMESTAMP(NOW())", lambda value: isinstance(value, int), "data error")
        self.assert_value(14, "CURRENT_TIMESTAMP()", lambda value: isinstance(value, datetime.datetime), "data error")

        self.assert_value(16, "UTC_DATE()", lambda value: isinstance(value, datetime.date), "data error")
        self.assert_value(16, "UTC_TIME()", lambda value: isinstance(value, datetime.time), "data error")
        self.assert_value(16, "UTC_TIMESTAMP()", lambda value: isinstance(value, datetime.datetime), "data error")

        self.assert_value(18, "DATE(NOW())", lambda value: isinstance(value, datetime.date), "data error")
        self.assert_value(18, "DATETIME(NOW())", lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(18, "TIME(NOW())", lambda value: isinstance(value, datetime.time), "data error")
        self.assert_value(18, "DATETIME(DATE(NOW()))", lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(18, "DATETIME(TIME(NOW()))", lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(18, "DATE(TIME(NOW()))", None, "data error")
        self.assert_value(18, "TIME(DATE(NOW()))", lambda value: isinstance(value, datetime.time), "data error")