# -*- coding: utf-8 -*-
# 2023/7/3
# create by: snower

import datetime
import pytz
from syncany.utils import ensure_timezone, get_timezone
from .example import ExampleTestCase

gmt8 = pytz.timezone("Etc/GMT-8")


class TimeWindowExampleTestCase(ExampleTestCase):
    example_name = "time_window"

    def test_time_window(self):
        self.execute("time_window.sql")

        self.assert_value(2, "a", lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(2, "b", lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(2, "c", lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(2, "d", lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(2, "e", lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(2, "f", lambda value: isinstance(value, datetime.datetime), "data error")

        self.assert_value(4, "a", ensure_timezone(datetime.datetime(2023, 7, 3, 12, 24, 27, tzinfo=get_timezone())),
                          "data error")
        self.assert_value(4, "b", ensure_timezone(datetime.datetime(2023, 7, 3, 12, 24, 15, tzinfo=get_timezone())),
                          "data error")
        self.assert_value(4, "c", ensure_timezone(datetime.datetime(2023, 7, 3, 12, 24, tzinfo=get_timezone())),
                          "data error")
        self.assert_value(4, "d", ensure_timezone(datetime.datetime(2023, 7, 3, 12, 15, tzinfo=get_timezone())),
                          "data error")
        self.assert_value(4, "e", ensure_timezone(datetime.datetime(2023, 7, 3, 12, 0, tzinfo=get_timezone())),
                          "data error")
        self.assert_value(4, "f", ensure_timezone(datetime.datetime(2023, 7, 3, 0, 0, tzinfo=get_timezone())),
                          "data error")

        self.assert_value(7, "a", ensure_timezone(datetime.datetime(2023, 7, 3, 12, 24, 30, tzinfo=get_timezone())),
                          "data error")
        self.assert_value(7, "b", ensure_timezone(datetime.datetime(2023, 7, 3, 12, 25, tzinfo=get_timezone())),
                          "data error")
        self.assert_value(7, "c", ensure_timezone(datetime.datetime(2023, 7, 3, 12, 27, tzinfo=get_timezone())),
                          "data error")
        self.assert_value(7, "d", ensure_timezone(datetime.datetime(2023, 7, 3, 13, 0, tzinfo=get_timezone())),
                          "data error")
        self.assert_value(7, "e", ensure_timezone(datetime.datetime(2023, 7, 3, 18, 0, tzinfo=get_timezone())),
                          "data error")
        self.assert_value(7, "f", ensure_timezone(datetime.datetime(2023, 7, 6, 0, 0, tzinfo=get_timezone())),
                          "data error")

        self.assert_value(10, "TIME_WINDOW('15M', create_time)",
                          ensure_timezone(datetime.datetime(2018, 9, 15, 12, 15, tzinfo=gmt8)), "data error")
        self.assert_value(10, "COUNT(*)", 3, "data error")
        self.assert_value(10, "TIME_WINDOW('15M', create_time)",
                          ensure_timezone(datetime.datetime(2018, 9, 15, 14, 15, tzinfo=gmt8)), "data error", index=1)
        self.assert_value(10, "COUNT(*)", 3, "data error", index=1)
        self.assert_value(10, "TIME_WINDOW('15M', create_time)",
                          ensure_timezone(datetime.datetime(2018, 9, 13, 16, 45, tzinfo=gmt8)), "data error", index=2)
        self.assert_value(10, "COUNT(*)", 3, "data error", index=2)
        self.assert_value(10, "TIME_WINDOW('15M', create_time)",
                          ensure_timezone(datetime.datetime(2018, 9, 13, 17, 0, tzinfo=gmt8)), "data error", index=3)
        self.assert_value(10, "COUNT(*)", 3, "data error", index=3)
        self.assert_value(10, "TIME_WINDOW('15M', create_time)",
                          ensure_timezone(datetime.datetime(2018, 9, 14, 16, 15, tzinfo=gmt8)), "data error", index=4)
        self.assert_value(10, "COUNT(*)", 3, "data error", index=4)
        self.assert_value(10, "TIME_WINDOW('15M', create_time)",
                          ensure_timezone(datetime.datetime(2020, 2, 25, 15, 45, tzinfo=gmt8)), "data error", index=5)
        self.assert_value(10, "COUNT(*)", 1, "data error", index=5)
        self.assert_value(10, "TIME_WINDOW('15M', create_time)",
                          ensure_timezone(datetime.datetime(2020, 2, 25, 16, 0, tzinfo=gmt8)), "data error", index=6)
        self.assert_value(10, "COUNT(*)", 2, "data error", index=6)
        self.assert_value(10, "TIME_WINDOW('15M', create_time)",
                          ensure_timezone(datetime.datetime(2020, 2, 25, 16, 45, tzinfo=gmt8)), "data error", index=7)
        self.assert_value(10, "COUNT(*)", 1, "data error", index=7)
        self.assert_value(10, "TIME_WINDOW('15M', create_time)",
                          ensure_timezone(datetime.datetime(2020, 5, 16, 15, 15, tzinfo=gmt8)), "data error", index=8)
        self.assert_value(10, "COUNT(*)", 1, "data error", index=8)
        self.assert_value(10, "TIME_WINDOW('15M', create_time)",
                          ensure_timezone(datetime.datetime(2020, 5, 17, 15, 0, tzinfo=gmt8)), "data error", index=9)
        self.assert_value(10, "COUNT(*)", 2, "data error", index=9)
