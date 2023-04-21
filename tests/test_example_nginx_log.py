# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class NginxLogExampleTestCase(ExampleTestCase):
    example_name = "nginx-log"

    def test_ip_top3(self):
        self.execute("ip-top-3.sql")
