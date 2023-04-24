# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class NginxLogExampleTestCase(ExampleTestCase):
    example_name = "nginx-log"

    def test_ip_top3(self):
        self.execute("ip-top-3.sql")

        self.assert_data(3, [{'cnt': 22, 'ip': '54.37.79.75'}, {'cnt': 14, 'ip': '143.110.222.166'},
                             {'cnt': 9, 'ip': '35.216.169.119'}], "data error")
