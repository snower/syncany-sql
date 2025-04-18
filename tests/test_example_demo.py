# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class DemoExampleTestCase(ExampleTestCase):
    example_name = "demo"

    def test_demo(self):
        self.execute("demo.sql")

        self.assert_data(3, [{'site_id': 8, 'site_name': '黄豆网', 'site_amount': 17.04, 'timeout_at': '16:00:00',
                              'vip_timeout_at': '11:00:00'},
                             {'site_id': 15, 'site_name': '青菜网', 'site_amount': 7.2, 'timeout_at': '15:00:00',
                              'vip_timeout_at': '11:00:00'},
                             {'site_id': 21, 'site_name': '去啥网', 'site_amount': 0, 'timeout_at': '16:00:00',
                              'vip_timeout_at': '11:00:00'},
                             {'site_id': 26, 'site_name': '汽车网', 'site_amount': 0, 'timeout_at': '16:00:00',
                              'vip_timeout_at': '11:00:00'},
                             {'site_id': 28, 'site_name': '火箭网', 'site_amount': 0, 'timeout_at': '15:00:00',
                              'vip_timeout_at': '10:00:00'},
                             {'site_id': 34, 'site_name': '卫星网', 'site_amount': 11.2, 'timeout_at': '16:40:00',
                              'vip_timeout_at': '11:20:00'}], "data error")

    def test_demo2(self):
        self.execute("demo2.sql")

        self.assert_data(2, [{'site_id': 8, 'site_name': '黄豆网'}, {'site_id': 15, 'site_name': '青菜网'},
                             {'site_id': 21, 'site_name': '去啥网'}, {'site_id': 26, 'site_name': '汽车网'},
                             {'site_id': 8, 'site_name': '黄豆网'}, {'site_id': 21, 'site_name': '去啥网'},
                             {'site_id': 26, 'site_name': '汽车网'}, {'site_id': 15, 'site_name': '青菜网'},
                             {'site_id': 28, 'site_name': '火箭网'}, {'site_id': 15, 'site_name': '青菜网'},
                             {'site_id': 28, 'site_name': '火箭网'}, {'site_id': 34, 'site_name': '卫星网'},
                             {'site_id': 34, 'site_name': '卫星网'}], "data error")

        self.assert_data(5, [{'order_id': 6, 'site_id': 34, 'amount': 11.2, 'status': 0},
                             {'order_id': 1, 'site_id': 8, 'amount': 10, 'status': 0}], "data error")

        self.assert_data(7, [{'order_id': 6, 'site_id': 34, 'amount': 1120.0},
                             {'order_id': 4, 'site_id': 28, 'amount': 470.0}], "data error")

        self.assert_data(9, [{'order_id': 6, 'site_id': 34, 'amount': 1120.0},
                             {'order_id': 1, 'site_id': 8, 'amount': 1000}], "data error")

        self.assert_data(15, [{'site_id': 8, 'site_name': '黄豆网', 'site_amount': 17.04, 'timeout_at': '16:00:00',
                              'vip_timeout_at': '11:00:00'},
                             {'site_id': 15, 'site_name': '青菜网', 'site_amount': 7.2, 'timeout_at': '15:00:00',
                              'vip_timeout_at': '11:00:00'},
                             {'site_id': 21, 'site_name': '去啥网', 'site_amount': 0, 'timeout_at': '16:00:00',
                              'vip_timeout_at': '11:00:00'},
                             {'site_id': 26, 'site_name': '汽车网', 'site_amount': 0, 'timeout_at': '16:00:00',
                              'vip_timeout_at': '11:00:00'},
                             {'site_id': 28, 'site_name': '火箭网', 'site_amount': 0, 'timeout_at': '15:00:00',
                              'vip_timeout_at': '10:00:00'},
                             {'site_id': 34, 'site_name': '卫星网', 'site_amount': 11.2, 'timeout_at': '16:40:00',
                              'vip_timeout_at': '11:20:00'}], "data error")

        self.assert_data(17, [{'site_id': 8, 'site_name': '黄豆网', 'site_amount': 17.04, 'timeout_at': '16:00:00',
                              'vip_timeout_at': '11:00:00'},
                             {'site_id': 15, 'site_name': '青菜网', 'site_amount': 7.2, 'timeout_at': '15:00:00',
                              'vip_timeout_at': '10:00:00'},
                             {'site_id': 21, 'site_name': '去啥网', 'site_amount': 0, 'timeout_at': '16:00:00',
                              'vip_timeout_at': '11:00:00'},
                             {'site_id': 26, 'site_name': '汽车网', 'site_amount': 0, 'timeout_at': '16:00:00',
                              'vip_timeout_at': '11:00:00'},
                             {'site_id': 28, 'site_name': '火箭网', 'site_amount': 0, 'timeout_at': '15:00:00',
                              'vip_timeout_at': '10:00:00'},
                             {'site_id': 34, 'site_name': '卫星网', 'site_amount': 11.2, 'timeout_at': '16:40:00',
                              'vip_timeout_at': '11:20:00'}], "data error")
