# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class GetValueExampleTestCase(ExampleTestCase):
    example_name = "get_value"

    def test_get_value(self):
        self.execute("get_value.sql")

        self.assert_data(4, [
            {'orderId': 3243243, 'username': 'snower', 'payId': '23232323232323', 'refundId': ['111111']}],
                         "data error")
        self.assert_data(6, [
            {'orderId': 3243243, 'username': 'snower', 'payId': '23232323232323', 'refundId': ['111111']}],
                         "data error")
        self.assert_data(8, [{'mobile1': '12345678911', 'mobile2': '12345678911'}], "data error")
        self.assert_data(10, [{'payTypes1': 1, 'payTypes2': 1, 'payTypes3': 1}], "data error")
        self.assert_data(12, [{'payTypes1': [1, 3], 'payTypes2': [1, 3], 'payTypes3': [1, 3]}], "data error")
        self.assert_data(14, [{'payTypes1': [4, 3], 'payTypes2': [4, 3], 'payTypes3': [4, 3]}], "data error")
        self.assert_data(16, [{'channel1': 'weixin', 'channel2': 'weixin'}], "data error")
