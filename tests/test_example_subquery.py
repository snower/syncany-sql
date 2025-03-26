# -*- coding: utf-8 -*-
# 2025/3/25
# create by: snower

from .example import ExampleTestCase


class SubqueryExampleTestCase(ExampleTestCase):
    example_name = "subquery"

    def test_strings(self):
        self.execute("subquery.sql")

        self.assert_data(1, [{'cnt': 4, 'latest_order_id': 6, 'total_amount': 27.6, 'uid': 2}], "data error")
        self.assert_data(5, [{'amount': 3,
                              'history_count': 5,
                              'history_exists': 1,
                              'order_id': 3,
                              'uid': 2}], "data error")
        self.assert_data(9, [{'goods_id': 1, 'goods_name': '青菜', 'order_count': 4, 'order_exists': 1},
                             {'goods_id': 2, 'goods_name': '白菜', 'order_count': 2, 'order_exists': 1},
                             {'goods_id': 3, 'goods_name': '萝卜', 'order_count': None, 'order_exists': 0}],
                         "data error")
        self.assert_data(13, [{'amount': 3,
                               'history_count': 5,
                               'history_exists': 1,
                               'order_id': 3,
                               'uid': 2}], "data error")
        self.assert_data(17, [{'goods_id': 1, 'goods_name': '青菜', 'order_count': 4, 'order_exists': 1},
                              {'goods_id': 2, 'goods_name': '白菜', 'order_count': 2, 'order_exists': 1},
                              {'goods_id': 3, 'goods_name': '萝卜', 'order_count': None, 'order_exists': 0}],
                         "data error")
        self.assert_data(21, [{'cnt': 1, 'order_id': 1, 'total_amount': 9.6},
                              {'cnt': 1, 'order_id': 2, 'total_amount': 7.6},
                              {'cnt': 1, 'order_id': 3, 'total_amount': 3}],
                         "data error")
        self.assert_data(23, [{'total_amount': 15.6, 'uid': 1}, {'total_amount': 27.6, 'uid': 2}],
                         "data error")
        self.assert_data(28, [{'amount': 3,
                               'has_history': 1,
                               'history_exists': 1,
                               'order_id': 3,
                               'uid': 2}], "data error")
