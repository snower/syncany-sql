# -*- coding: utf-8 -*-
# 2023/4/24
# create by: snower

from .example import ExampleTestCase


class AggregateExampleTestCase(ExampleTestCase):
    example_name = "aggregate"

    def test_aggregate(self):
        self.execute("aggregate.sql")

        self.assert_data(2, [{'avg_amount': 7.2, 'cnt': 6, 'max_amount': 9.6, 'min_amount': 3, 'total_amount': 43.2}], "data error")
        self.assert_data(4, [{'avg_amount': 6.9, 'cnt': 4, 'max_amount': 9.6, 'min_amount': 3, 'total_amount': 27.6, 'uid': 2},
                             {'avg_amount': 7.8, 'cnt': 2, 'max_amount': 8, 'min_amount': 7.6, 'total_amount': 15.6, 'uid': 1}], "data error")
        self.assert_data(6, [{'amount': 9.6, 'order_id': 1, 'uid': 2}, {'amount': 7.6, 'order_id': 2, 'uid': 1},
                             {'amount': 3, 'order_id': 3, 'uid': 2}, {'amount': 8, 'order_id': 4, 'uid': 1},
                             {'amount': 8, 'order_id': 5, 'uid': 2}, {'amount': 7, 'order_id': 6, 'uid': 2}], "data error")
        self.assert_data(8, [{'ucnt': 2}], "data error")
        self.assert_data(10, [{'gcnt': 2, 'uid': 2}, {'gcnt': 1, 'uid': 1}], "data error")
        self.assert_data(12, [{'gcnt': 1, 'uid': 1}], "data error")
        self.assert_data(14, [{'gcnt': 2, 'uid': 2}], "data error")
        self.assert_data(16, [{'avg_amount': 0.069, 'uid': 2}, {'avg_amount': 0.078, 'uid': 1}], "data error")
        self.assert_data(18, [{'cnt': 6, 'goods_name': '青菜', 'name': '李四'}], "data error")
        self.assert_data(21, [{'cnt': 2, 'goods_name': '青菜', 'name': '李四'}, {'cnt': 2, 'goods_name': '青菜', 'name': '王五'},
                              {'cnt': 2, 'goods_name': '白菜', 'name': '李四'}], "data error")
        self.assert_data(24, [{'goods_name': '青菜', 'name': '李四'}, {'goods_name': '青菜', 'name': '王五'},
                              {'goods_name': '白菜', 'name': '李四'}], "data error")
        self.assert_data(27, [{'cnt': 2, 'goods_name': '青菜', 'name': '李四'}, {'cnt': 1, 'goods_name': '白菜', 'name': '李四'}], "data error")

