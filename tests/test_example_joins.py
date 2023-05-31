# -*- coding: utf-8 -*-
# 2023/5/31
# create by: snower

from .example import ExampleTestCase


class JsonExampleTestCase(ExampleTestCase):
    example_name = "joins"

    def test_left_join(self):
        self.execute("left_join.sql")

        self.assert_data(2, [{'order_id': 1, 'name': '李四', 'goods_name': '青菜'},
                             {'order_id': 2, 'name': '王五', 'goods_name': '青菜'},
                             {'order_id': 3, 'name': '李四', 'goods_name': '白菜'},
                             {'order_id': 4, 'name': '王五', 'goods_name': '青菜'},
                             {'order_id': 5, 'name': '李四', 'goods_name': '青菜'},
                             {'order_id': 6, 'name': '李四', 'goods_name': '白菜'}], "data error")

        self.assert_data(7, [{'order_id': 1, 'name': '李四', 'goods_name': '青菜'},
                             {'order_id': 2, 'name': '王五', 'goods_name': '青菜'},
                             {'order_id': 3, 'name': '李四', 'goods_name': '白菜'},
                             {'order_id': 4, 'name': '王五', 'goods_name': '青菜'},
                             {'order_id': 5, 'name': '李四', 'goods_name': '青菜'},
                             {'order_id': 6, 'name': '李四', 'goods_name': '白菜'}], "data error")

        self.assert_data(12, [{'order_id': 1, 'name': '李四', 'goods_name': '青菜', 'cnt': 4},
                              {'order_id': 2, 'name': '王五', 'goods_name': '青菜', 'cnt': 2}], "data error")

        self.assert_data(18, [{'order_id': 1, 'name': '李四', 'goods_name': '青菜', 'ucnt': 2, 'total_amount': 33.2},
                              {'order_id': 3, 'name': '李四', 'goods_name': '白菜', 'ucnt': 1, 'total_amount': 10}],
                         "data error")

        self.assert_data(24, [{'name': '李四', 'goods_name': '青菜'}, {'name': '王五', 'goods_name': '青菜'},
                              {'name': '李四', 'goods_name': '白菜'}], "data error")

    def test_right_join(self):
        self.execute("right_join.sql")

        self.assert_data(2, [{'order_id': 1, 'name': '李四', 'goods_name': '青菜'},
                             {'order_id': 2, 'name': '王五', 'goods_name': '青菜'},
                             {'order_id': 3, 'name': '李四', 'goods_name': '白菜'},
                             {'order_id': 4, 'name': '王五', 'goods_name': '青菜'},
                             {'order_id': 5, 'name': '李四', 'goods_name': '青菜'},
                             {'order_id': 6, 'name': '李四', 'goods_name': '白菜'}], "data error")

        self.assert_data(7, [{'order_id': 1, 'name': '李四', 'goods_name': '青菜', 'cnt': 4},
                             {'order_id': 2, 'name': '王五', 'goods_name': '青菜', 'cnt': 2}], "data error")

        self.assert_data(12, [{'order_id': 1, 'name': '李四', 'goods_name': '青菜', 'cnt': 2, 'total_amount': 33.2},
                              {'order_id': 3, 'name': '李四', 'goods_name': '白菜', 'cnt': 1, 'total_amount': 10}],
                         "data error")

    def test_inner_join(self):
        self.execute("inner_join.sql")

        self.assert_data(2, [{'goods_name': '青菜', 'order_id': 1, 'name': '李四'},
                             {'goods_name': '青菜', 'order_id': 2, 'name': '王五'},
                             {'goods_name': '青菜', 'order_id': 4, 'name': '王五'},
                             {'goods_name': '青菜', 'order_id': 5, 'name': '李四'},
                             {'goods_name': '白菜', 'order_id': 3, 'name': '李四'},
                             {'goods_name': '白菜', 'order_id': 6, 'name': '李四'},
                             {'goods_name': '萝卜', 'order_id': None, 'name': None}], "data error")

        self.assert_data(7, [{'goods_name': '青菜', 'order_id': 1, 'name': '李四'},
                             {'goods_name': '青菜', 'order_id': 2, 'name': '王五'},
                             {'goods_name': '青菜', 'order_id': 4, 'name': '王五'},
                             {'goods_name': '青菜', 'order_id': 5, 'name': '李四'},
                             {'goods_name': '白菜', 'order_id': 3, 'name': '李四'},
                             {'goods_name': '白菜', 'order_id': 6, 'name': '李四'}], "data error")

        self.assert_data(12, [{'goods_name': '青菜', 'latest_order_id': 5, 'name': '李四', 'order_cnt': 4, 'user_cnt': 2,
                               'total_amount': 33.2},
                              {'goods_name': '白菜', 'latest_order_id': 6, 'name': '李四', 'order_cnt': 2, 'user_cnt': 1,
                               'total_amount': 10},
                              {'goods_name': '萝卜', 'latest_order_id': None, 'name': None, 'order_cnt': 0, 'user_cnt': 0,
                               'total_amount': 0}], "data error")

        self.assert_data(18, [{'goods_name': '青菜', 'latest_order_id': 6, 'name': '李四', 'order_cnt': 4},
                              {'goods_name': '青菜', 'latest_order_id': 4, 'name': '王五', 'order_cnt': 2}], "data error")