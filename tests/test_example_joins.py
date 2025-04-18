# -*- coding: utf-8 -*-
# 2023/5/31
# create by: snower

from .example import ExampleTestCase


class JoinsExampleTestCase(ExampleTestCase):
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

        self.assert_data(18, [{'order_id': 1, 'name': '李四', 'goods_name': '青菜', 'ucnt': 2, 'SUM(a.amount)': 33.2},
                              {'order_id': 3, 'name': '李四', 'goods_name': '白菜', 'ucnt': 1, 'SUM(a.amount)': 10}],
                         "data error")

        self.assert_data(24, [{'name': '李四', 'goods_name': '青菜'}, {'name': '王五', 'goods_name': '青菜'},
                              {'name': '李四', 'goods_name': '白菜'}], "data error")

        self.assert_data(29, [{'code': '123', 'name': '王五', 'goods_name': '青菜'},
                              {'code': '124', 'name': '李四', 'goods_name': '青菜'},
                              {'code': '125', 'name': '李四', 'goods_name': '青菜'}], "data error")

        self.assert_data(35, [{'order_id': 1, 'name': 'ServiceA'}, {'order_id': 1, 'name': 'ServiceA'},
                              {'order_id': 2, 'name': 'ServiceA'}, {'order_id': 2, 'name': 'ServiceB'}], "data error")

        self.assert_data(40, [{'code': '123', 'name': '王五', 'goods_name': '青菜'},
                              {'code': '124', 'name': '李四', 'goods_name': '青菜'},
                              {'code': '125', 'name': '李四', 'goods_name': '青菜'}], "data error")

        self.assert_data(46, [{'cnt': 2, 'goods_name': '青菜', 'history_type': 1, 'name': '李四', 'order_id': 1,
                               'total_amount': 28.700000000000003},
                              {'cnt': 2, 'goods_name': '白菜', 'history_type': 1, 'name': '李四', 'order_id': 3,
                               'total_amount': 7.3},
                              {'cnt': 3, 'goods_name': '白菜', 'history_type': 0, 'name': '李四', 'order_id': 3,
                               'total_amount': 37.4},
                              {'cnt': 2, 'goods_name': '青菜', 'history_type': 1, 'name': '李四', 'order_id': 5,
                               'total_amount': 89.9}], "data error")

        self.assert_data(52, [{'cnt': 2, 'goods_name': '青菜', 'history_type': 1, 'name': '李四', 'order_id': 1,
                               'total_amount': 28.700000000000003},
                              {'cnt': 2, 'goods_name': '白菜', 'history_type': 1, 'name': '李四', 'order_id': 3,
                               'total_amount': 7.3},
                              {'cnt': 3, 'goods_name': '白菜', 'history_type': 0, 'name': '李四', 'order_id': 3,
                               'total_amount': 37.4},
                              {'cnt': 2, 'goods_name': '青菜', 'history_type': 1, 'name': '李四', 'order_id': 5,
                               'total_amount': 89.9}], "data error")

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

        self.assert_data(12, [{'order_id': 1, 'name': '李四', 'goods_name': '青菜', 'cnt': 2, 'SUM(b.amount)': 33.2},
                              {'order_id': 3, 'name': '李四', 'goods_name': '白菜', 'cnt': 1, 'SUM(b.amount)': 10}],
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

        self.assert_data(12, [{'goods_name': '青菜', 'latest_order_id': 5, 'name': '李四',
                               'SUM(CASE WHEN NOT b.order_id IS NULL THEN 1 ELSE 0 END)': 4, 'user_cnt': 2,
                               'total_amount': 33.2}, {'goods_name': '白菜', 'latest_order_id': 6, 'name': '李四',
                                                       'SUM(CASE WHEN NOT b.order_id IS NULL THEN 1 ELSE 0 END)': 2,
                                                       'user_cnt': 1, 'total_amount': 10},
                              {'goods_name': '萝卜', 'latest_order_id': None, 'name': None,
                               'SUM(CASE WHEN NOT b.order_id IS NULL THEN 1 ELSE 0 END)': 0, 'user_cnt': 0,
                               'total_amount': 0}], "data error")

        self.assert_data(18, [{'goods_name': '青菜', 'latest_order_id': 6, 'name': '李四',
                               'SUM(CASE WHEN NOT b.order_id IS NULL THEN 1 ELSE 0 END)': 4},
                              {'goods_name': '青菜', 'latest_order_id': 4, 'name': '王五',
                               'SUM(CASE WHEN NOT b.order_id IS NULL THEN 1 ELSE 0 END)': 2}], "data error")

        self.assert_data(24, [{'goods_name': '青菜', 'order_id': 1, 'name': '李四'},
                             {'goods_name': '青菜', 'order_id': 2, 'name': '王五'},
                             {'goods_name': '青菜', 'order_id': 4, 'name': '王五'},
                             {'goods_name': '青菜', 'order_id': 5, 'name': '李四'},
                             {'goods_name': '白菜', 'order_id': 3, 'name': '李四'},
                             {'goods_name': '白菜', 'order_id': 6, 'name': '李四'}], "data error")

        self.assert_data(28, [{'goods_name': '青菜', 'order_id': 1, 'name': '李四'},
                              {'goods_name': '青菜', 'order_id': 2, 'name': '王五'},
                              {'goods_name': '青菜', 'order_id': 4, 'name': '王五'},
                              {'goods_name': '青菜', 'order_id': 5, 'name': '李四'},
                              {'goods_name': '白菜', 'order_id': 3, 'name': '李四'},
                              {'goods_name': '白菜', 'order_id': 6, 'name': '李四'}], "data error")
