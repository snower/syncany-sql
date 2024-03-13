# -*- coding: utf-8 -*-
# 2023/6/2
# create by: snower

from .example import ExampleTestCase


class WindowAggregateExampleTestCase(ExampleTestCase):
    example_name = "window_aggregate"

    def test_window(self):
        self.execute("window.sql")

        self.assert_data(3, [{'order_id': 1, 'uid': 2, 'goods_id': 1, 'order_cnt': 6, 'uorder_cnt': 4, 'goods_cnt': 2},
                             {'order_id': 2, 'uid': 1, 'goods_id': 1, 'order_cnt': 6, 'uorder_cnt': 2, 'goods_cnt': 1},
                             {'order_id': 3, 'uid': 2, 'goods_id': 2, 'order_cnt': 6, 'uorder_cnt': 4, 'goods_cnt': 2},
                             {'order_id': 4, 'uid': 1, 'goods_id': 1, 'order_cnt': 6, 'uorder_cnt': 2, 'goods_cnt': 1},
                             {'order_id': 5, 'uid': 2, 'goods_id': 1, 'order_cnt': 6, 'uorder_cnt': 4, 'goods_cnt': 2},
                             {'order_id': 6, 'uid': 2, 'goods_id': 2, 'order_cnt': 6, 'uorder_cnt': 4, 'goods_cnt': 2}],
                         "data error")

        self.assert_data(6, [{'order_id': 1, 'uid': 2, 'goods_id': 1, 'order_cnt': 6, 'uorder_cnt': 4, 'goods_cnt': 2},
                             {'order_id': 2, 'uid': 1, 'goods_id': 1, 'order_cnt': 5, 'uorder_cnt': 2, 'goods_cnt': 1},
                             {'order_id': 3, 'uid': 2, 'goods_id': 2, 'order_cnt': 4, 'uorder_cnt': 3, 'goods_cnt': 2},
                             {'order_id': 4, 'uid': 1, 'goods_id': 1, 'order_cnt': 3, 'uorder_cnt': 1, 'goods_cnt': 1},
                             {'order_id': 5, 'uid': 2, 'goods_id': 1, 'order_cnt': 2, 'uorder_cnt': 2, 'goods_cnt': 2},
                             {'order_id': 6, 'uid': 2, 'goods_id': 2, 'order_cnt': 1, 'uorder_cnt': 1, 'goods_cnt': 1}],
                         "data error")

        self.assert_data(9, [
            {'order_id': 1, 'uid': 2, 'goods_id': 1, 'rn': 1, 'rk': 1, 'drk': 1, 'prk': 4.833333333333333,
             'cd': 1.1666666666666667},
            {'order_id': 2, 'uid': 1, 'goods_id': 1, 'rn': 4, 'rk': 4, 'drk': 4, 'prk': 3.8, 'cd': 4.166666666666666},
            {'order_id': 3, 'uid': 2, 'goods_id': 2, 'rn': 6, 'rk': 6, 'drk': 6, 'prk': 2.75, 'cd': 6.166666666666667},
            {'order_id': 4, 'uid': 1, 'goods_id': 1, 'rn': 2, 'rk': 2, 'drk': 2, 'prk': 1.6666666666666665,
             'cd': 2.166666666666667},
            {'order_id': 5, 'uid': 2, 'goods_id': 1, 'rn': 3, 'rk': 3, 'drk': 3, 'prk': 0.5, 'cd': 3.1666666666666665},
            {'order_id': 6, 'uid': 2, 'goods_id': 2, 'rn': 5, 'rk': 5, 'drk': 5, 'prk': 0, 'cd': 5.166666666666667}],
                         "data error")

        self.assert_data(13,
                         [{'order_id': 1, 'uid': 2, 'goods_id': 1, 'rn': 1, 'rk': 1, 'drk': 1, 'prk': 0, 'cd': 1.25},
                          {'order_id': 2, 'uid': 1, 'goods_id': 1, 'rn': 2, 'rk': 2, 'drk': 2, 'prk': 0.5, 'cd': 2.5},
                          {'order_id': 3, 'uid': 2, 'goods_id': 2, 'rn': 4, 'rk': 4, 'drk': 4, 'prk': 2.75, 'cd': 4.25},
                          {'order_id': 4, 'uid': 1, 'goods_id': 1, 'rn': 1, 'rk': 1, 'drk': 1, 'prk': 0, 'cd': 1.5},
                          {'order_id': 5, 'uid': 2, 'goods_id': 1, 'rn': 2, 'rk': 2, 'drk': 2, 'prk': 0.5, 'cd': 2.25},
                          {'order_id': 6, 'uid': 2, 'goods_id': 2, 'rn': 3, 'rk': 3, 'drk': 3,
                           'prk': 1.6666666666666665, 'cd': 3.25}], "data error")

        self.assert_data(17, [{'order_id': 1, 'uid': 2, 'goods_id': 1, 'a': 2, 'b': 0.0, 'c': 125.0},
                              {'order_id': 2, 'uid': 1, 'goods_id': 1, 'a': 4, 'b': 0.1, 'c': 250.0},
                              {'order_id': 3, 'uid': 2, 'goods_id': 2, 'a': 8, 'b': 1.1, 'c': 425.0},
                              {'order_id': 4, 'uid': 1, 'goods_id': 1, 'a': 2, 'b': 0.0, 'c': 150.0},
                              {'order_id': 5, 'uid': 2, 'goods_id': 1, 'a': 4, 'b': 0.1, 'c': 225.0},
                              {'order_id': 6, 'uid': 2, 'goods_id': 2, 'a': 6, 'b': 0.5, 'c': 325.0}], "data error")

        self.assert_data(21, [
            {'order_id': 1, 'name': '李四', 'goods_name': '青菜', 'order_cnt': 6, 'uorder_cnt': 4, 'goods_cnt': 2},
            {'order_id': 2, 'name': '王五', 'goods_name': '青菜', 'order_cnt': 6, 'uorder_cnt': 2, 'goods_cnt': 1},
            {'order_id': 3, 'name': '李四', 'goods_name': '白菜', 'order_cnt': 6, 'uorder_cnt': 4, 'goods_cnt': 2},
            {'order_id': 4, 'name': '王五', 'goods_name': '青菜', 'order_cnt': 6, 'uorder_cnt': 2, 'goods_cnt': 1},
            {'order_id': 5, 'name': '李四', 'goods_name': '青菜', 'order_cnt': 6, 'uorder_cnt': 4, 'goods_cnt': 2},
            {'order_id': 6, 'name': '李四', 'goods_name': '白菜', 'order_cnt': 6, 'uorder_cnt': 4, 'goods_cnt': 2}],
                         "data error")

        self.assert_data(27, [
            {'order_id': 1, 'name': '李四', 'goods_name': '青菜', 'order_cnt': 6, 'uorder_cnt': 4, 'goods_cnt': 2},
            {'order_id': 2, 'name': '王五', 'goods_name': '青菜', 'order_cnt': 5, 'uorder_cnt': 2, 'goods_cnt': 1},
            {'order_id': 3, 'name': '李四', 'goods_name': '白菜', 'order_cnt': 4, 'uorder_cnt': 3, 'goods_cnt': 2},
            {'order_id': 4, 'name': '王五', 'goods_name': '青菜', 'order_cnt': 3, 'uorder_cnt': 1, 'goods_cnt': 1},
            {'order_id': 5, 'name': '李四', 'goods_name': '青菜', 'order_cnt': 2, 'uorder_cnt': 2, 'goods_cnt': 2},
            {'order_id': 6, 'name': '李四', 'goods_name': '白菜', 'order_cnt': 1, 'uorder_cnt': 1, 'goods_cnt': 1}],
                         "data error")

        self.assert_data(33, [
            {'order_id': 1, 'name': '李四', 'goods_name': '青菜', 'rn': 1, 'rk': 1, 'drk': 1, 'prk': 4.833333333333333,
             'cd': 1.1666666666666667},
            {'order_id': 2, 'name': '王五', 'goods_name': '青菜', 'rn': 4, 'rk': 4, 'drk': 4, 'prk': 3.8,
             'cd': 4.166666666666666},
            {'order_id': 3, 'name': '李四', 'goods_name': '白菜', 'rn': 6, 'rk': 6, 'drk': 6, 'prk': 2.75,
             'cd': 6.166666666666667},
            {'order_id': 4, 'name': '王五', 'goods_name': '青菜', 'rn': 2, 'rk': 2, 'drk': 2, 'prk': 1.6666666666666665,
             'cd': 2.166666666666667},
            {'order_id': 5, 'name': '李四', 'goods_name': '青菜', 'rn': 3, 'rk': 3, 'drk': 3, 'prk': 0.5,
             'cd': 3.1666666666666665},
            {'order_id': 6, 'name': '李四', 'goods_name': '白菜', 'rn': 5, 'rk': 5, 'drk': 5, 'prk': 0,
             'cd': 5.166666666666667}], "data error")

        self.assert_data(40, [
            {'order_id': 1, 'name': '李四', 'goods_name': '青菜', 'rn': 1, 'rk': 1, 'drk': 1, 'prk': 0, 'cd': 1.25},
            {'order_id': 2, 'name': '王五', 'goods_name': '青菜', 'rn': 2, 'rk': 2, 'drk': 2, 'prk': 0.5, 'cd': 2.5},
            {'order_id': 3, 'name': '李四', 'goods_name': '白菜', 'rn': 4, 'rk': 4, 'drk': 4, 'prk': 2.75, 'cd': 4.25},
            {'order_id': 4, 'name': '王五', 'goods_name': '青菜', 'rn': 1, 'rk': 1, 'drk': 1, 'prk': 0, 'cd': 1.5},
            {'order_id': 5, 'name': '李四', 'goods_name': '青菜', 'rn': 2, 'rk': 2, 'drk': 2, 'prk': 0.5, 'cd': 2.25},
            {'order_id': 6, 'name': '李四', 'goods_name': '白菜', 'rn': 3, 'rk': 3, 'drk': 3, 'prk': 1.6666666666666665,
             'cd': 3.25}], "data error")

        self.assert_data(47, [{'order_id': 1, 'name': '李四', 'goods_name': '青菜', 'a': 2, 'b': 0.0, 'c': 125.0},
                              {'order_id': 2, 'name': '王五', 'goods_name': '青菜', 'a': 4, 'b': 0.1, 'c': 250.0},
                              {'order_id': 3, 'name': '李四', 'goods_name': '白菜', 'a': 8, 'b': 1.1, 'c': 425.0},
                              {'order_id': 4, 'name': '王五', 'goods_name': '青菜', 'a': 2, 'b': 0.0, 'c': 150.0},
                              {'order_id': 5, 'name': '李四', 'goods_name': '青菜', 'a': 4, 'b': 0.1, 'c': 225.0},
                              {'order_id': 6, 'name': '李四', 'goods_name': '白菜', 'a': 6, 'b': 0.5, 'c': 325.0}],
                         "data error")

        self.assert_data(56, [{'order_id': 1, 'uid': 2, 'goods_id': 1, 'va': {1, 2}, 'vb': {2}, 'vc': {2}, 'vd': '2'},
                              {'order_id': 2, 'uid': 1, 'goods_id': 1, 'va': {1, 2}, 'vb': {1}, 'vc': {1}, 'vd': '1,1'},
                              {'order_id': 3, 'uid': 2, 'goods_id': 2, 'va': {1, 2}, 'vb': {2}, 'vc': {2},
                               'vd': '2,2,2,2'},
                              {'order_id': 4, 'uid': 1, 'goods_id': 1, 'va': {1, 2}, 'vb': {1}, 'vc': {1}, 'vd': '1'},
                              {'order_id': 5, 'uid': 2, 'goods_id': 1, 'va': {1, 2}, 'vb': {2}, 'vc': {2}, 'vd': '2,2'},
                              {'order_id': 6, 'uid': 2, 'goods_id': 2, 'va': {1, 2}, 'vb': {2}, 'vc': {2},
                               'vd': '2,2,2'}], "data error")

        self.assert_data(59, [
            {'order_id': 1, 'name': '李四', 'goods_name': '青菜', 'va': {'李四', '王五'}, 'vb': {'李四'}, 'vc': {'李四'},
             'vd': '李四'},
            {'order_id': 2, 'name': '王五', 'goods_name': '青菜', 'va': {'李四', '王五'}, 'vb': {'王五'}, 'vc': {'王五'},
             'vd': '王五,王五'},
            {'order_id': 3, 'name': '李四', 'goods_name': '白菜', 'va': {'李四', '王五'}, 'vb': {'李四'}, 'vc': {'李四'},
             'vd': '李四,李四,李四,李四'},
            {'order_id': 4, 'name': '王五', 'goods_name': '青菜', 'va': {'李四', '王五'}, 'vb': {'王五'}, 'vc': {'王五'},
             'vd': '王五'},
            {'order_id': 5, 'name': '李四', 'goods_name': '青菜', 'va': {'李四', '王五'}, 'vb': {'李四'}, 'vc': {'李四'},
             'vd': '李四,李四'},
            {'order_id': 6, 'name': '李四', 'goods_name': '白菜', 'va': {'李四', '王五'}, 'vb': {'李四'}, 'vc': {'李四'},
             'vd': '李四,李四,李四'}], "data error")

        self.assert_data(65, [{'order_id': 1, 'uid': 2, 'goods_id': 1, 'rn': 1, 'lag': None, 'lead': 2},
                              {'order_id': 2, 'uid': 1, 'goods_id': 1, 'rn': 2, 'lag': 1, 'lead': 3},
                              {'order_id': 3, 'uid': 2, 'goods_id': 2, 'rn': 3, 'lag': 2, 'lead': 4},
                              {'order_id': 4, 'uid': 1, 'goods_id': 1, 'rn': 4, 'lag': 3, 'lead': 5},
                              {'order_id': 5, 'uid': 2, 'goods_id': 1, 'rn': 5, 'lag': 4, 'lead': 6},
                              {'order_id': 6, 'uid': 2, 'goods_id': 2, 'rn': 6, 'lag': 5, 'lead': None}], "data error")

        self.assert_data(67, [{'order_id': 1, 'uid': 2, 'goods_id': 1, 'rn': 1, 'lag': 0, 'lead': 0},
                              {'order_id': 2, 'uid': 1, 'goods_id': 1, 'rn': 2, 'lag': 0, 'lead': 0},
                              {'order_id': 3, 'uid': 2, 'goods_id': 2, 'rn': 3, 'lag': 1, 'lead': 1},
                              {'order_id': 4, 'uid': 1, 'goods_id': 1, 'rn': 4, 'lag': 2, 'lead': 2},
                              {'order_id': 5, 'uid': 2, 'goods_id': 1, 'rn': 5, 'lag': 3, 'lead': 3},
                              {'order_id': 6, 'uid': 2, 'goods_id': 2, 'rn': 6, 'lag': 4, 'lead': 4}], "data error")

        self.assert_data(70, [{'order_id': 1, 'uid': 2, 'goods_id': 1, 'rn': 3, 'lead': 0},
                              {'order_id': 2, 'uid': 1, 'goods_id': 1, 'rn': 4, 'lead': 0},
                              {'order_id': 3, 'uid': 2, 'goods_id': 2, 'rn': 5, 'lead': 6},
                              {'order_id': 4, 'uid': 1, 'goods_id': 1, 'rn': 6, 'lead': 12},
                              {'order_id': 5, 'uid': 2, 'goods_id': 1, 'rn': 7, 'lead': 18},
                              {'order_id': 6, 'uid': 2, 'goods_id': 2, 'rn': 8, 'lead': 24}], "data error")