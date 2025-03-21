# -*- coding: utf-8 -*-
# 2023/4/24
# create by: snower

from .example import ExampleTestCase


class AggregateExampleTestCase(ExampleTestCase):
    example_name = "aggregate"

    def test_aggregate(self):
        self.execute("aggregate.sql")

        self.assert_data(5, [{'cnt': 6, 'total_amount': 43.2, 'avg_amount': 7.2, 'min_amount': 3, 'max_amount': 9.6}],
                         "data error")

        self.assert_data(7, [
            {'uid': 2, 'cnt': 4, 'total_amount': 27.6, 'avg_amount': 6.9, 'min_amount': 3, 'max_amount': 9.6},
            {'uid': 1, 'cnt': 2, 'total_amount': 15.6, 'avg_amount': 7.8, 'min_amount': 7.6, 'max_amount': 8}],
                         "data error")

        self.assert_data(9, [{'uid': 2, 'order_id': 1, 'amount': 9.6}, {'uid': 1, 'order_id': 2, 'amount': 7.6},
                             {'uid': 2, 'order_id': 3, 'amount': 3}, {'uid': 1, 'order_id': 4, 'amount': 8},
                             {'uid': 2, 'order_id': 5, 'amount': 8}, {'uid': 2, 'order_id': 6, 'amount': 7}],
                         "data error")

        self.assert_data(11, [{'ucnt': 2}], "data error")

        self.assert_data(13, [{'uid': 2, 'gcnt': 2}, {'uid': 1, 'gcnt': 1}], "data error")

        self.assert_data(15, [{'uid': 1, 'gcnt': 1}], "data error")

        self.assert_data(17, [{'uid': 2, 'gcnt': 2}], "data error")

        self.assert_data(19, [{'uid': 2, 'avg_amount': 0.069}, {'uid': 1, 'avg_amount': 0.078}], "data error")

        self.assert_data(21, [{'name': '李四', 'goods_name': '青菜', 'cnt': 6}], "data error")

        self.assert_data(24,
                         [{'name': '李四', 'goods_name': '青菜', 'cnt': 2},
                          {'name': '王五', 'goods_name': '青菜', 'cnt': 2},
                          {'name': '李四', 'goods_name': '白菜', 'cnt': 2}], "data error")

        self.assert_data(27, [{'name': '李四', 'goods_name': '青菜'}, {'name': '王五', 'goods_name': '青菜'},
                              {'name': '李四', 'goods_name': '白菜'}], "data error")

        self.assert_data(30,
                         [{'name': '李四', 'goods_name': '青菜', 'cnt': 2},
                          {'name': '李四', 'goods_name': '白菜', 'cnt': 1}],
                         "data error")

        self.assert_data(33, [{'uid': 2, 'cgoods_ids': '1,2,1,2', 'agoods_ids': [1, 2, 1, 2], 'uagoods_ids': [1, 2]},
                              {'uid': 1, 'cgoods_ids': '1,1', 'agoods_ids': [1, 1], 'uagoods_ids': [1]}], "data error")

        self.assert_data(35, [{'uid': 2, 'goods_id_and': 0, 'goods_id_or': 3, 'goods_id_xor': 0},
                              {'uid': 1, 'goods_id_and': 1, 'goods_id_or': 1, 'goods_id_xor': 0}], "data error")

        self.assert_data(38, [{'uids': {1, 2}, 'order_ids': '1,2,3,4,5,6'}], "data error")

        self.assert_data(40, [{'uid': 2, 'goods_ids': {1, 2}, 'order_ids': '1,3,5,6'},
                              {'uid': 1, 'goods_ids': {1}, 'order_ids': '2,4'}], "data error")

        self.assert_data(42, [{'uid': 2, 'avg_amount': 0.07, 'percent': 0.0275},
                              {'uid': 1, 'avg_amount': 0.03, 'percent': 0.025}], "data error")

        self.assert_data(44, [
            {'name': '李四', 'goods_name': '青菜', 'names': {'李四', '王五'},
             'goods_namees': '青菜,青菜,白菜,青菜,青菜,白菜'}],
                         "data error")

        self.assert_data(48, [{'name': '李四', 'goods_name': '青菜', 'names': {'李四'}, 'goods_namees': '青菜,青菜'},
                              {'name': '王五', 'goods_name': '青菜', 'names': {'王五'}, 'goods_namees': '青菜,青菜'},
                              {'name': '李四', 'goods_name': '白菜', 'names': {'李四'}, 'goods_namees': '白菜,白菜'}],
                         "data error")

        self.assert_data(52, [{'uid': 2, 'agoods_ids': '[1, 2, 1, 2]', 'ogoods_ids': '{"1": 5, "2": 6}'},
                              {'uid': 1, 'agoods_ids': '[1, 1]', 'ogoods_ids': '{"1": 4}'}], "data error")

    def test_aggregate_batch(self):
        self.execute("aggregate_batch.sql")

        self.assert_data(5, [{'cnt': 6, 'total_amount': 43.2, 'avg_amount': 7.2, 'min_amount': 3, 'max_amount': 9.6}],
                         "data error")

        self.assert_data(7, [
            {'uid': 2, 'cnt': 4, 'total_amount': 27.6, 'avg_amount': 6.9, 'min_amount': 3, 'max_amount': 9.6},
            {'uid': 1, 'cnt': 2, 'total_amount': 15.6, 'avg_amount': 7.8, 'min_amount': 7.6, 'max_amount': 8}],
                         "data error")

        self.assert_data(9, [{'uid': 2, 'order_id': 1, 'amount': 9.6}, {'uid': 1, 'order_id': 2, 'amount': 7.6},
                             {'uid': 2, 'order_id': 3, 'amount': 3}, {'uid': 1, 'order_id': 4, 'amount': 8},
                             {'uid': 2, 'order_id': 5, 'amount': 8}, {'uid': 2, 'order_id': 6, 'amount': 7}],
                         "data error")

        self.assert_data(11, [{'ucnt': 2}], "data error")

        self.assert_data(13, [{'uid': 2, 'gcnt': 2}, {'uid': 1, 'gcnt': 1}], "data error")

        self.assert_data(15, [{'uid': 1, 'gcnt': 1}], "data error")

        self.assert_data(17, [{'uid': 2, 'gcnt': 2}], "data error")

        self.assert_data(19, [{'uid': 2, 'avg_amount': 0.069}, {'uid': 1, 'avg_amount': 0.078}], "data error")

        self.assert_data(21, [{'name': '李四', 'goods_name': '青菜', 'cnt': 6}], "data error")

        self.assert_data(24,
                         [{'name': '李四', 'goods_name': '青菜', 'cnt': 2},
                          {'name': '王五', 'goods_name': '青菜', 'cnt': 2},
                          {'name': '李四', 'goods_name': '白菜', 'cnt': 2}], "data error")

        self.assert_data(27, [{'name': '李四', 'goods_name': '青菜'}, {'name': '王五', 'goods_name': '青菜'},
                              {'name': '李四', 'goods_name': '白菜'}], "data error")

        self.assert_data(30,
                         [{'name': '李四', 'goods_name': '青菜', 'cnt': 2},
                          {'name': '李四', 'goods_name': '白菜', 'cnt': 1}],
                         "data error")

        self.assert_data(33, [{'uid': 2, 'cgoods_ids': '1,2,1,2', 'agoods_ids': [1, 2, 1, 2], 'uagoods_ids': [1, 2]},
                              {'uid': 1, 'cgoods_ids': '1,1', 'agoods_ids': [1, 1], 'uagoods_ids': [1]}], "data error")

        self.assert_data(35, [{'uid': 2, 'goods_id_and': 0, 'goods_id_or': 3, 'goods_id_xor': 0},
                              {'uid': 1, 'goods_id_and': 1, 'goods_id_or': 1, 'goods_id_xor': 0}], "data error")

        self.assert_data(38, [{'uids': {1, 2}, 'order_ids': '1,2,3,4,5,6'}], "data error")

        self.assert_data(40, [{'uid': 2, 'goods_ids': {1, 2}, 'order_ids': '1,3,5,6'},
                              {'uid': 1, 'goods_ids': {1}, 'order_ids': '2,4'}], "data error")

        self.assert_data(42, [{'uid': 2, 'avg_amount': 0.07}, {'uid': 1, 'avg_amount': 0.03}], "data error")

        self.assert_data(44, [
            {'name': '李四', 'goods_name': '青菜', 'names': {'王五', '李四'},
             'goods_namees': '青菜,青菜,白菜,青菜,青菜,白菜'}],
                         "data error")

        self.assert_data(48, [{'name': '李四', 'goods_name': '青菜', 'names': {'李四'}, 'goods_namees': '青菜,青菜'},
                              {'name': '王五', 'goods_name': '青菜', 'names': {'王五'}, 'goods_namees': '青菜,青菜'},
                              {'name': '李四', 'goods_name': '白菜', 'names': {'李四'}, 'goods_namees': '白菜,白菜'}],
                         "data error")

    def test_aggregate(self):
        self.execute("window_aggregate.sql")

        self.assert_data(1, [{'cnt': 2,
                              'goods_name': '青菜',
                              'name': '李四',
                              'order_id': 1,
                              'order_time': '2024-10-01 10:09:10',
                              'total_amount': 24.3,
                              'unorder_time': '2024-10-06 11:09:10'},
                             {'cnt': 1,
                              'goods_name': '青菜',
                              'name': '王五',
                              'order_id': 2,
                              'order_time': '2024-10-01 12:09:10',
                              'total_amount': 7.6,
                              'unorder_time': None},
                             {'cnt': 2,
                              'goods_name': '白菜',
                              'name': '李四',
                              'order_id': 3,
                              'order_time': '2024-10-02 09:09:10',
                              'total_amount': 34.4,
                              'unorder_time': '2024-10-08 09:09:10'},
                             {'cnt': 1,
                              'goods_name': '青菜',
                              'name': '王五',
                              'order_id': 4,
                              'order_time': '2024-10-01 10:09:10',
                              'total_amount': 8,
                              'unorder_time': None},
                             {'cnt': 2,
                              'goods_name': '青菜',
                              'name': '李四',
                              'order_id': 5,
                              'order_time': '2024-10-01 10:09:10',
                              'total_amount': 100.80000000000001,
                              'unorder_time': '2024-10-06 15:09:10'},
                             {'cnt': 1,
                              'goods_name': '白菜',
                              'name': '李四',
                              'order_id': 6,
                              'order_time': '2024-10-01 10:09:10',
                              'total_amount': 7,
                              'unorder_time': None}],
                         "data error")

        self.assert_data(14, [{'first_create_time': '2024-10-01 10:09:10',
                               'history_type': 1,
                               'id': 1,
                               'next_create_time': '2024-10-02 14:09:10',
                               'next_history_type': 0,
                               'order_id': 1},
                              {'first_create_time': '2024-10-01 10:09:10',
                               'history_type': 0,
                               'id': 2,
                               'next_create_time': '2024-10-06 11:09:10',
                               'next_history_type': 1,
                               'order_id': 1},
                              {'first_create_time': '2024-10-06 11:09:10',
                               'history_type': 1,
                               'id': 4,
                               'next_create_time': None,
                               'next_history_type': None,
                               'order_id': 1}],
                         "data error")

        self.assert_data(22, [{'cnt': 3,
                               'history_type': 1,
                               'id': 1,
                               'next_create_time': None,
                               'next_history_type': None,
                               'order_id': 1,
                               'total_amount': 33.900000000000006},
                              {'cnt': 1,
                               'history_type': 1,
                               'id': 3,
                               'next_create_time': None,
                               'next_history_type': None,
                               'order_id': 2,
                               'total_amount': 7.6},
                              {'cnt': 5,
                               'history_type': 1,
                               'id': 5,
                               'next_create_time': None,
                               'next_history_type': None,
                               'order_id': 3,
                               'total_amount': 44.7},
                              {'cnt': 1,
                               'history_type': 1,
                               'id': 8,
                               'next_create_time': None,
                               'next_history_type': None,
                               'order_id': 4,
                               'total_amount': 8},
                              {'cnt': 3,
                               'history_type': 1,
                               'id': 11,
                               'next_create_time': None,
                               'next_history_type': None,
                               'order_id': 5,
                               'total_amount': 108.80000000000001},
                              {'cnt': 1,
                               'history_type': 1,
                               'id': 12,
                               'next_create_time': None,
                               'next_history_type': None,
                               'order_id': 6,
                               'total_amount': 7}],
                         "data error")
