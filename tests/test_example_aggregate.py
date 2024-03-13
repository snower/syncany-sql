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
                         [{'name': '李四', 'goods_name': '青菜', 'cnt': 2}, {'name': '王五', 'goods_name': '青菜', 'cnt': 2},
                          {'name': '李四', 'goods_name': '白菜', 'cnt': 2}], "data error")

        self.assert_data(27, [{'name': '李四', 'goods_name': '青菜'}, {'name': '王五', 'goods_name': '青菜'},
                              {'name': '李四', 'goods_name': '白菜'}], "data error")

        self.assert_data(30,
                         [{'name': '李四', 'goods_name': '青菜', 'cnt': 2}, {'name': '李四', 'goods_name': '白菜', 'cnt': 1}],
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
            {'name': '李四', 'goods_name': '青菜', 'names': {'李四', '王五'}, 'goods_namees': '青菜,青菜,白菜,青菜,青菜,白菜'}],
                         "data error")

        self.assert_data(48, [{'name': '李四', 'goods_name': '青菜', 'names': {'李四'}, 'goods_namees': '青菜,青菜'},
                              {'name': '王五', 'goods_name': '青菜', 'names': {'王五'}, 'goods_namees': '青菜,青菜'},
                              {'name': '李四', 'goods_name': '白菜', 'names': {'李四'}, 'goods_namees': '白菜,白菜'}],
                         "data error")

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
                         [{'name': '李四', 'goods_name': '青菜', 'cnt': 2}, {'name': '王五', 'goods_name': '青菜', 'cnt': 2},
                          {'name': '李四', 'goods_name': '白菜', 'cnt': 2}], "data error")

        self.assert_data(27, [{'name': '李四', 'goods_name': '青菜'}, {'name': '王五', 'goods_name': '青菜'},
                              {'name': '李四', 'goods_name': '白菜'}], "data error")

        self.assert_data(30,
                         [{'name': '李四', 'goods_name': '青菜', 'cnt': 2}, {'name': '李四', 'goods_name': '白菜', 'cnt': 1}],
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
            {'name': '李四', 'goods_name': '青菜', 'names': {'王五', '李四'}, 'goods_namees': '青菜,青菜,白菜,青菜,青菜,白菜'}],
                         "data error")

        self.assert_data(48, [{'name': '李四', 'goods_name': '青菜', 'names': {'李四'}, 'goods_namees': '青菜,青菜'},
                              {'name': '王五', 'goods_name': '青菜', 'names': {'王五'}, 'goods_namees': '青菜,青菜'},
                              {'name': '李四', 'goods_name': '白菜', 'names': {'李四'}, 'goods_namees': '白菜,白菜'}],
                         "data error")