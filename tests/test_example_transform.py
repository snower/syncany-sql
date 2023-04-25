# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class TransformExampleTestCase(ExampleTestCase):
    example_name = "transform"

    def test_transform_h2v(self):
        self.execute("transform_h2v.sql")

        self.assert_data(32, [{'amount': 10, 'create_date': '2023-01-03', 'name': '黄豆网'},
                              {'amount': 7.2, 'create_date': '2023-01-03', 'name': '青菜网'},
                              {'amount': 0, 'create_date': '2023-01-03', 'name': '火箭网'},
                              {'amount': 0, 'create_date': '2023-01-03', 'name': '卫星网'},
                              {'amount': 6.3, 'create_date': '2023-01-05', 'name': '黄豆网'},
                              {'amount': 0, 'create_date': '2023-01-05', 'name': '青菜网'},
                              {'amount': 4.7, 'create_date': '2023-01-05', 'name': '火箭网'},
                              {'amount': 0, 'create_date': '2023-01-05', 'name': '卫星网'},
                              {'amount': 0, 'create_date': '2023-01-07', 'name': '黄豆网'},
                              {'amount': 0, 'create_date': '2023-01-07', 'name': '青菜网'},
                              {'amount': 0, 'create_date': '2023-01-07', 'name': '火箭网'},
                              {'amount': 11.2, 'create_date': '2023-01-07', 'name': '卫星网'},
                              {'amount': 3.54, 'create_date': '2023-01-08', 'name': '黄豆网'},
                              {'amount': 0, 'create_date': '2023-01-08', 'name': '青菜网'},
                              {'amount': 0, 'create_date': '2023-01-08', 'name': '火箭网'},
                              {'amount': 0, 'create_date': '2023-01-08', 'name': '卫星网'}], "data error")
        self.assert_data(38, [{'amount': 5.5, 'name': 'limei', 'order_date': '2022-01-01'},
                              {'amount': 8.2, 'name': 'wanzhi', 'order_date': '2022-01-01'},
                              {'amount': 4.3, 'name': 'limei', 'order_date': '2022-01-02'},
                              {'amount': 1.8, 'name': 'wanzhi', 'order_date': '2022-01-02'}], "data error")
        self.assert_data(42, [{'name': 'limei', 'order_date': '2022-01-01'},
                              {'name': 'wanzhi', 'order_date': '2022-01-01'},
                              {'name': 'limei', 'order_date': '2022-01-02'},
                              {'name': 'wanzhi', 'order_date': '2022-01-02'}], "data error")

    def test_transform_h4v(self):
        self.execute("transform_h4v.sql")

        self.assert_data(35, [{'amount': 10, 'create_date': '2023-01-03', 'name': '黄豆网', 'order_id': 1, 'site_id': 8},
                              {'amount': 7.2, 'create_date': '2023-01-03', 'name': '青菜网', 'order_id': 2, 'site_id': 15},
                              {'amount': 2.8, 'create_date': '2023-01-05', 'name': '黄豆网', 'order_id': 3, 'site_id': 8},
                              {'amount': 4.7, 'create_date': '2023-01-05', 'name': '火箭网', 'order_id': 4, 'site_id': 28},
                              {'amount': 3.5, 'create_date': '2023-01-05', 'name': '黄豆网', 'order_id': 5, 'site_id': 8},
                              {'amount': 11.2, 'create_date': '2023-01-07', 'name': '卫星网', 'order_id': 6, 'site_id': 34},
                              {'amount': 3.54, 'create_date': '2023-01-08', 'name': '黄豆网', 'order_id': 7, 'site_id': 8}], "data error")
        self.assert_data(43, [{'amount': 10, 'create_date': '2023-01-03', 'name': '黄豆网', 'order_id': 1, 'site_id': 8},
                              {'amount': 7.2, 'create_date': '2023-01-03', 'name': '青菜网', 'order_id': 2, 'site_id': 15},
                              {'amount': 2.8, 'create_date': '2023-01-05', 'name': '黄豆网', 'order_id': 3, 'site_id': 8},
                              {'amount': 4.7, 'create_date': '2023-01-05', 'name': '火箭网', 'order_id': 4, 'site_id': 28},
                              {'amount': 3.5, 'create_date': '2023-01-05', 'name': '黄豆网', 'order_id': 5, 'site_id': 8},
                              {'amount': 11.2, 'create_date': '2023-01-07', 'name': '卫星网', 'order_id': 6, 'site_id': 34},
                              {'amount': 3.54, 'create_date': '2023-01-08', 'name': '黄豆网', 'order_id': 7, 'site_id': 8}], "data error")
        self.assert_data(51, [{'age': '18', 'goods': '青菜', 'name': 'limei', 'order_id': '1'},
                              {'age': '22', 'goods': '白菜', 'name': 'wanzhi', 'order_id': '2'},
                              {'age': '22', 'goods': '青菜', 'name': 'wanzhi', 'order_id': '3'}], "data error")
        self.assert_data(59, [{'age': '18', 'goods': '青菜', 'name': 'limei', 'order_id': '1'},
                              {'age': '22', 'goods': '白菜', 'name': 'wanzhi', 'order_id': '2'},
                              {'age': '22', 'goods': '青菜', 'name': 'wanzhi', 'order_id': '3'}], "data error")

    def test_transform_uniqkv(self):
        self.execute("transform_uniqkv.sql")

        self.assert_data(26, [{'amount': 10, 'create_date': '2023-01-03', 'name': '黄豆网', 'order_id': 1, 'site_id': 8, '卫星网': None, '火箭网': None, '青菜网': 7.2, '黄豆网': 10},
                              {'amount': 2.8, 'create_date': '2023-01-05', 'name': '黄豆网', 'order_id': 3, 'site_id': 8, '卫星网': None, '火箭网': 4.7, '青菜网': None, '黄豆网': 6.3},
                              {'amount': 11.2, 'create_date': '2023-01-07', 'name': '卫星网', 'order_id': 6, 'site_id': 34, '卫星网': 11.2, '火箭网': None, '青菜网': None, '黄豆网': None},
                              {'amount': 3.54, 'create_date': '2023-01-08', 'name': '黄豆网', 'order_id': 7, 'site_id': 8, '卫星网': None, '火箭网': None, '青菜网': None, '黄豆网': 3.54}], "data error")
        self.assert_data(32, [{'amount': 5.5, 'goods': '青菜', 'id': 1, 'limei': 5.5, 'name': 'limei', 'order_date': '2022-01-01', 'wanzhi': 10.399999999999999}], "data error")
        self.assert_data(37, [{'amount': 5.5, 'goods': '青菜', 'id': 1, 'limei': 1, 'name': 'limei', 'order_date': '2022-01-01', 'wanzhi': 2}], "data error")
        self.assert_data(42, [{'amount': '5.50', 'goods': '青菜', 'id': 1, 'limei': '5.50', 'name': 'limei', 'order_date': '2022-01-01', 'wanzhi': '2.20'}], "data error")

    def test_transform_v2h(self):
        self.execute("transform_v2h.sql")

        self.assert_data(26, [{'create_date': '2023-01-03', '卫星网': 0, '火箭网': 0, '青菜网': 7.2, '黄豆网': 10},
                              {'create_date': '2023-01-05', '卫星网': 0, '火箭网': 4.7, '青菜网': 0, '黄豆网': 6.3},
                              {'create_date': '2023-01-07', '卫星网': 11.2, '火箭网': 0, '青菜网': 0, '黄豆网': 0},
                              {'create_date': '2023-01-08', '卫星网': 0, '火箭网': 0, '青菜网': 0, '黄豆网': 3.54}], "data error")
        self.assert_data(30, [{'create_date': '2023-01-03', '卫星网': 0, '火箭网': 0, '青菜网': 1, '黄豆网': 1},
                              {'create_date': '2023-01-05', '卫星网': 0, '火箭网': 1, '青菜网': 0, '黄豆网': 2},
                              {'create_date': '2023-01-07', '卫星网': 1, '火箭网': 0, '青菜网': 0, '黄豆网': 0},
                              {'create_date': '2023-01-08', '卫星网': 0, '火箭网': 0, '青菜网': 0, '黄豆网': 1}], "data error")
        self.assert_data(36, [{'limei': 5.5, 'order_date': '2022-01-01', 'wanzhi': 10.399999999999999}], "data error")
        self.assert_data(41, [{'limei': 1, 'order_date': '2022-01-01', 'wanzhi': 2}], "data error")
        self.assert_data(46, [{'limei': '青菜', 'order_date': '2022-01-01', 'wanzhi': '青菜'}], "data error")
        self.assert_data(51, [{'limei': '5.50', 'order_date': '2022-01-01', 'wanzhi': '2.20'}], "data error")

    def test_transform_v4h(self):
        self.execute("transform_v4h.sql")

        self.assert_data(27, [{'key': 'order_id', 'value': 1}, {'key': 'site_id', 'value': 8}, {'key': 'amount', 'value': 10},
                              {'key': 'create_date', 'value': '2023-01-03'}, {'key': 'name', 'value': '黄豆网'},
                              {'key': 'order_id', 'value': 2}, {'key': 'site_id', 'value': 15}, {'key': 'amount', 'value': 7.2},
                              {'key': 'create_date', 'value': '2023-01-03'}, {'key': 'name', 'value': '青菜网'},
                              {'key': 'order_id', 'value': 3}, {'key': 'site_id', 'value': 8}, {'key': 'amount', 'value': 2.8},
                              {'key': 'create_date', 'value': '2023-01-05'}, {'key': 'name', 'value': '黄豆网'},
                              {'key': 'order_id', 'value': 4}, {'key': 'site_id', 'value': 28}, {'key': 'amount', 'value': 4.7},
                              {'key': 'create_date', 'value': '2023-01-05'}, {'key': 'name', 'value': '火箭网'},
                              {'key': 'order_id', 'value': 5}, {'key': 'site_id', 'value': 8}, {'key': 'amount', 'value': 3.5},
                              {'key': 'create_date', 'value': '2023-01-05'}, {'key': 'name', 'value': '黄豆网'},
                              {'key': 'order_id', 'value': 6}, {'key': 'site_id', 'value': 34}, {'key': 'amount', 'value': 11.2},
                              {'key': 'create_date', 'value': '2023-01-07'}, {'key': 'name', 'value': '卫星网'},
                              {'key': 'order_id', 'value': 7}, {'key': 'site_id', 'value': 8}, {'key': 'amount', 'value': 3.54},
                              {'key': 'create_date', 'value': '2023-01-08'}, {'key': 'name', 'value': '黄豆网'}], "data error")
        self.assert_data(31, [{'key': 'order_id', 'site_id': 8, 'value': 1}, {'key': 'amount', 'site_id': 8, 'value': 10},
                              {'key': 'create_date', 'site_id': 8, 'value': '2023-01-03'}, {'key': 'name', 'site_id': 8, 'value': '黄豆网'},
                              {'key': 'order_id', 'site_id': 15, 'value': 2}, {'key': 'amount', 'site_id': 15, 'value': 7.2},
                              {'key': 'create_date', 'site_id': 15, 'value': '2023-01-03'}, {'key': 'name', 'site_id': 15, 'value': '青菜网'},
                              {'key': 'order_id', 'site_id': 8, 'value': 3}, {'key': 'amount', 'site_id': 8, 'value': 2.8},
                              {'key': 'create_date', 'site_id': 8, 'value': '2023-01-05'}, {'key': 'name', 'site_id': 8, 'value': '黄豆网'},
                              {'key': 'order_id', 'site_id': 28, 'value': 4}, {'key': 'amount', 'site_id': 28, 'value': 4.7},
                              {'key': 'create_date', 'site_id': 28, 'value': '2023-01-05'}, {'key': 'name', 'site_id': 28, 'value': '火箭网'},
                              {'key': 'order_id', 'site_id': 8, 'value': 5}, {'key': 'amount', 'site_id': 8, 'value': 3.5},
                              {'key': 'create_date', 'site_id': 8, 'value': '2023-01-05'}, {'key': 'name', 'site_id': 8, 'value': '黄豆网'},
                              {'key': 'order_id', 'site_id': 34, 'value': 6}, {'key': 'amount', 'site_id': 34, 'value': 11.2},
                              {'key': 'create_date', 'site_id': 34, 'value': '2023-01-07'}, {'key': 'name', 'site_id': 34, 'value': '卫星网'},
                              {'key': 'order_id', 'site_id': 8, 'value': 7}, {'key': 'amount', 'site_id': 8, 'value': 3.54},
                              {'key': 'create_date', 'site_id': 8, 'value': '2023-01-08'}, {'key': 'name', 'site_id': 8, 'value': '黄豆网'}], "data error")
        self.assert_data(37, [{'key': 'id', 'value': 1}, {'key': 'name', 'value': 'limei'}, {'key': 'age', 'value': 18}, {'key': 'id', 'value': 2},
                              {'key': 'name', 'value': 'wanzhi'}, {'key': 'age', 'value': 22}], "data error")
        self.assert_data(41, [{'key': 'id', 'name': 'limei', 'value': 1}, {'key': 'age', 'name': 'limei', 'value': 18}, {'key': 'id', 'name': 'wanzhi', 'value': 2},
                              {'key': 'age', 'name': 'wanzhi', 'value': 22}], "data error")

    def test_transform_customize(self):
        self.execute("transform_customize.sql")

        self.assert_data(4, [{'amount': 10, 'create_date': '2023-01-03', 'name': '黄豆网', 'order_id': 7, 'row_id': 1, 'site_id': 8},
                             {'amount': 7.2, 'create_date': '2023-01-03', 'name': '青菜网', 'order_id': 2, 'row_id': 2, 'site_id': 15},
                             {'amount': 4.7, 'create_date': '2023-01-05', 'name': '火箭网', 'order_id': 4, 'row_id': 3, 'site_id': 28},
                             {'amount': 11.2, 'create_date': '2023-01-07', 'name': '卫星网', 'order_id': 6, 'row_id': 4, 'site_id': 34}],
                         "data error")
