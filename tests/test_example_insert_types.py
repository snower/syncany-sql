# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class InsertTypesExampleTestCase(ExampleTestCase):
    example_name = "insert_types"

    def test_delete_insert(self):
        self.execute("delete_insert.sql")

        self.assert_data(10, [{'id': 1, 'name': '萝卜', 'create_time': '2023-03-12 10:12:34'},
                              {'id': 2, 'name': '青菜', 'create_time': '2023-03-12 10:12:34'},
                              {'id': 3, 'name': '油麦菜', 'create_time': '2023-03-12 10:12:34'}], "data error")

    def test_insert(self):
        self.execute("insert.sql")

        self.assert_data(10, [{'id': 1, 'name': '萝卜', 'create_time': '2023-03-12 10:12:34'},
                              {'id': 2, 'name': '土豆', 'create_time': '2023-03-12 10:12:34'},
                              {'id': 4, 'name': '花菜', 'create_time': '2023-03-12 10:12:34'},
                              {'id': 2, 'name': '青菜', 'create_time': '2023-03-12 10:12:34'},
                              {'id': 3, 'name': '油麦菜', 'create_time': '2023-03-12 10:12:34'}], "data error")

    def test_update_delete_insert(self):
        self.execute("update_delete_insert.sql")

        self.assert_data(17, [{'create_time': '2023-03-12 10:12:34', 'id': 1, 'name': '萝卜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 2, 'name': '青菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 5, 'name': '白菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 6, 'name': '青菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 3, 'name': '油麦菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 7, 'name': '油麦菜'}], "data error")

        self.assert_data(20, [{'create_time': '2023-03-12 10:12:34', 'id': 1, 'name': '萝卜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 2, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 5, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 6, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 3, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 7, 'name': '玉米'}], "data error")

        self.assert_data(31, [{'id': 1, 'name': '萝卜', 'create_time': '2023-03-12 10:12:34'},
                              {'id': 2, 'name': '青菜', 'create_time': '2023-03-12 10:12:34'},
                              {'id': 3, 'name': '油麦菜', 'create_time': '2023-03-12 10:12:34'}], "data error")

    def test_update_insert(self):
        self.execute("update_insert.sql")

        self.assert_data(17, [{'create_time': '2023-03-12 10:12:34', 'id': 1, 'name': '萝卜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 2, 'name': '青菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 4, 'name': '花菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 5, 'name': '白菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 6, 'name': '青菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 8, 'name': '花菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 3, 'name': '油麦菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 7, 'name': '油麦菜'}], "data error")

        self.assert_data(20, [{'create_time': '2023-03-12 10:12:34', 'id': 1, 'name': '萝卜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 2, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 4, 'name': '花菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 5, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 6, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 8, 'name': '花菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 3, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 7, 'name': '玉米'}], "data error")

    def test_update(self):
        self.execute("update.sql")

        self.assert_data(17, [{'create_time': '2023-03-12 10:12:34', 'id': 1, 'name': '萝卜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 2, 'name': '青菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 4, 'name': '花菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 5, 'name': '白菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 6, 'name': '青菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 8, 'name': '花菜'}], "data error")

        self.assert_data(20, [{'create_time': '2023-03-12 10:12:34', 'id': 1, 'name': '萝卜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 2, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 4, 'name': '花菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 5, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 6, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 8, 'name': '花菜'}], "data error")

        self.assert_data(23, [{'create_time': '2023-03-12 10:12:34', 'id': 1, 'name': '花生'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 2, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 4, 'name': '花菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 5, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 6, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 8, 'name': '花菜'}], "data error")

        self.assert_data(26, [{'create_time': '2023-03-12 10:12:34', 'id': 1, 'name': '胡萝卜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 2, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 4, 'name': '花菜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 5, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 6, 'name': '胡萝卜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 8, 'name': '胡萝卜'}], "data error")

        self.assert_data(29, [{'create_time': '2023-03-12 10:12:34', 'id': 1, 'name': '胡萝卜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 2, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 4, 'name': '青椒'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 5, 'name': '玉米'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 6, 'name': '胡萝卜'},
                              {'create_time': '2023-03-12 10:12:34', 'id': 8, 'name': '胡萝卜'}], "data error")
