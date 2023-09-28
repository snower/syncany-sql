# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class JsonExampleTestCase(ExampleTestCase):
    example_name = "json"

    def test_json(self):
        self.execute("json.sql")

        self.assert_data(4, [{"JSON_CONTAINS(@j, @j2, '$.a')": 0}], "data error")

        self.assert_data(5, [{"JSON_CONTAINS(@j, @j2, '$.b')": 0}], "data error")

        self.assert_data(7, [{"JSON_CONTAINS(@j, @j2, '$.a')": 0}], "data error")

        self.assert_data(8, [{"JSON_CONTAINS(@j, @j2, '$.c')": 1}], "data error")

        self.assert_data(11, [{"JSON_CONTAINS_PATH(@j, 'one', '$.a', '$.e')": 1}], "data error")

        self.assert_data(12, [{"JSON_CONTAINS_PATH(@j, 'all', '$.a', '$.e')": 0}], "data error")

        self.assert_data(13, [{"JSON_CONTAINS_PATH(@j, 'one', '$.c.d')": 1}], "data error")

        self.assert_data(14, [{"JSON_CONTAINS_PATH(@j, 'one', '$.a.d')": 0}], "data error")

        self.assert_data(16, [{"JSON_EXTRACT('[10, 20, [30, 40]]', '$[1]')": 20}], "data error")

        self.assert_data(17, [{"JSON_EXTRACT('[10, 20, [30, 40]]', '$[2][*]')": [30, 40]}], "data error")

        self.assert_data(18, [{'JSON_EXTRACT(\'[10, 20, [{"a":30}, {"b":40}]]\', \'$[2][*]["a"]\')': [30]}],
                         "data error")

        self.assert_data(20, [{"JSON_DEPTH('{}')": 1, "JSON_DEPTH('[]')": 1, "JSON_DEPTH('true')": 1}], "data error")

        self.assert_data(21, [{"JSON_DEPTH('[10, 20]')": 2, "JSON_DEPTH('[[], {}]')": 2}], "data error")

        self.assert_data(22, [{'JSON_DEPTH(\'[10, {"a": 20}]\')': 3}], "data error")

        self.assert_data(24, [{'JSON_KEYS(\'{"a": 1, "b": {"c": 30}}\')': ['a', 'b']}], "data error")

        self.assert_data(25, [{'JSON_KEYS(\'{"a": 1, "b": {"c": 30}}\', \'$.b\')': ['c']}], "data error")

        self.assert_data(27, [{'JSON_LENGTH(\'[1, 2, {"a": 3}]\')': 3}], "data error")

        self.assert_data(28, [{'JSON_LENGTH(\'{"a": 1, "b": {"c": 30}}\')': 2}], "data error")

        self.assert_data(29, [{'JSON_LENGTH(\'{"a": 1, "b": {"c": 30}}\', \'$.b\')': 1}], "data error")

        self.assert_data(31, [{'JSON_VALID(\'{"a": 1}\')': 1}], "data error")

        self.assert_data(32, [{"JSON_VALID('hello')": 0, 'JSON_VALID(\'"hello"\')': 1}], "data error")

        self.assert_data(35, [{"JSON_SET(@j, '$.a', 2)": {'a': 2, 'b': 2, 'c': {'d': 4}},
                               "JSON_SET(@j, '$.c.d', 2)": {'a': 1, 'b': 2, 'c': {'d': 2}}}], "data error")

        self.assert_data(36, [
            {'JSON_SET(\'"1"\', \'$[0]\', \'a\')': 'a', 'JSON_SET(\'"1"\', \'$[2]\', \'a\')': ['1', 'a']}],
                         "data error")

        self.assert_data(37, [
            {'JSON_SET(\'["1"]\', \'$[0]\', \'a\')': ['a'], 'JSON_SET(\'["1"]\', \'$[2]\', \'a\')': ['1', 'a']}],
                         "data error")

        self.assert_data(39, [{"JSON_REMOVE(@j, '$.a', '$.c.d')": {'b': 2, 'c': {}},
                               "JSON_REMOVE(@j, '$.c.a')": {'a': 1, 'b': 2, 'c': {'d': 4}}}], "data error")

        self.assert_data(40, [{'JSON_REMOVE(\'"1"\', \'$[0]\')': '1', 'JSON_REMOVE(\'"1"\', \'$[2]\')': '1'}],
                         "data error")

        self.assert_data(41, [{'JSON_REMOVE(\'["1"]\', \'$[0]\')': [], 'JSON_REMOVE(\'["1"]\', \'$[2]\')': ['1']}],
                         "data error")
