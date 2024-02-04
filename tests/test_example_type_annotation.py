# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

import datetime
import uuid
from bson.objectid import ObjectId
from .example import ExampleTestCase


class TypeAnnotationExampleTestCase(ExampleTestCase):
    example_name = "type_annotation"

    def test_type_annotation(self):
        self.execute("type_annotation.sql")

        self.assert_data(5, [{'t': 'str', 'v': '3243243'}], "data error")
        self.assert_data(7, [{'t': 'str', 'v': '12.90'}], "data error")
        self.assert_value(10, 't', 'datetime', "data error")
        self.assert_value(10, 'v', lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(12, 'orderAt', lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(12, 't', 'str', "data error")
        self.assert_value(12, 'v', '2023-02-23', "data error")
        self.assert_value(16, 'orderAt', lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(16, 't', 'int', "data error")
        self.assert_value(16, 'v', lambda value: isinstance(value, int), "data error")
        self.assert_data(21, [{'t': 'objectid', 'v': ObjectId('6422a08fb4055348da72633b')},
                              {'t': 'objectid', 'v': ObjectId('6422a0a4b4055348da72633c')}], "data error")
        self.assert_data(26, [{'t': 'uuid', 'v': uuid.UUID('da984ae6-cd3f-11ed-af1b-eb91b1b4fa12')}], "data error")
        self.assert_data(28, [{'payId': uuid.UUID('da984ae6-cd3f-11ed-af1b-eb91b1b4fa12'), 't': 'int',
                               'v': 290562451387917676601713545966572075538}], "data error")
        self.assert_data(33, [{'t': 'bool', 'v': True}], "data error")
        self.assert_data(34, [{'v1': False, 'v2': True, 'v3': False, 'v4': True, 'v5': False}], "data error")

    def test_type_declaration_cast(self):
        self.execute("type_declaration_cast.sql")

        self.assert_data(5, [{'t': 'str', 'v': '3243243'}], "data error")
        self.assert_data(7, [{'t': 'str', 'v': '12.90'}], "data error")
        self.assert_value(10, 't', 'datetime', "data error")
        self.assert_value(10, 'v', lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(12, 'orderAt', lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(12, 't', 'str', "data error")
        self.assert_value(12, 'v', '2023-02-23', "data error")
        self.assert_value(16, 'orderAt', lambda value: isinstance(value, datetime.datetime), "data error")
        self.assert_value(16, 't', 'int', "data error")
        self.assert_value(16, 'v', lambda value: isinstance(value, int), "data error")
        self.assert_data(21, [{'t': 'objectid', 'v': ObjectId('6422a08fb4055348da72633b')},
                              {'t': 'objectid', 'v': ObjectId('6422a0a4b4055348da72633c')}], "data error")
        self.assert_data(26, [{'t': 'uuid', 'v': uuid.UUID('da984ae6-cd3f-11ed-af1b-eb91b1b4fa12')}], "data error")
        self.assert_data(28, [{'payId': uuid.UUID('da984ae6-cd3f-11ed-af1b-eb91b1b4fa12'), 't': 'int',
                               'v': 290562451387917676601713545966572075538}], "data error")
        self.assert_data(33, [{'t': 'bool', 'v': True}], "data error")
        self.assert_data(34, [{'v1': False, 'v2': True, 'v3': False, 'v4': True, 'v5': False}], "data error")
