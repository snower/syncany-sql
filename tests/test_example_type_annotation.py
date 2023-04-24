# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

import datetime
import uuid
from bson.objectid import ObjectId
from syncany.utils import get_timezone
from .example import ExampleTestCase


class TypeAnnotationExampleTestCase(ExampleTestCase):
    example_name = "type_annotation"

    def test_type_annotation(self):
        self.execute("type_annotation.sql")

        self.assert_data(5, [{'t': 'str', 'v': '3243243'}], "data error")
        self.assert_data(7, [{'t': 'str', 'v': '12.90'}], "data error")
        self.assert_data(10, [{'t': 'datetime', 'v': datetime.datetime(2023, 2, 23, 10, 23, 21,
                                                                       tzinfo=get_timezone())}], "data error")
        self.assert_data(12, [{'orderAt': datetime.datetime(2023, 2, 23, 10, 23, 21, tzinfo=get_timezone()),
                               't': 'str', 'v': '2023-02-23'}], "data error")
        self.assert_data(16, [{'orderAt': datetime.datetime(2023, 2, 23, 10, 23, 21, tzinfo=get_timezone()),
                               't': 'int', 'v': 1677119001}], "data error")
        self.assert_data(21, [{'t': 'objectid', 'v': ObjectId('6422a08fb4055348da72633b')},
                              {'t': 'objectid', 'v': ObjectId('6422a0a4b4055348da72633c')}], "data error")
        self.assert_data(26, [{'t': 'uuid', 'v': uuid.UUID('da984ae6-cd3f-11ed-af1b-eb91b1b4fa12')}], "data error")
        self.assert_data(28, [{'payId': uuid.UUID('da984ae6-cd3f-11ed-af1b-eb91b1b4fa12'), 't': 'int',
                               'v': 290562451387917676601713545966572075538}], "data error")
        self.assert_data(33, [{'t': 'bool', 'v': True}], "data error")
        self.assert_data(34, [{'v1': False, 'v2': True, 'v3': False, 'v4': True, 'v5': False}], "data error")
