# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class InsertTypesExampleTestCase(ExampleTestCase):
    example_name = "insert_types"

    def test_delete_insert(self):
        self.execute("delete_insert.sql")

    def test_insert(self):
        self.execute("insert.sql")

    def test_update_delete_insert(self):
        self.execute("update_delete_insert.sql")

    def test_update_insert(self):
        self.execute("update_insert.sql")
