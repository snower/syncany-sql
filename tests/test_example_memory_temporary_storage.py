# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class MemoryTemporaryStorageExampleTestCase(ExampleTestCase):
    example_name = "memory_temporary_storage"

    def test_memory_temporary_storage(self):
        self.execute("memory_temporary_storage.sql")

        self.assert_data(6, [
            {'Id': 'aa7e941b-d399-4bec-0ba4-08d8dd2f9239', 'Email': 'bm6U11zDIspdNW1iQiVZdHX8uqOWZe0cers9BZEcCrE=',
             'FirstName': 'John', 'LastName': 'Doe'}], "data error")
        self.assert_data(8, [{'Id': 'aa7e941b-d399-4bec-0ba4-08d8dd2f9239', 'Name': 'LiMei'}], "data error")
        self.assert_data(10, [
            {'Id': 'aa7e941b-d399-4bec-0ba4-08d8dd2f9239', 'Email': 'bm6U11zDIspdNW1iQiVZdHX8uqOWZe0cers9BZEcCrE=',
             'Name': 'LiMei', 'FirstName': 'John', 'LastName': 'Doe'}], "data error")
