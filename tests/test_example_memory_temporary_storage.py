# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class MemoryTemporaryStorageExampleTestCase(ExampleTestCase):
    example_name = "memory_temporary_storage"

    def test_memory_temporary_storage(self):
        self.execute("memory_temporary_storage.sql")
