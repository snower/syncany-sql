# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class TransformExampleTestCase(ExampleTestCase):
    example_name = "transform"

    def test_transform_h2v(self):
        self.execute("transform_h2v.sql")

    def test_transform_h4v(self):
        self.execute("transform_h4v.sql")

    def test_transform_uniqkv(self):
        self.execute("transform_uniqkv.sql")

    def test_transform_v2h(self):
        self.execute("transform_v2h.sql")

    def test_transform_v4h(self):
        self.execute("transform_v4h.sql")
