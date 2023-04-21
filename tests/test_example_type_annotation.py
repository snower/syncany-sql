# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class TypeAnnotationExampleTestCase(ExampleTestCase):
    example_name = "type_annotation"

    def test_type_annotation(self):
        self.execute("type_annotation.sql")
