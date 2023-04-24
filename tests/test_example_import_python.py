# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class ImportPythonExampleTestCase(ExampleTestCase):
    example_name = "import_python"

    def test_import_python(self):
        self.execute("import_python.sql")

        self.assert_data(4, [{'UTILS$HELLO()': 'hello world!'}], "data error")
        self.assert_data(6, [{'UTILS$ADD_NUMBER(1, 2)': 3, 'UTILS$SUM_ARRAY((1, 2, 3))': 6}], "data error")
