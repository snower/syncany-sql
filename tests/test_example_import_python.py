# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

from .example import ExampleTestCase


class ImportPythonExampleTestCase(ExampleTestCase):
    example_name = "import_python"

    def test_import_python(self):
        self.execute("import_python.sql")
