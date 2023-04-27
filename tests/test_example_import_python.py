# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

import sys
import os
import datetime
from .example import ExampleTestCase


class ImportPythonExampleTestCase(ExampleTestCase):
    example_name = "import_python"

    def test_import_python(self):
        self.execute("import_python.sql")

        self.assert_value(8, "UTILS$HELLO()", 'hello world!', "data error")

        self.assert_value(10, "UTILS$ADD_NUMBER(1, 2)", 3, "data error")
        self.assert_value(10, "UTILS$SUM_ARRAY((1, 2, 3))", 6, "data error")

        self.assert_value(12, "PARSING$PARSE('2023-02-10 10:33:22')",
                          lambda value: isinstance(value, datetime.datetime), "data error")

        self.assert_value(14, "SYS$VERSION()", sys.version, "data error")
        self.assert_value(14, "OS$GETCWD()", lambda value: os.getcwd() in value, "data error")

        self.assert_value(16, "PYTHON_DATETIME$DATETIME$NOW()",
                          lambda value: isinstance(value, datetime.datetime), "data error")
