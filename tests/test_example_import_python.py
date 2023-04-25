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

        self.assert_value(7, "UTILS$HELLO()", 'hello world!', "data error")

        self.assert_value(9, "UTILS$ADD_NUMBER(1, 2)", 3, "data error")
        self.assert_value(9, "UTILS$SUM_ARRAY((1, 2, 3))", 6, "data error")

        self.assert_value(11, "PARSING$PARSE('2023-02-10 10:33:22')",
                          lambda value: isinstance(value, datetime.datetime), "data error")

        self.assert_value(13, "SYS$VERSION()", sys.version, "data error")
        self.assert_value(13, "OS$GETCWD()", lambda value: os.getcwd() in value, "data error")
