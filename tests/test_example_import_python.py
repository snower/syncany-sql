# -*- coding: utf-8 -*-
# 2023/4/21
# create by: snower

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

        self.assert_value(13, "SYS$VERSION()",
                          '3.10.1 (tags/v3.10.1:2cd268a, Dec  6 2021, 19:10:37) [MSC v.1929 64 bit (AMD64)]',
                          "data error")
        self.assert_value(13, "OS$GETCWD()",
                          'C:\\Users\\admin\\workspace\\projects\\github\\syncany-sql\\examples\\import_python',
                          "data error")
