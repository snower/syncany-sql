# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

class FileParser(object):
    def __init__(self, filename):
        self.filename = filename

    def load(self):
        with open(self.filename, "r+") as fp:
            content = fp.read()
            content = "\n".join([line for line in content.split("\n") if not line.strip().startswith("--")])
        sqls = []
        for sql in content.split(";"):
            sql = sql.strip()
            if not sql:
                continue
            sqls.append(sql)
        return sqls