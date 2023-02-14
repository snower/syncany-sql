# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

class SqlParser(object):
    ESCAPE_CHARS = ['\a', '\b', '\f', '\n', '\r', '\t', '\v', '\\', '\'', '"', '\0']

    def __init__(self, sql):
        self.sql = sql
        self.index = 0
        self.len = len(sql)

    def skip_escape(self, c):
        start_index = self.index
        self.index += 1
        while self.index < self.len:
            if self.sql[self.index] != c:
                self.index += 1
                continue
            backslash = self.index - 1
            while backslash >= 0:
                if self.sql[backslash] != '\\':
                    break
                backslash -= 1
            if (self.index - backslash + 1) % 2 != 0:
                self.index += 1
                continue
            self.index += 1
            return start_index, self.index, self.sql[start_index: self.index]
        self.index += 1
        raise EOFError(self.sql[start_index:])

    def read_util(self, cs, escape_chars=('"', "'")):
        start_index = self.index
        while self.index < self.len:
            if self.sql[self.index] in escape_chars:
                self.skip_escape(self.sql[self.index])
                continue
            if self.sql[self.index: self.index + len(cs)] != cs:
                self.index += 1
                continue
            return start_index, self.index + len(cs) - 1, self.sql[start_index: self.index + len(cs) - 1]
        raise EOFError(self.sql[start_index:])

    def read_segment(self, c='`'):
        while self.index < self.len:
            self.read_util(c)
            self.index += 1
            start_index, end_index, segment = self.read_util(c)
            self.index += 1
            return start_index, end_index, segment
        raise EOFError()

    def read_util_complete(self):
        while self.index < self.len:
            return self.read_util(';', ('`', '"', "'"))

    def split(self):
        segments = []
        start_index = None
        while self.index < self.len:
            if self.sql[self.index] in ('`', '"', "'"):
                self.skip_escape(self.sql[self.index])
                continue
            if self.sql[self.index] == "#":
                try:
                    self.read_util('\n')
                except EOFError:
                    pass
                self.index += 1
                continue
            segment = self.sql[self.index: self.index + 2]
            if segment == "--":
                try:
                    self.read_util('\n')
                except EOFError:
                    pass
                self.index += 1
                continue
            if segment == "/*":
                self.read_util("*/")
                self.index += 1
                continue
            if self.sql[self.index] == ';':
                if start_index is None:
                    self.index += 1
                    continue
                segments.append(self.sql[start_index: self.index])
                start_index = None
                self.index += 1
                continue
            if start_index is None and self.sql[self.index].isalpha():
                start_index = self.index
            self.index += 1
        if start_index is not None:
            segments.append(self.sql[start_index: self.index])
        return segments


class FileParser(object):
    def __init__(self, filename):
        self.filename = filename

    def load(self):
        with open(self.filename, "r+") as fp:
            content = fp.read()
        sql_parser = SqlParser(content)
        sqls = sql_parser.split()
        return sqls