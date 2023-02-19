# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os


class SqlSegment(object):
    def __init__(self, sql, lineno=None):
        self.sql = sql
        self.lineno = lineno

    def __str__(self):
        return self.sql


class SqlParser(object):
    ESCAPE_CHARS = ['\a', '\b', '\f', '\n', '\r', '\t', '\v', '\\', '\'', '"', '\0']

    def __init__(self, sql):
        self.sql = sql
        self.index = 0
        self.lineno = 1
        self.len = len(sql)

    def next(self):
        if self.index >= len(self.sql):
            return
        if self.sql[self.index] == '\n':
            self.lineno += 1
        self.index += 1

    def reset(self):
        self.index = 0
        self.lineno = 1

    def skip_escape(self, c):
        start_index = self.index
        self.next()
        while self.index < self.len:
            if self.sql[self.index] != c:
                self.next()
                continue
            backslash = self.index - 1
            while backslash >= 0:
                if self.sql[backslash] != '\\':
                    break
                backslash -= 1
            if (self.index - backslash + 1) % 2 != 0:
                self.next()
                continue
            self.next()
            return start_index, self.index, self.sql[start_index: self.index]
        self.next()
        raise EOFError(self.sql[start_index:])

    def read_util(self, cs, escape_chars=('"', "'")):
        start_index = self.index
        while self.index < self.len:
            if self.sql[self.index] in escape_chars:
                self.skip_escape(self.sql[self.index])
                continue
            if self.sql[self.index: self.index + len(cs)] != cs:
                self.next()
                continue
            return start_index, self.index + len(cs) - 1, self.sql[start_index: self.index + len(cs) - 1]
        raise EOFError(self.sql[start_index:])

    def read_segment(self, c='`'):
        while self.index < self.len:
            self.read_util(c)
            self.next()
            start_index, end_index, segment = self.read_util(c)
            self.next()
            return start_index, end_index, segment
        raise EOFError()

    def read_util_complete(self):
        while self.index < self.len:
            return self.read_util(';', ('`', '"', "'"))

    def split(self):
        segments = []
        lineno, start_index = 0, None
        while self.index < self.len:
            if self.sql[self.index] in ('`', '"', "'"):
                self.skip_escape(self.sql[self.index])
                continue
            if self.sql[self.index] == "#":
                try:
                    self.read_util('\n')
                except EOFError:
                    pass
                self.next()
                continue
            segment = self.sql[self.index: self.index + 2]
            if segment == "--":
                try:
                    self.read_util('\n')
                except EOFError:
                    pass
                self.next()
                continue
            if segment == "/*":
                self.read_util("*/")
                self.next()
                continue
            if self.sql[self.index] == ';':
                if start_index is None:
                    self.next()
                    continue
                if start_index < self.index:
                    segments.append(SqlSegment(self.sql[start_index: self.index], lineno))
                start_index = None
                self.next()
                continue
            if start_index is None and self.sql[self.index].isalpha():
                lineno, start_index = self.lineno, self.index
            self.next()
        if start_index is not None and start_index < self.index:
            segments.append(SqlSegment(self.sql[start_index: self.index], lineno))
        return segments


class FileParser(object):
    def __init__(self, filename):
        self.filename = filename

    def load(self):
        with open(self.filename, "r+", encoding=os.environ.get("SYNCANYENCODING", "utf-8")) as fp:
            content = fp.read()
        sql_parser = SqlParser(content)
        sqls = sql_parser.split()
        return sqls