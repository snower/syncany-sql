# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

class FileParser(object):
    def __init__(self, filename):
        self.filename = filename

    def load(self):
        with open(self.filename, "r+") as fp:
            content = fp.read()
        return [content]