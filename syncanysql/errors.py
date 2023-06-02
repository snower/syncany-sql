# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

from syncany.errors import SyncanyException

class SyncanySqlException(SyncanyException):
    pass

class SyncanySqlCompileException(SyncanySqlException):
    pass

class SyncanySqlExecutorException(SyncanySqlException):
    pass