# -*- coding: utf-8 -*-
# 2023/5/29
# create by: snower

from syncany.calculaters import typing_filter
from ...utils import ensure_number, ensure_str

@typing_filter(int)
def mysql_eq(a, b):
    if a is None or b is None:
        return None
    if isinstance(a, (int, float, bool)) or isinstance(b, (int, float, bool)):
        try:
            return 1 if ensure_number(a) == ensure_number(b) else 0
        except:
            pass
    if isinstance(a, str) or isinstance(b, str):
        return 1 if ensure_str(a) == ensure_str(b) else 0
    return 1 if a == b else 0

@typing_filter(int)
def mysql_neq(a, b):
    if a is None or b is None:
        return None
    if isinstance(a, (int, float, bool)) or isinstance(b, (int, float, bool)):
        try:
            return 1 if ensure_number(a) != ensure_number(b) else 0
        except:
            pass
    if isinstance(a, str) or isinstance(b, str):
        return 1 if ensure_str(a) != ensure_str(b) else 0
    return 1 if a != b else 0

@typing_filter(int)
def mysql_gt(a, b):
    if a is None or b is None:
        return None
    if isinstance(a, (int, float, bool)) or isinstance(b, (int, float, bool)):
        try:
            return 1 if ensure_number(a) > ensure_number(b) else 0
        except:
            pass
    if isinstance(a, str) or isinstance(b, str):
        return 1 if ensure_str(a) > ensure_str(b) else 0
    return 1 if a > b else 0

@typing_filter(int)
def mysql_gte(a, b):
    if a is None or b is None:
        return None
    if isinstance(a, (int, float, bool)) or isinstance(b, (int, float, bool)):
        try:
            return 1 if ensure_number(a) >= ensure_number(b) else 0
        except:
            pass
    if isinstance(a, str) or isinstance(b, str):
        return 1 if ensure_str(a) >= ensure_str(b) else 0
    return 1 if a >= b else 0

@typing_filter(int)
def mysql_lt(a, b):
    if a is None or b is None:
        return None
    if isinstance(a, (int, float, bool)) or isinstance(b, (int, float, bool)):
        try:
            return 1 if ensure_number(a) < ensure_number(b) else 0
        except:
            pass
    if isinstance(a, str) or isinstance(b, str):
        return 1 if ensure_str(a) < ensure_str(b) else 0
    return 1 if a < b else 0

@typing_filter(int)
def mysql_lte(a, b):
    if a is None or b is None:
        return None
    if isinstance(a, (int, float, bool)) or isinstance(b, (int, float, bool)):
        try:
            return 1 if ensure_number(a) <= ensure_number(b) else 0
        except:
            pass
    if isinstance(a, str) or isinstance(b, str):
        return 1 if ensure_str(a) <= ensure_str(b) else 0
    return 1 if a <= b else 0

@typing_filter(int)
def mysql_in(a, b):
    if a is None:
        return None
    hs_none = False
    for v in b:
        if v is None:
            hs_none = True
            continue
        if mysql_eq(a, v):
            return 1
    return None if hs_none else 0

@typing_filter(int)
def mysql_not(a):
    if a is None:
        return None
    return 1 if not a else 0

@typing_filter(int)
def mysql_is(a, b):
    if a is None or b is None:
        return 1 if a is b else 0
    return mysql_eq(a, b)

@typing_filter(int)
def mysql_is_not(a, b):
    if a is None or b is None:
        return 0 if a is b else 1
    return mysql_neq(a, b)

funcs = {key[6:]: value for key, value in globals().items() if key.startswith("mysql_")}