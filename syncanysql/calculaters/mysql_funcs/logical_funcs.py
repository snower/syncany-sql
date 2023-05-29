# -*- coding: utf-8 -*-
# 2023/5/29
# create by: snower

from .number_funcs import ensure_number
from .string_funcs import ensure_str

def mysql_eq(a, b):
    if isinstance(a, (int, float, bool)) or isinstance(b, (int, float, bool)):
        return 1 if ensure_number(a) == ensure_number(b) else 0
    if isinstance(a, str) or isinstance(b, str):
        return 1 if ensure_str(a) == ensure_str(b) else 0
    return 1 if a == b else 0

def mysql_neq(a, b):
    if isinstance(a, (int, float, bool)) or isinstance(b, (int, float, bool)):
        return 1 if ensure_number(a) != ensure_number(b) else 0
    if isinstance(a, str) or isinstance(b, str):
        return 1 if ensure_str(a) != ensure_str(b) else 0
    return 1 if a != b else 0

def mysql_gt(a, b):
    if isinstance(a, (int, float, bool)) or isinstance(b, (int, float, bool)):
        return 1 if ensure_number(a) > ensure_number(b) else 0
    if isinstance(a, str) or isinstance(b, str):
        return 1 if ensure_str(a) > ensure_str(b) else 0
    return 1 if a > b else 0

def mysql_gte(a, b):
    if isinstance(a, (int, float, bool)) or isinstance(b, (int, float, bool)):
        return 1 if ensure_number(a) >= ensure_number(b) else 0
    if isinstance(a, str) or isinstance(b, str):
        return 1 if ensure_str(a) >= ensure_str(b) else 0
    return 1 if a >= b else 0

def mysql_lt(a, b):
    if isinstance(a, (int, float, bool)) or isinstance(b, (int, float, bool)):
        return 1 if ensure_number(a) < ensure_number(b) else 0
    if isinstance(a, str) or isinstance(b, str):
        return 1 if ensure_str(a) < ensure_str(b) else 0
    return 1 if a < b else 0

def mysql_lte(a, b):
    if isinstance(a, (int, float, bool)) or isinstance(b, (int, float, bool)):
        return 1 if ensure_number(a) <= ensure_number(b) else 0
    if isinstance(a, str) or isinstance(b, str):
        return 1 if ensure_str(a) <= ensure_str(b) else 0
    return 1 if a <= b else 0

def mysql_in(a, b):
    if a is None:
        return None
    hs_none = False
    for v in b:
        if b is None:
            hs_none = True
            continue
        if mysql_eq(a, v):
            return 1
    return None if hs_none else 0

def mysql_not(a):
    if a is None:
        return None
    return 1 if not a else 0

def mysql_is(a, b):
    if a is None or b is None:
        return 1 if a is b else 0
    return mysql_eq(a, b)

funcs = {key[6:]: value for key, value in globals().items() if key.startswith("mysql_")}