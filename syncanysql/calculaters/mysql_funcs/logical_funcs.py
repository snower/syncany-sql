# -*- coding: utf-8 -*-
# 2023/5/29
# create by: snower

from syncany.calculaters import typing_filter
from ...utils import NumberDecimalTypes, ensure_number, ensure_str

@typing_filter(int)
def mysql_eq(a, b):
    if type(a) is type(b):
        if a is None:
            return None
        return 1 if a == b else 0
    if a is None or b is None:
        return None
    if isinstance(a, NumberDecimalTypes):
        if isinstance(b, NumberDecimalTypes):
            return 1 if a == b else 0
        try:
            return 1 if ensure_number(a) == ensure_number(b) else 0
        except:
            return 1 if a == b else 0
    if isinstance(a, str):
        if isinstance(b, str):
            return 1 if a == b else 0
        return 1 if ensure_str(a) == ensure_str(b) else 0
    if isinstance(a, bool):
        if isinstance(b, bool):
            return 1 if a == b else 0
        try:
            return 1 if ensure_number(a) == ensure_number(b) else 0
        except:
            return 1 if a == b else 0
    return 1 if a == b else 0

@typing_filter(int)
def mysql_neq(a, b):
    if type(a) is type(b):
        if a is None:
            return None
        return 1 if a != b else 0
    if a is None or b is None:
        return None
    if isinstance(a, NumberDecimalTypes):
        if isinstance(b, NumberDecimalTypes):
            return 1 if a != b else 0
        try:
            return 1 if ensure_number(a) != ensure_number(b) else 0
        except:
            return 1 if a != b else 0
    if isinstance(a, str):
        if isinstance(b, str):
            return 1 if a != b else 0
        return 1 if ensure_str(a) != ensure_str(b) else 0
    if isinstance(a, bool):
        if isinstance(b, bool):
            return 1 if a != b else 0
        try:
            return 1 if ensure_number(a) != ensure_number(b) else 0
        except:
            return 1 if a != b else 0
    return 1 if a != b else 0

@typing_filter(int)
def mysql_gt(a, b):
    if type(a) is type(b):
        if a is None:
            return None
        try:
            return 1 if a > b else 0
        except:
            pass
    if a is None or b is None:
        return None
    if isinstance(a, NumberDecimalTypes):
        if isinstance(b, NumberDecimalTypes):
            return 1 if a > b else 0
        try:
            return 1 if ensure_number(a) > ensure_number(b) else 0
        except:
            return 1 if a > b else 0
    if isinstance(a, str):
        if isinstance(b, str):
            return 1 if a > b else 0
        return 1 if ensure_str(a) > ensure_str(b) else 0
    if isinstance(a, bool):
        if isinstance(b, bool):
            return 1 if a > b else 0
        try:
            return 1 if ensure_number(a) > ensure_number(b) else 0
        except:
            return 1 if a > b else 0
    return 1 if a > b else 0

@typing_filter(int)
def mysql_gte(a, b):
    if type(a) is type(b):
        if a is None:
            return None
        try:
            return 1 if a >= b else 0
        except:
            pass
    if a is None or b is None:
        return None
    if isinstance(a, NumberDecimalTypes):
        if isinstance(b, NumberDecimalTypes):
            return 1 if a >= b else 0
        try:
            return 1 if ensure_number(a) >= ensure_number(b) else 0
        except:
            return 1 if a >= b else 0
    if isinstance(a, str):
        if isinstance(b, str):
            return 1 if a >= b else 0
        return 1 if ensure_str(a) >= ensure_str(b) else 0
    if isinstance(a, bool):
        if isinstance(b, bool):
            return 1 if a >= b else 0
        try:
            return 1 if ensure_number(a) >= ensure_number(b) else 0
        except:
            return 1 if a >= b else 0
    return 1 if a >= b else 0

@typing_filter(int)
def mysql_lt(a, b):
    if type(a) is type(b):
        if a is None:
            return None
        try:
            return 1 if a < b else 0
        except:
            pass
    if a is None or b is None:
        return None
    if isinstance(a, NumberDecimalTypes):
        if isinstance(b, NumberDecimalTypes):
            return 1 if a < b else 0
        try:
            return 1 if ensure_number(a) < ensure_number(b) else 0
        except:
            return 1 if a < b else 0
    if isinstance(a, str):
        if isinstance(b, str):
            return 1 if a < b else 0
        return 1 if ensure_str(a) < ensure_str(b) else 0
    if isinstance(a, bool):
        if isinstance(b, bool):
            return 1 if a < b else 0
        try:
            return 1 if ensure_number(a) < ensure_number(b) else 0
        except:
            return 1 if a < b else 0
    return 1 if a < b else 0

@typing_filter(int)
def mysql_lte(a, b):
    if type(a) is type(b):
        if a is None:
            return None
        try:
            return 1 if a <= b else 0
        except:
            pass
    if a is None or b is None:
        return None
    if isinstance(a, NumberDecimalTypes):
        if isinstance(b, NumberDecimalTypes):
            return 1 if a <= b else 0
        try:
            return 1 if ensure_number(a) <= ensure_number(b) else 0
        except:
            return 1 if a <= b else 0
    if isinstance(a, str):
        if isinstance(b, str):
            return 1 if a <= b else 0
        return 1 if ensure_str(a) <= ensure_str(b) else 0
    if isinstance(a, bool):
        if isinstance(b, bool):
            return 1 if a <= b else 0
        try:
            return 1 if ensure_number(a) <= ensure_number(b) else 0
        except:
            return 1 if a <= b else 0
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
    if type(a) is type(b):
        return 1 if a is b else 0
    if a is None or b is None:
        return 1 if a is b else 0
    return mysql_eq(a, b)

@typing_filter(int)
def mysql_is_not(a, b):
    if type(a) is type(b):
        return 0 if a is b else 1
    if a is None or b is None:
        return 0 if a is b else 1
    return mysql_neq(a, b)

@typing_filter(int)
def mysql_exists(a):
    if a is None:
        return 0
    if isinstance(a, dict):
        return 1
    return 1 if a else 0


funcs = {key[6:]: value for key, value in globals().items() if key.startswith("mysql_")}