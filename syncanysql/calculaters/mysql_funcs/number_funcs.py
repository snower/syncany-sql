# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

import math
import random
from syncany.calculaters import typing_filter
from ...utils import ensure_number, ensure_int

@typing_filter(float)
def mysql_add(x, y):
    if x.__class__ is int or x.__class__ is float:
        if y.__class__ is int or y.__class__ is float:
            return x + y
    if x is None or y is None:
        return None
    return ensure_number(x) + ensure_number(y)

@typing_filter(float)
def mysql_sub(x, y):
    if x.__class__ is int or x.__class__ is float:
        if y.__class__ is int or y.__class__ is float:
            return x - y
    if x is None or y is None:
        return None
    return ensure_number(x) - ensure_number(y)

@typing_filter(float)
def mysql_mul(x, y):
    if x.__class__ is int or x.__class__ is float:
        if y.__class__ is int or y.__class__ is float:
            return x * y
    if x is None or y is None:
        return None
    return ensure_number(x) * ensure_number(y)

@typing_filter(float)
def mysql_div(x, y):
    if x.__class__ is int or x.__class__ is float:
        if y.__class__ is int or y.__class__ is float:
            return x / y
    if x is None or y is None:
        return None
    return ensure_number(x) / ensure_number(y)

@typing_filter(float)
def mysql_mod(x, y):
    if x.__class__ is int or x.__class__ is float:
        if y.__class__ is int or y.__class__ is float:
            return x % y
    if x is None or y is None:
        return None
    return ensure_number(x) % ensure_number(y)

@typing_filter(int)
def mysql_bitwiseand(x, y):
    if x is None or y is None:
        return None
    try:
        return x & y
    except:
        return ensure_int(x) & ensure_int(y)

@typing_filter(int)
def mysql_bitwiseor(x, y):
    if x is None or y is None:
        return None
    try:
        return x | y
    except:
        return ensure_int(x) | ensure_int(y)

@typing_filter(int)
def mysql_bitwisenot(x):
    if x is None:
        return None
    try:
        return ~ x
    except:
        return ~ ensure_int(x)

@typing_filter(int)
def mysql_bitwisexor(x, y):
    if x is None or y is None:
        return None
    try:
        return x ^ y
    except:
        return ensure_int(x) ^ ensure_int(y)

@typing_filter(int)
def mysql_bitwiserightshift(x, y):
    if x is None or y is None:
        return None
    try:
        return x >> y
    except:
        return ensure_int(x) >> ensure_int(y)

@typing_filter(int)
def mysql_bitwiseleftshift(x, y):
    if x is None or y is None:
        return None
    try:
        return x << y
    except:
        return ensure_int(x) << ensure_int(y)

@typing_filter(int)
def mysql_bit_and(x, y):
    if x is None or y is None:
        return None
    try:
        return x & y
    except:
        return ensure_int(x) & ensure_int(y)

@typing_filter(int)
def mysql_bit_or(x, y):
    if x is None or y is None:
        return None
    try:
        return x | y
    except:
        return ensure_int(x) | ensure_int(y)

@typing_filter(int)
def mysql_bit_xor(x, y):
    if x is None or y is None:
        return None
    try:
        return x ^ y
    except:
        return ensure_int(x) ^ ensure_int(y)

@typing_filter(float)
def mysql_abs(x):
    if x is None:
        return None
    try:
        return abs(x)
    except:
        return abs(ensure_number(x))

@typing_filter(float)
def mysql_sqrt(x):
    if x is None:
        return None
    try:
        return math.sqrt(x)
    except:
        return math.sqrt(ensure_number(x))

@typing_filter(float)
def mysql_exp(x):
    if x is None:
        return None
    try:
        return math.exp(x)
    except:
        return math.exp(ensure_number(x))

@typing_filter(float)
def mysql_pi():
    return math.pi

@typing_filter(float)
def mysql_ln(x):
    if x is None:
        return None
    try:
        return math.log(x, math.e)
    except:
        return math.log(ensure_number(x), math.e)

@typing_filter(float)
def mysql_log(x, base=10):
    if x is None:
        return None
    try:
        return math.log(x, base)
    except:
        return math.log(ensure_number(x), base)

@typing_filter(int)
def mysql_ceil(x):
    if x is None:
        return None
    try:
        return math.ceil(x)
    except:
        return math.ceil(ensure_number(x))

@typing_filter(int)
def mysql_ceiling(x):
    if x is None:
        return None
    try:
        return math.ceil(x)
    except:
        return math.ceil(ensure_number(x))

@typing_filter(int)
def mysql_floor(x):
    if x is None:
        return None
    try:
        return math.floor(x)
    except:
        return math.floor(ensure_number(x))

@typing_filter(float)
def mysql_rand():
    return random.random()

@typing_filter(float)
def mysql_round(x, y=2):
    if x is None or y is None:
        return None
    try:
        return round(x, y)
    except:
        return round(ensure_number(x), ensure_int(y))

@typing_filter(float)
def mysql_sign(x):
    if x is None:
        return None
    try:
        return 0 if x == 0 else (-1 if x < 0 else 1)
    except:
        x = ensure_number(x)
        return 0 if x == 0 else (-1 if x < 0 else 1)

@typing_filter(float)
def mysql_pow(x, y):
    if x is None or y is None:
        return None
    try:
        return math.pow(x, y)
    except:
        return math.pow(ensure_number(x), ensure_number(y))

@typing_filter(float)
def mysql_power(x, y):
    if x is None or y is None:
        return None
    try:
        return math.pow(x, y)
    except:
        return math.pow(ensure_number(x), ensure_number(y))

@typing_filter(float)
def mysql_sin(x):
    if x is None:
        return None
    try:
        return math.sin(x)
    except:
        return math.sin(ensure_number(x))

@typing_filter(float)
def mysql_asin(x):
    if x is None:
        return None
    try:
        return math.asin(x)
    except:
        return math.asin(ensure_number(x))

@typing_filter(float)
def mysql_cos(x):
    if x is None:
        return None
    try:
        return math.cos(x)
    except:
        return math.cos(ensure_number(x))

@typing_filter(float)
def mysql_acos(x):
    if x is None:
        return None
    try:
        return math.acos(x)
    except:
        return math.acos(ensure_number(x))

@typing_filter(float)
def mysql_tan(x):
    if x is None:
        return None
    try:
        return math.tan(x)
    except:
        return math.tan(ensure_number(x))

@typing_filter(float)
def mysql_atan(x):
    if x is None:
        return None
    try:
        return math.atan(x)
    except:
        return math.atan(ensure_number(x))

@typing_filter(float)
def mysql_greatest(*args):
    try:
        return max(*args)
    except:
        return max(*tuple([ensure_number(x) for x in args]))

@typing_filter(float)
def mysql_least(*args):
    try:
        return min(*args)
    except:
        return min(*tuple([ensure_number(x) for x in args]))


funcs = {key[6:]: value for key, value in globals().items() if key.startswith("mysql_")}