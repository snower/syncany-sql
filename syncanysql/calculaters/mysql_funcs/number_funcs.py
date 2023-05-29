# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

import string
import datetime
import math
import random

def parse_number(x, is_float=False):
    index, dot_index = -1, -1
    for i in range(len(x)):
        if x[i] in string.digits:
            index = i
            continue
        if index >= 0 and is_float and dot_index < 0 and x[i] == ".":
            dot_index = i
            continue
        break
    if index < 0:
        return float(x) if is_float else int(x)
    return float(x[:index + 1]) if dot_index > 0 else int(x[:index + 1])

def ensure_int(x):
    if isinstance(x, int):
        return x
    if x is None:
        raise ValueError('value is None')
    if not x:
        return 0
    if x is True:
        return 1
    if isinstance(x, datetime.date):
        if isinstance(x, datetime.datetime):
            return int(x.strftime("%Y%m%d%H%M%S"))
        return int(x.strftime("%Y%m%d"))
    if isinstance(x, datetime.time):
        return int(x.strftime("%H%M%S"))
    if isinstance(x, str):
        try:
            return int(x)
        except:
            return parse_number(x, False)
    return int(x)

def ensure_float(x):
    if isinstance(x, float):
        return x
    if x is None:
        raise ValueError('value is None')
    if not x:
        return 0
    if x is True:
        return 1
    if isinstance(x, datetime.date):
        if isinstance(x, datetime.datetime):
            return float(x.strftime("%Y%m%d%H%M%S")) + x.microsecond / 1000
        return float(x.strftime("%Y%m%d"))
    if isinstance(x, datetime.time):
        return float(x.strftime("%H%M%S")) + x.microsecond / 1000
    if isinstance(x, str):
        try:
            return float(x)
        except:
            return parse_number(x, True)
    return float(x)

def ensure_number(x):
    if isinstance(x, (int, float)):
        return x
    if x is None:
        raise ValueError('value is None')
    if not x:
        return 0
    if x is True:
        return 1
    if isinstance(x, str):
        if "." in x:
            try:
                return float(x)
            except:
                return ensure_float(x)
        try:
            return int(x)
        except:
            return ensure_int(x)
    if isinstance(x, datetime.date):
        if isinstance(x, datetime.datetime):
            return int(x.strftime("%Y%m%d%H%M%S"))
        return int(x.strftime("%Y%m%d"))
    if isinstance(x, datetime.time):
        return int(x.strftime("%H%M%S"))
    return ensure_int(x)

def mysql_add(x, y):
    return ensure_number(x) + ensure_number(y)

def mysql_sub(x, y):
    return ensure_number(x) - ensure_number(y)

def mysql_mul(x, y):
    return ensure_number(x) * ensure_number(y)

def mysql_div(x, y):
    return ensure_number(x) / ensure_number(y)

def mysql_mod(x, y):
    return ensure_number(x) % ensure_number(y)

def mysql_bitwiseand(x, y):
    try:
        return x & y
    except:
        return ensure_int(x) & ensure_int(y)

def mysql_bitwiseor(x, y):
    try:
        return x | y
    except:
        return ensure_int(x) | ensure_int(y)

def mysql_bitwisenot(x):
    try:
        return ~ x
    except:
        return ~ ensure_int(x)

def mysql_bitwisexor(x, y):
    try:
        return x ^ y
    except:
        return ensure_int(x) ^ ensure_int(y)

def mysql_bitwiserightshift(x, y):
    try:
        return x >> y
    except:
        return ensure_int(x) >> ensure_int(y)

def mysql_bitwiseleftshift(x, y):
    try:
        return x << y
    except:
        return ensure_int(x) << ensure_int(y)

def mysql_bit_and(x, y):
    try:
        return x & y
    except:
        return ensure_int(x) & ensure_int(y)

def mysql_bit_or(x, y):
    try:
        return x | y
    except:
        return ensure_int(x) | ensure_int(y)

def mysql_bit_xor(x, y):
    try:
        return x ^ y
    except:
        return ensure_int(x) ^ ensure_int(y)

def mysql_abs(x):
    try:
        return abs(x)
    except:
        return abs(ensure_number(x))

def mysql_sqrt(x):
    try:
        return math.sqrt(x)
    except:
        return math.sqrt(ensure_number(x))

def mysql_exp(x):
    try:
        return math.exp(x)
    except:
        return math.exp(ensure_number(x))

def mysql_pi():
    return math.pi

def mysql_ln(x):
    try:
        return math.log(x, math.e)
    except:
        return math.log(ensure_number(x), math.e)

def mysql_log(x, base=10):
    try:
        return math.log(x, base)
    except:
        return math.log(ensure_number(x), base)

def mysql_ceil(x):
    try:
        return math.ceil(x)
    except:
        return math.ceil(ensure_number(x))

def mysql_ceiling(x):
    try:
        return math.ceil(x)
    except:
        return math.ceil(ensure_number(x))

def mysql_floor(x):
    try:
        return math.floor(x)
    except:
        return math.floor(ensure_number(x))

def mysql_rand():
    return random.random()

def mysql_round(x, y=2):
    try:
        return round(x, y)
    except:
        return round(ensure_number(x), ensure_int(y))

def mysql_sign(x):
    try:
        return 0 if x == 0 else (-1 if x < 0 else 1)
    except:
        x = ensure_number(x)
        return 0 if x == 0 else (-1 if x < 0 else 1)

def mysql_pow(x, y):
    try:
        return math.pow(x, y)
    except:
        return math.pow(ensure_number(x), ensure_number(y))

def mysql_power(x, y):
    try:
        return math.pow(x, y)
    except:
        return math.pow(ensure_number(x), ensure_number(y))

def mysql_sin(x):
    try:
        return math.sin(x)
    except:
        return math.sin(ensure_number(x))

def mysql_asin(x):
    try:
        return math.asin(x)
    except:
        return math.asin(ensure_number(x))

def mysql_cos(x):
    try:
        return math.cos(x)
    except:
        return math.cos(ensure_number(x))

def mysql_acos(x):
    try:
        return math.acos(x)
    except:
        return math.acos(ensure_number(x))

def mysql_tan(x):
    try:
        return math.tan(x)
    except:
        return math.tan(ensure_number(x))

def mysql_atan(x):
    try:
        return math.atan(x)
    except:
        return math.atan(ensure_number(x))

def mysql_greatest(*args):
    try:
        return max(*args)
    except:
        return max(*tuple([ensure_number(x) for x in args]))

def mysql_least(*args):
    try:
        return min(*args)
    except:
        return min(*tuple([ensure_number(x) for x in args]))


funcs = {key[6:]: value for key, value in globals().items() if key.startswith("mysql_")}