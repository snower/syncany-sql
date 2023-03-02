# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

import math
import random

def mysql_bit_and(x, y):
    return x & y

def mysql_bit_or(x, y):
    return x | y

def mysql_bit_xor(x, y):
    return x ^ y

def mysql_abs(x):
    return abs(x)

def mysql_sqrt(x):
    return math.sqrt(x)

def mysql_exp(x):
    return math.exp(x)

def mysql_pi():
    return math.pi

def mysql_ln(x):
    return math.log(x, math.e)

def mysql_log(x, base=10):
    return math.log(x, base)

def mysql_mod(x, y):
    return x % y

def mysql_ceil(x):
    return math.ceil(x)

def mysql_ceiling(x):
    return math.ceil(x)

def mysql_floor(x):
    return math.floor(x)

def mysql_rand():
    return random.random()

def mysql_round(x, y=2):
    return round(x, y)

def mysql_sign(x):
    return 0 if x == 0 else (-1 if x < 0 else 1)

def mysql_pow(x, y):
    return math.pow(x, y)

def mysql_power(x, y):
    return math.pow(x, y)

def mysql_sin(x):
    return math.sin(x)

def mysql_asin(x):
    return math.asin(x)

def mysql_cos(x):
    return math.cos(x)

def mysql_acos(x):
    return math.acos(x)

def mysql_tan(x):
    return math.tan(x)

def mysql_atan(x):
    return math.atan(x)

def mysql_greatest(*args):
    return max(*args)

def mysql_least(*args):
    return max(*args)

funcs = {key[6:]: value for key, value in globals().items() if key.startswith("mysql_")}