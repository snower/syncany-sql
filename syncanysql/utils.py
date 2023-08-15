# -*- coding: utf-8 -*-
# 2023/2/19
# create by: snower

from decimal import Decimal
import datetime
import string

def parse_value(value):
    value = value.strip()
    if not value:
        return ""
    if len(value) >= 2 and value[0] in ("'", '"') and value[-1] in ("'", '"'):
        return value[1:-1]
    if value.isdigit() or (value[0] == "-" and value[1:].isdigit()):
        return int(value)
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    if value.lower() == "null":
        return None
    value_info = value.split(".")
    if len(value_info) == 2 and (value_info[0].isdigit() or (value[0][0] == "-" and value[0][1:].isdigit())) \
            and value_info[1].isdigit():
        return float(value)
    return value

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
    if isinstance(x, (int, float, Decimal)):
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

def ensure_str(x):
    if isinstance(x, str):
        return x
    if x is None:
        raise ValueError('value is None')
    if not x:
        return '0'
    if x is True:
        return '1'
    if isinstance(x, bytes):
        return x.decode("utf-8")
    if isinstance(x, datetime.date):
        if isinstance(x, datetime.datetime):
            return x.strftime("%Y-%m-%d %H:%M:%S")
        return x.strftime("%Y-%m-%d")
    if isinstance(x, datetime.time):
        return x.strftime("%H:%M:%S")
    return str(x)
