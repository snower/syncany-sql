# -*- coding: utf-8 -*-
# 2023/2/19
# create by: snower

def parse_value(value):
    value = value.strip()
    if not value:
        return ""
    if len(value) >= 2 and value[0] in ("'", '"') and value[-1] in ("'", '"'):
        return value[1:-1]
    if value.isdigit():
        return int(value)
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    if value.lower() == "null":
        return None
    value_info = value.split(".")
    if len(value_info) == 2 and value_info[0].isdigit() and value_info[1].isdigit():
        return float(value)
    return value