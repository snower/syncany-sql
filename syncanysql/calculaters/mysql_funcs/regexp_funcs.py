# -*- coding: utf-8 -*-
# 2025/3/21
# create by: snower

import re
from syncany.calculaters import typing_filter

def parse_flags(match_type):
    if not match_type:
        return re.IGNORECASE
    if match_type.isdigit():
        return int(match_type) | re.IGNORECASE
    flags = re.IGNORECASE
    for c in match_type.lower():
        if c == 'c':
            flags = flags & (~re.IGNORECASE)
        elif c == 'i':
            flags = flags | re.IGNORECASE
        elif c == 'm':
            flags = flags | re.MULTILINE
        elif c == 'n':
            flags = flags | re.DOTALL
        elif c == 'u':
            flags = flags | re.UNICODE
    return flags

@typing_filter(int)
def mysql_regexp(s, r, match_type=None):
    if r is None or s is None:
        return None
    try:
        return 1 if re.match(r, s, parse_flags(match_type)) else 0
    except:
        return 0

@typing_filter(int)
def mysql_regexplike(s, r, match_type=None):
    if r is None or s is None:
        return None
    try:
        return 1 if re.match(r, s, parse_flags(match_type)) else 0
    except:
        return 0

@typing_filter(int)
def mysql_regexp_instr(s, r, pos=1, occurrence=1, return_option=0, match_type=None):
    if r is None or s is None:
        return None
    try:
        flags = parse_flags(match_type)
        cs = s[pos-1:]
        while cs:
            m = re.search(r, cs, flags)
            if not m:
                return 0
            start_index, end_index = m.span()
            if occurrence <= 1:
                return pos + (end_index if return_option else start_index)
            cs = cs[end_index:]
            occurrence -= 1
            pos += end_index
        return 0
    except:
        return 0

@typing_filter(str)
def mysql_regexp_replace(s, r, rs, pos=1, occurrence=0, match_type=None):
    if r is None or s is None or rs is None:
        return None
    try:
        flags = parse_flags(match_type)
        cs = s[pos-1:]
        if occurrence <= 0:
            return re.sub(r, rs, cs, 0, flags)
        while cs:
            m = re.search(r, cs, flags)
            if not m:
                return s
            start_index, end_index = m.span()
            if occurrence <= 1:
                return s[:pos + start_index - 1] + rs + s[pos + end_index - 1:]
            cs = cs[end_index:]
            occurrence -= 1
            pos += end_index
        return s
    except:
        return None

@typing_filter(str)
def mysql_regexp_substr(s, r, pos=1, occurrence=1, match_type=None):
    if r is None or s is None:
        return None
    try:
        flags = parse_flags(match_type)
        cs = s[pos-1:]
        while cs:
            m = re.search(r, cs, flags)
            if not m:
                return None
            start_index, end_index = m.span()
            if occurrence <= 1:
                return cs[start_index:end_index]
            cs = cs[end_index:]
            occurrence -= 1
            pos += end_index
        return None
    except:
        return None

@typing_filter(int)
def mysql_like(s, r, match_type=None):
    if r is None or s is None:
        return None
    r = ".*" if r == '%%' else "".join([rg.replace("%", ".*") for rg in re.escape(r).split("%%")])
    try:
        return 1 if re.match(r, s, parse_flags(match_type)) else 0
    except:
        return 0

funcs = {key[6:]: value for key, value in globals().items() if key.startswith("mysql_")}