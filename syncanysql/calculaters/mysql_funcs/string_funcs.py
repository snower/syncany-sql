# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

import socket
import base64
import binascii
from syncany.calculaters import typing_filter
from ...utils import NumberTypes, ensure_str

@typing_filter(str)
def mysql_bin(x):
    if isinstance(x, NumberTypes):
        return bin(int(x))
    if x is None:
        return None
    if x is True:
        return bin(1)
    if not x:
        return bin(0)
    return bin(x)

@typing_filter(str)
def mysql_hex(x):
    if isinstance(x, NumberTypes):
        return hex(int(x))
    if x is None:
        return None
    if x is True:
        return hex(1)
    if not x:
        return hex(0)
    return binascii.b2a_hex(x.encode("utf-8")).decode("utf-8")

@typing_filter(str)
def mysql_unhex(x):
    if x is None:
        return None
    return binascii.a2b_hex(ensure_str(x))

@typing_filter(str)
def mysql_oct(x):
    if x is None:
        return None
    return oct(x)[2:]

@typing_filter(int)
def mysql_ord(x):
    if x is None:
        return None
    return ord(ensure_str(x))

@typing_filter(str)
def mysql_ascii(s):
    if s is None:
        return None
    return sum([ord(c) for c in ensure_str(s)])

@typing_filter(str)
def mysql_char(*args):
    return "".join([chr(arg) for arg in args])

@typing_filter(int)
def mysql_bit_length(s):
    if s is None:
        return None
    return len(s.encode("utf-8")) * 8

@typing_filter(int)
def mysql_length(s):
    if s is None:
        return None
    return len(s.encode("utf-8"))

@typing_filter(int)
def mysql_char_length(s):
    if s is None:
        return None
    return len(ensure_str(s))

@typing_filter(int)
def mysql_character_length(s):
    if s is None:
        return None
    return len(ensure_str(s))

@typing_filter(str)
def mysql_concat(*args):
    return "".join(ensure_str(arg) for arg in args)

@typing_filter(str)
def mysql_concat_ws(sep, *args):
    return sep.join(ensure_str(arg) for arg in args)

@typing_filter(str)
def mysql_insert(s1, x, l, s2):
    s1, s2 = ensure_str(s1), ensure_str(s2)
    return s1[: x - 1] + s2 + s1[x + l - 1:]

@typing_filter(str)
def mysql_lower(s):
    if s is None:
        return None
    return ensure_str(s).lower()

@typing_filter(str)
def mysql_upper(s):
    if s is None:
        return None
    return ensure_str(s).upper()

@typing_filter(str)
def mysql_ucase(s):
    if s is None:
        return None
    return ensure_str(s).upper()

@typing_filter(str)
def mysql_left(s, x):
    if s is None:
        return None
    return ensure_str(s)[:x]

@typing_filter(str)
def mysql_right(s, x):
    if s is None:
        return None
    return ensure_str(s)[-x:]

@typing_filter(str)
def mysql_trim(s):
    if s is None:
        return None
    return ensure_str(s).strip()

@typing_filter(str)
def mysql_elt(n, *args):
    return args[n - 1] if n < len(args) else None

@typing_filter(int)
def mysql_field(s, *args):
    if s is None:
        return None
    try:
        return args.index(s) + 1
    except ValueError:
        return 0

@typing_filter(int)
def mysql_find_in_set(s, ss):
    if s is None:
        return None
    try:
        return ensure_str(ss).split(",").index(s) + 1
    except ValueError:
        return 0

@typing_filter(str)
def mysql_replace(s, s1, s2):
    if s is None:
        return None
    return ensure_str(s).replace(s1, s2)

@typing_filter(str)
def mysql_substring(s, n, l=None):
    if s is None:
        return None
    s = ensure_str(s)
    if isinstance(l, int):
        return s[n - 1: n + l - 1]
    return s[n - 1:]

@typing_filter(str)
def mysql_substr(s, n, l=None):
    if s is None:
        return None
    s = ensure_str(s)
    if isinstance(l, int):
        return s[n - 1: n + l - 1]
    return s[n - 1:]

@typing_filter(str)
def mysql_substring_index(s, d, c):
    if s is None:
        return None
    s, d = ensure_str(s), ensure_str(d)
    if c < 0:
        return d.join(s.split(d)[-c:])
    return d.join(s.split(d)[:c])

@typing_filter(str)
def mysql_repeat(s, c):
    if s is None:
        return None
    return ensure_str(s) * c

@typing_filter(str)
def mysql_reverse(s):
    if s is None:
        return None
    return ensure_str(s)[::-1]

@typing_filter(int)
def mysql_strcmp(s1, s2):
    if s1 is None or s2 is None:
        return None
    s1, s2 = ensure_str(s1), ensure_str(s2)
    return 0 if s1 == s2 else (-1 if s1 < s2 else 1)

@typing_filter(int)
def mysql_startswith(s1, s2):
    if s1 is None or s2 is None:
        return None
    s1, s2 = ensure_str(s1), ensure_str(s2)
    return 1 if s1.startswith(s2) else 0

@typing_filter(int)
def mysql_endswith(s1, s2):
    if s1 is None or s2 is None:
        return None
    s1, s2 = ensure_str(s1), ensure_str(s2)
    return 1 if s1.endswith(s2) else 0

@typing_filter(int)
def mysql_contains(s1, s2):
    if s1 is None or s2 is None:
        return None
    try:
        return 1 if s2 in s1 else 0
    except:
        s1, s2 = ensure_str(s1), ensure_str(s2)
        return 1 if s2 in s1 else 0

@typing_filter(int)
def mysql_crc32(s):
    if s is None:
        return None
    import zlib
    return zlib.crc32(s)

@typing_filter(str)
def mysql_from_base64(s):
    if s is None:
        return None
    return base64.b64decode(s.encode("utf-8")).decode("utf-8")

@typing_filter(str)
def mysql_to_base64(s):
    if s is None:
        return None
    return base64.b64encode(s.encode("utf-8")).decode("utf-8")

@typing_filter(str)
def mysql_inet4_aton(s):
    if s is None:
        return None
    try:
        return binascii.b2a_hex(socket.inet_aton(s)).decode("utf-8")
    except:
        return None

@typing_filter(str)
def mysql_inet4_ntoa(b):
    if b is None:
        return None
    try:
        return socket.inet_ntoa(b)
    except:
        return None

@typing_filter(int)
def mysql_is_ipv4(s):
    if s is None:
        return None
    try:
        socket.inet_aton(s)
        return 1
    except:
        return 0

@typing_filter(str)
def mysql_inet6_aton(s):
    if s is None:
        return None
    try:
        return binascii.b2a_hex(socket.inet_pton(socket.AF_INET6, s)).decode("utf-8")
    except:
        return None

@typing_filter(str)
def mysql_inet6_ntoa(b):
    if b is None:
        return None
    try:
        return socket.inet_ntop(socket.AF_INET6, b)
    except:
        return None

@typing_filter(int)
def mysql_is_ipv6(s):
    if s is None:
        return None
    try:
        socket.inet_pton(socket.AF_INET6, s)
        return 1
    except:
        return 0


funcs = {key[6:]: value for key, value in globals().items() if key.startswith("mysql_")}
