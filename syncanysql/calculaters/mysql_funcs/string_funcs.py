# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

import socket
import base64
import binascii

def mysql_bin(x):
    return bin(x)

def mysql_hex(x):
    if isinstance(x, (int, float)):
        return hex(int(x))
    return binascii.b2a_hex(x.encode("utf-8")).decode("utf-8")

def mysql_unhex(x):
    return binascii.a2b_hex(x)

def mysql_oct(x):
    return oct(x)[2:]

def mysql_ord(x):
    return ord(x)

def mysql_ascii(s):
    return sum([ord(c) for c in s])

def mysql_char(*args):
    return "".join([chr(arg) for arg in args])

def mysql_bit_length(s):
    return len(s.encode("utf-8")) * 8

def mysql_length(s):
    return len(s.encode("utf-8"))

def mysql_char_length(s):
    return len(s)

def mysql_character_length(s):
    return len(s)

def mysql_concat(*args):
    return "".join(str(arg) for arg in args)

def mysql_concat_ws(sep, *args):
    return sep.join(str(arg) for arg in args)

def mysql_insert(s1, x, l, s2):
    return s1[: x - 1] + s2 + s1[x + l - 1:]

def mysql_lower(s):
    return s.lower()

def mysql_upper(s):
    return s.upper()

def mysql_ucase(s):
    return s.upper()

def mysql_left(s, x):
    return s[:x]

def mysql_right(s, x):
    return s[-x:]

def mysql_trim(s):
    return s.strip()

def mysql_elt(n, *args):
    return args[n - 1] if n < len(args) else None

def mysql_field(s, *args):
    try:
        return args.index(s) + 1
    except ValueError:
        return 0

def mysql_find_in_set(s, ss):
    try:
        return ss.split(",").index(s) + 1
    except ValueError:
        return 0

def mysql_replace(s, s1, s2):
    return s.replace(s1, s2)

def mysql_substring(s, n, l=None):
    if isinstance(l, int):
        return s[n - 1: n + l - 1]
    return s[n - 1:]

def mysql_substr(s, n, l=None):
    if isinstance(l, int):
        return s[n - 1: n + l - 1]
    return s[n - 1:]

def mysql_substring_index(s, d, c):
    if c < 0:
        return d.join(s.split(d)[-c:])
    return d.join(s.split(d)[:c])

def mysql_repeat(s, c):
    return s * c

def mysql_reverse(s):
    return s[::-1]

def mysql_strcmp(s1, s2):
    return 0 if s1 == s2 else (-1 if s1 < s2 else 1)

def mysql_crc32(s):
    import zlib
    return zlib.crc32(s)

def mysql_from_base64(s):
    return base64.b64decode(s.encode("utf-8")).decode("utf-8")

def mysql_to_base64(s):
    return base64.b64encode(s.encode("utf-8")).decode("utf-8")

def mysql_inet4_aton(s):
    try:
        return binascii.b2a_hex(socket.inet_aton(s)).decode("utf-8")
    except:
        return None

def mysql_inet4_ntoa(b):
    try:
        return socket.inet_ntoa(b)
    except:
        return None

def mysql_is_ipv4(s):
    try:
        socket.inet_aton(s)
        return 1
    except:
        return 0

def mysql_inet6_aton(s):
    try:
        return binascii.b2a_hex(socket.inet_pton(socket.AF_INET6, s)).decode("utf-8")
    except:
        return None

def mysql_inet6_ntoa(b):
    try:
        return socket.inet_ntop(socket.AF_INET6, b)
    except:
        return None

def mysql_is_ipv6(s):
    try:
        socket.inet_pton(socket.AF_INET6, s)
        return 1
    except:
        return 0

funcs = {key[6:]: value for key, value in globals().items() if key.startswith("mysql_")}