# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

import time
import datetime
import pytz
from syncany.utils import get_timezone, parse_datetime


def mysql_curdate():
    return datetime.date.today()

def mysql_current_date():
    return datetime.date.today()

def mysql_curtime():
    dt = datetime.datetime.now(tz=get_timezone())
    return datetime.time(dt.hour, dt.minute, dt.second, dt.microsecond)

def mysql_current_time():
    dt = datetime.datetime.now(tz=get_timezone())
    return datetime.time(dt.hour, dt.minute, dt.second, dt.microsecond)

def mysql_sysdate():
    return datetime.datetime.now(tz=get_timezone())

def mysql_unix_timestamp(dt=None):
    if dt is None:
        return int(time.time())
    return int(time.mktime(dt.utctimetuple()))

def mysql_from_unixtime(t):
    return datetime.datetime.utcfromtimestamp(t).replace(tzinfo=pytz.UTC).astimezone(tz=get_timezone())

def mysql_month(dt):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt.month

def mysql_monthname(dt):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt.strftime("%b")

def mysql_dayname(dt):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt.strftime("%A")

def mysql_dayofweek(dt):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt.weekday() + 1

def mysql_week(dt, mod=None):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return int(dt.strftime("%U"))

def mysql_dayofyear(dt):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return int(dt.strftime("%j"))

def mysql_dayofmonth(dt):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt.day

def mysql_year(dt):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt.year

def mysql_time_to_sec(dt):
    st = parse_datetime("2000-01-01 00:00:00", None, get_timezone())
    if isinstance(dt, str):
        dt = parse_datetime("2000-01-01 " + dt, None, get_timezone())
    return (dt - st).total_seconds()

def mysql_sec_to_time(t):
    st = parse_datetime("2000-01-01 00:00:00", None, get_timezone())
    return (st + datetime.timedelta(seconds=t)).strftime("%H:%M:%S")

def mysql_date_add(dt, t):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt + datetime.timedelta(seconds=t)

def mysql_adddate(dt, t):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt + datetime.timedelta(seconds=t)

def mysql_date_sub(dt, t):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt - datetime.timedelta(seconds=t)

def mysql_subdate(dt, t):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt - datetime.timedelta(seconds=t)

def mysql_addtime(dt, t):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt + datetime.timedelta(seconds=t)

def mysql_subtime(dt, t):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt - datetime.timedelta(seconds=t)

def mysql_datediff(dt1, dt2):
    if isinstance(dt1, str):
        dt1 = parse_datetime(dt1, None, get_timezone())
    if isinstance(dt2, str):
        dt2 = parse_datetime(dt2, None, get_timezone())
    return int((dt2 - dt1).total_seconds() / 86400)

def mysql_date_format(dt, f):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt.strftime(f)

def mysql_weekday(dt):
    if isinstance(dt, str):
        dt = parse_datetime(dt, None, get_timezone())
    return dt.weekday()

funcs = {key[6:]: value for key, value in globals().items() if key.startswith("mysql_")}