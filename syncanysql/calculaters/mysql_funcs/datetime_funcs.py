# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

import re
import time
import datetime
import pytz
from syncany.utils import get_timezone, parse_datetime

TIMEDELTA_UNITS = {"YEAR": 365 * 24 * 60 * 60, "QUARTER": 90 * 24 * 60 * 60, "MONTH": 30 * 24 * 60 * 60,
                   "WEEK": 7 * 24 * 60 * 60, "DAY": 24 * 60 * 60, "HOUR": 3600, "MINUTE": 60, "SECOND": 1,
                   "MICROSECOND": 1}
TIME_INTERVAL_RE = re.compile('^(\d{4}?\-)?(\d+?\-)?(\d+?\s)?(\d+?:)?(\d+?)?(:\d+)?(\.\d+)?$')

def calculate_datetime(dt, interval, is_sub=False):
    def calculate(unit, value):
        if unit == "YEAR":
            return dt.replace(year=dt.year - value) if is_sub else dt.replace(year=dt.year + value)
        if unit in ("QUARTER", "MONTH"):
            months = value * 3 if unit == "QUARTER" else value
            if is_sub:
                new_year = dt.year - int(months / 12) - (1 if dt.month - months % 12 < 1 else 0)
                new_month = 12 + (dt.month - months % 12)
            else:
                new_year = dt.year + int(months / 12) + (1 if dt.month + months % 12 > 12 else 0)
                new_month = (dt.month + months % 12) % 12
            return dt.replace(year=new_year, month=new_month)
        if unit in TIMEDELTA_UNITS:
            td = datetime.timedelta(seconds=TIMEDELTA_UNITS[unit] * value)
            return (dt - td) if is_sub else (dt + td)
        return None
    
    if isinstance(interval, dict):
        dt = calculate(interval["unit"].upper(), int(interval["value"]))
        if dt:
            return dt
        interval = str(interval["value"])
    m = TIME_INTERVAL_RE.match(interval)
    if not m:
        return None
    values, units = list(m.groups()), ("YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND", "MICROSECOND")
    if not values[2] and values[3] and values[4] and not values[5]:
        values[5], values[4], values[3] = values[4], values[3], None
    for i in range(7):
        if not values[i]:
            continue
        dt = calculate(units[i], int(values[i].replace("-", "").replace(".", "").replace(":", "")))
        if not dt:
            return None
    return dt


def mysql_currenttimestamp():
    return datetime.datetime.now(tz=get_timezone())

def mysql_curdate():
    return datetime.date.today()

def mysql_currentdate():
    return datetime.date.today()

def mysql_curtime():
    dt = datetime.datetime.now(tz=get_timezone())
    return datetime.time(dt.hour, dt.minute, dt.second, dt.microsecond)

def mysql_currenttime():
    dt = datetime.datetime.now(tz=get_timezone())
    return datetime.time(dt.hour, dt.minute, dt.second, dt.microsecond)

def mysql_sysdate():
    return datetime.datetime.now(tz=get_timezone())

def mysql_date(dt):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(dt, datetime.datetime):
        return datetime.date(dt.year, dt.month, dt.day)
    if isinstance(dt, datetime.date):
        return dt
    return None

def mysql_datetime(dt):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(dt, datetime.datetime):
        return dt
    if isinstance(dt, datetime.date):
        return datetime.datetime(dt.year, dt.month, dt.day, tzinfo=get_timezone())
    if isinstance(dt, datetime.time):
        now = datetime.datetime.now(tz=get_timezone())
        return datetime.datetime(now.year, now.month, now.day, dt.hour, dt.minute,
                                 dt.second, dt.microsecond, tzinfo=get_timezone())
    return None

def mysql_time(dt):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(dt, datetime.time):
        return dt
    if isinstance(dt, datetime.datetime):
        return datetime.time(dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=get_timezone())
    if isinstance(dt, datetime.date):
        return datetime.time(tzinfo=get_timezone())
    return None

def mysql_unix_timestamp(dt=None):
    if dt is None:
        return int(time.time())
    return int(time.mktime(dt.utctimetuple()))

def mysql_from_unixtime(t):
    if t is None:
        return None
    return datetime.datetime.utcfromtimestamp(t).replace(tzinfo=pytz.UTC).astimezone(tz=get_timezone())

def mysql_month(dt):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.month

def mysql_monthname(dt):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.strftime("%b")

def mysql_dayname(dt):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.strftime("%A")

def mysql_dayofweek(dt):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.weekday() + 1

def mysql_week(dt, mod=None):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    return int(dt.strftime("%U"))

def mysql_yearweek(dt, mod=None):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    return int(dt.strftime("%Y%U"))

def mysql_dayofyear(dt):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    return int(dt.strftime("%j"))

def mysql_dayofmonth(dt):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.day

def mysql_year(dt):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.year

def mysql_time_to_sec(dt):
    if dt is None:
        return None
    st = parse_datetime("2000-01-01 00:00:00", None, get_timezone())
    if isinstance(dt, str):
        dt = parse_datetime("2000-01-01 " + dt, None, get_timezone())
    return int((dt - st).total_seconds())

def mysql_sec_to_time(t):
    if t is None:
        return None
    st = parse_datetime("2000-01-01 00:00:00", None, get_timezone())
    return (st + datetime.timedelta(seconds=t)).strftime("%H:%M:%S")

def mysql_dateadd(dt, i):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(i, (int, float)) or (isinstance(i, str) and i.isdigit()):
        return dt + datetime.timedelta(days=int(i))
    return calculate_datetime(dt, i, False)

def mysql_adddate(dt, i):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(i, (int, float)) or (isinstance(i, str) and i.isdigit()):
        return dt + datetime.timedelta(days=int(i))
    return calculate_datetime(dt, i, False)

def mysql_datesub(dt, i):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(i, (int, float)) or (isinstance(i, str) and i.isdigit()):
        return dt - datetime.timedelta(days=int(i))
    return calculate_datetime(dt, i, True)

def mysql_subdate(dt, i):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(i, (int, float)) or (isinstance(i, str) and i.isdigit()):
        return dt - datetime.timedelta(days=int(i))
    return calculate_datetime(dt, i, True)

def mysql_addtime(dt, i):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(i, (int, float)) or (isinstance(i, str) and i.isdigit()):
        return dt + datetime.timedelta(days=int(i))
    return calculate_datetime(dt, i, False)

def mysql_subtime(dt, i):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(i, (int, float)) or (isinstance(i, str) and i.isdigit()):
        return dt - datetime.timedelta(days=int(i))
    return calculate_datetime(dt, i, True)

def mysql_datediff(dt1, dt2):
    if dt1 is None or dt2 is None:
        return None
    if isinstance(dt1, (int, float, str)):
        dt1 = parse_datetime(str(dt1), None, get_timezone())
    if isinstance(dt2, (int, float, str)):
        dt2 = parse_datetime(str(dt2), None, get_timezone())
    return int((dt2 - dt1).total_seconds() / 86400)

def mysql_date_format(dt, f):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.strftime(f.replace("%v", "%V"))

def mysql_time_format(dt, f):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime("2000-01-01 " + str(dt), None, get_timezone())
    return dt.strftime(f.replace("%v", "%V"))

def mysql_weekday(dt):
    if dt is None:
        return None
    if isinstance(dt, (int, float, str)):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.weekday()

def mysql_utc_date():
    dt = datetime.datetime.utcnow()
    return datetime.date(dt.year, dt.month, dt.day)

def mysql_utc_time():
    dt = datetime.datetime.utcnow()
    return datetime.time(dt.hour, dt.minute, dt.second, dt.microsecond)

def mysql_utc_timestamp():
    dt = datetime.datetime.utcnow()
    return dt.replace(tzinfo=pytz.UTC)


funcs = {key[6:]: value for key, value in globals().items() if key.startswith("mysql_")}
