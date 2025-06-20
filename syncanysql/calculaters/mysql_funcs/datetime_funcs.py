# -*- coding: utf-8 -*-
# 2023/3/2
# create by: snower

import re
import time
from datetime import datetime as datetime_datetime, date as datetime_date, time as datetime_time, timedelta as datetime_timedelta
import pytz
from syncany.utils import get_timezone, parse_datetime
from syncany.calculaters import typing_filter
from ...utils import NumberTypes, NumberStringTypes

TIMEDELTA_UNITS = {"YEAR": 365 * 24 * 60 * 60, "QUARTER": 90 * 24 * 60 * 60, "MONTH": 30 * 24 * 60 * 60,
                   "WEEK": 7 * 24 * 60 * 60, "DAY": 24 * 60 * 60, "HOUR": 3600, "MINUTE": 60, "SECOND": 1,
                   "MICROSECOND": 1}
TIME_INTERVAL_RE = re.compile(r'^(\d{4}?\-)?(\d+?\-)?(\d+?\s)?(\d+?:)?(\d+?)?(:\d+)?(\.\d+)?$')

def calculate_datetime(dt, interval, is_sub=False):
    def calculate(unit, value):
        if unit == "YEAR":
            return dt.replace(year=dt.year - value) if is_sub else dt.replace(year=dt.year + value)
        if unit in ("QUARTER", "MONTH"):
            months = value * 3 if unit == "QUARTER" else value
            if is_sub:
                new_year = dt.year - int(months / 12) - (1 if dt.month - months % 12 < 1 else 0)
                new_month = (dt.month - months % 12) + (12 if dt.month - months % 12 < 1 else 0)
            else:
                new_year = dt.year + int(months / 12) + (1 if dt.month + months % 12 > 12 else 0)
                new_month = (dt.month + months % 12) % 12
            try:
                return dt.replace(year=new_year, month=new_month)
            except ValueError:
                day = dt.day - 1
                while day >= 1:
                    try:
                        return dt.replace(year=new_year, month=new_month, day=day)
                    except ValueError:
                        day -= 1
                return None
        if unit in TIMEDELTA_UNITS:
            td = datetime_timedelta(seconds=TIMEDELTA_UNITS[unit] * value)
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


@typing_filter(datetime_datetime)
def mysql_currenttimestamp():
    return datetime_datetime.now(tz=get_timezone())

@typing_filter(datetime_date)
def mysql_curdate():
    return datetime_date.today()

@typing_filter(datetime_date)
def mysql_currentdate():
    return datetime_date.today()

@typing_filter(datetime_time)
def mysql_curtime():
    dt = datetime_datetime.now(tz=get_timezone())
    return datetime_time(dt.hour, dt.minute, dt.second, dt.microsecond)

@typing_filter(datetime_time)
def mysql_currenttime():
    dt = datetime_datetime.now(tz=get_timezone())
    return datetime_time(dt.hour, dt.minute, dt.second, dt.microsecond)

@typing_filter(datetime_datetime)
def mysql_sysdate():
    return datetime_datetime.now(tz=get_timezone())

@typing_filter(datetime_date)
def mysql_date(dt):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(dt, datetime_datetime):
        return datetime_date(dt.year, dt.month, dt.day)
    if isinstance(dt, datetime_date):
        return dt
    return None

@typing_filter(datetime_datetime)
def mysql_datetime(dt):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(dt, datetime_datetime):
        return dt
    if isinstance(dt, datetime_date):
        return datetime_datetime(dt.year, dt.month, dt.day, tzinfo=get_timezone())
    if isinstance(dt, datetime_time):
        now = datetime_datetime.now(tz=get_timezone())
        return datetime_datetime(now.year, now.month, now.day, dt.hour, dt.minute,
                                 dt.second, dt.microsecond, tzinfo=get_timezone())
    return None

@typing_filter(datetime_time)
def mysql_time(dt):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(dt, datetime_time):
        return dt
    if isinstance(dt, datetime_datetime):
        return datetime_time(dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=get_timezone())
    if isinstance(dt, datetime_date):
        return datetime_time(tzinfo=get_timezone())
    return None

@typing_filter(int)
def mysql_unix_timestamp(dt=None):
    if dt is None:
        return int(time.time())
    return int(time.mktime(dt.utctimetuple()))

@typing_filter(datetime_datetime)
def mysql_from_unixtime(t):
    if t is None:
        return None
    return datetime_datetime.fromtimestamp(int(t), pytz.UTC).astimezone(tz=get_timezone())

@typing_filter(int)
def mysql_month(dt):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.month

@typing_filter(str)
def mysql_monthname(dt):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.strftime("%b")

@typing_filter(str)
def mysql_dayname(dt):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.strftime("%A")

@typing_filter(int)
def mysql_dayofweek(dt):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.weekday() + 1

@typing_filter(int)
def mysql_week(dt, mod=None):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    return int(dt.strftime("%W" if str(mod) == "1" else "%U"))

@typing_filter(int)
def mysql_yearweek(dt, mod=None):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    return int(dt.strftime("%Y%W" if str(mod) == "1" else "%Y%U"))

@typing_filter(int)
def mysql_dayofyear(dt):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    return int(dt.strftime("%j"))

@typing_filter(int)
def mysql_dayofmonth(dt):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.day

@typing_filter(int)
def mysql_year(dt):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.year

@typing_filter(int)
def mysql_time_to_sec(dt):
    if dt is None:
        return None
    st = parse_datetime("2000-01-01 00:00:00", None, get_timezone())
    if isinstance(dt, str):
        dt = parse_datetime("2000-01-01 " + dt, None, get_timezone())
    return int((dt - st).total_seconds())

@typing_filter(str)
def mysql_sec_to_time(t):
    if t is None:
        return None
    st = parse_datetime("2000-01-01 00:00:00", None, get_timezone())
    return (st + datetime_timedelta(seconds=t)).strftime("%H:%M:%S")

@typing_filter(datetime_datetime)
def mysql_dateadd(dt, i):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(i, NumberTypes) or (isinstance(i, str) and i.isdigit()):
        return dt + datetime_timedelta(days=int(i))
    return calculate_datetime(dt, i, False)

@typing_filter(datetime_datetime)
def mysql_adddate(dt, i):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(i, NumberTypes) or (isinstance(i, str) and i.isdigit()):
        return dt + datetime_timedelta(days=int(i))
    return calculate_datetime(dt, i, False)

@typing_filter(datetime_datetime)
def mysql_datesub(dt, i):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(i, NumberTypes) or (isinstance(i, str) and i.isdigit()):
        return dt - datetime_timedelta(days=int(i))
    return calculate_datetime(dt, i, True)

@typing_filter(datetime_datetime)
def mysql_subdate(dt, i):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(i, NumberTypes) or (isinstance(i, str) and i.isdigit()):
        return dt - datetime_timedelta(days=int(i))
    return calculate_datetime(dt, i, True)

@typing_filter(datetime_datetime)
def mysql_addtime(dt, i):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(i, NumberTypes) or (isinstance(i, str) and i.isdigit()):
        return dt + datetime_timedelta(days=int(i))
    return calculate_datetime(dt, i, False)

@typing_filter(datetime_datetime)
def mysql_subtime(dt, i):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    if isinstance(i, NumberTypes) or (isinstance(i, str) and i.isdigit()):
        return dt - datetime_timedelta(days=int(i))
    return calculate_datetime(dt, i, True)

@typing_filter(int)
def mysql_datediff(dt1, dt2):
    if dt1 is None or dt2 is None:
        return None
    if isinstance(dt1, NumberStringTypes):
        dt1 = parse_datetime(str(dt1), None, get_timezone())
    if isinstance(dt2, NumberStringTypes):
        dt2 = parse_datetime(str(dt2), None, get_timezone())
    return int((dt2 - dt1).total_seconds() / 86400)

@typing_filter(int)
def mysql_timestampdiff(unit, dt1, dt2):
    if dt1 is None or dt2 is None:
        return None
    if isinstance(dt1, NumberStringTypes):
        dt1 = parse_datetime(str(dt1), None, get_timezone())
    if isinstance(dt2, NumberStringTypes):
        dt2 = parse_datetime(str(dt2), None, get_timezone())
    return int((dt2 - dt1).total_seconds() / TIMEDELTA_UNITS[unit])

@typing_filter(str)
def mysql_date_format(dt, f):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.strftime(f.replace("%v", "%V"))

@typing_filter(str)
def mysql_time_format(dt, f):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime("2000-01-01 " + str(dt), None, get_timezone())
    return dt.strftime(f.replace("%v", "%V"))

@typing_filter(int)
def mysql_weekday(dt):
    if dt is None:
        return None
    if isinstance(dt, NumberStringTypes):
        dt = parse_datetime(str(dt), None, get_timezone())
    return dt.weekday()

@typing_filter(datetime_date)
def mysql_utc_date():
    dt = datetime_datetime.now(pytz.UTC)
    return datetime_date(dt.year, dt.month, dt.day)

@typing_filter(datetime_time)
def mysql_utc_time():
    dt = datetime_datetime.now(pytz.UTC)
    return datetime_time(dt.hour, dt.minute, dt.second, dt.microsecond)

@typing_filter(datetime_datetime)
def mysql_utc_timestamp():
    return datetime_datetime.now(pytz.UTC)


funcs = {key[6:]: value for key, value in globals().items() if key.startswith("mysql_")}
