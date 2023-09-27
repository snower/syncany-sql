# 支持的函数

- [内置函数](#内置函数)
- [支持的MySQL常用函数](#支持的MySQL常用函数)
- [聚合函数](#聚合函数)
- [窗口函数](#窗口函数)
- [YIELD函数](#YIELD函数)
- [transform转换函数](#transform转换函数)
- [时间窗口函数](#时间窗口函数)

## 内置函数

- type(expr)
- is_null(expr)
- is_int(expr)
- is_float(expr)
- is_decimal(expr)
- is_number(expr)
- is_string(expr)
- is_bytes(expr)
- is_bool(expr)
- is_array(expr)
- is_set(expr)
- is_map(expr)
- is_objectid(expr)
- is_uuid(expr)
- is_datetime(expr)
- is_date(expr)
- is_time(expr)
- convert_int(expr)
- convert_float(expr)
- convert_decimal(expr)
- convert_string(expr)
- convert_bytes(expr)
- convert_bool(expr)
- convert_array(expr)
- convert_set(expr)
- convert_map(expr)
- convert_objectid(expr)
- convert_uuid(expr)
- convert_datetime(expr)
- convert_date(expr)
- convert_time(expr)
- range()
- substring()
- split()
- join()
- now()
- current_env_variable()

## 支持的MySQL常用函数

- bitwiseand(x, y)
- bitwiseor(x, y)
- bitwisenot(x)
- bitwisexor(x, y)
- bitwiserightshift(x, y)
- bitwiseleftshift(x, y)
- abs(x)
- sqrt(x)
- exp(x)
- pi()
- ln(x)
- log(x, base)
- ceil(x)
- ceiling(x)
- floor(x)
- rand()
- round(x, y)
- sign(x)
- pow(x, y)
- power(x, y)
- sin(x)
- asin(x)
- cos(x)
- acos(x)
- tan(x)
- atan(x)
- greatest(expr, [expr, expr, ...])
- least(expr, [expr, expr, ...])
- bin(x)
- hex(x)
- unhex(x)
- oct(x)
- ord(x)
- ascii(s)
- char(expr, [expr, expr, ...])
- bit_length(s)
- length(s)
- char_length(s)
- character_length(s)
- concat(expr, [expr, expr, ...])
- concat_ws(sep)
- insert(s1, x, l, s2)
- lower(s)
- upper(s)
- ucase(s)
- left(s, x)
- right(s, x)
- trim(s)
- elt(n)
- field(s)
- find_in_set(s, ss)
- replace(s, s1, s2)
- substring(s, n, l)
- substr(s, n, l)
- substring_index(s, d, c)
- repeat(s, c)
- reverse(s)
- strcmp(s1, s2)
- startswith(s1, s2)
- endswith(s1, s2)
- contains(s1, s2)
- crc32(s)
- from_base64(s)
- to_base64(s)
- inet4_aton(s)
- inet4_ntoa(b)
- is_ipv4(s)
- inet6_aton(s)
- inet6_ntoa(b)
- is_ipv6(s)
- currenttimestamp()
- curdate()
- currentdate()
- curtime()
- currenttime()
- sysdate()
- date(dt)
- datetime(dt)
- time(dt)
- unix_timestamp(dt)
- from_unixtime(t)
- month(dt)
- monthname(dt)
- dayname(dt)
- dayofweek(dt)
- week(dt, mod)
- yearweek(dt, mod)
- dayofyear(dt)
- dayofmonth(dt)
- year(dt)
- time_to_sec(dt)
- sec_to_time(t)
- dateadd(dt, i)
- adddate(dt, i)
- datesub(dt, i)
- subdate(dt, i)
- addtime(dt, i)
- subtime(dt, i)
- datediff(dt1, dt2)
- date_format(dt, f)
- time_format(dt, f)
- weekday(dt)
- utc_date()
- utc_time()
- utc_timestamp()
- json_contains(target, candidate, path)
- json_contains_path(json_doc, one_or_all)
- json_extract(json_doc)
- json_depth(json_doc)
- json_keys(json_doc, path)
- json_length(json_doc, path)
- json_valid(val)

## 聚合函数

- count([distinct] expr)
- sum(expr)
- max(expr)
- min(expr)
- avg(expr)
- group_concat(expr)
- group_array(expr)
- group_uniq_array(expr)
- group_bit_and(expr)
- group_bit_or(expr)
- group_bit_xor(expr)

## 窗口函数

- row_number(expr)
- rank(expr)
- dense_rank(expr)
- percent_rank(expr)
- cume_dist(expr)

## YIELD函数

- yield_array(values)

## transform转换函数

- transform$v4h(key, vkey)
- transform$h4v(key, vkey)
- transform$v2h(key, vkey, [value])
- transform$h2v(key, vkey, [value])
- transform$uniqkv(key, vkey, [value])

## 时间窗口函数

- time_window(time_period, [dt, [offset]])