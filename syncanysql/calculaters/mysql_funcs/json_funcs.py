# -*- coding: utf-8 -*-
# 2023/3/14
# create by: snower

import json


def parse_json_path(json_path):
    if not json_path:
        return []
    try:
        dot_index = json_path.index(".")
    except ValueError:
        dot_index = -1

    json_path_keys = []
    try:
        start_index = json_path.index("[")
        if 0 < dot_index < start_index:
            json_path_keys.append(json_path[:dot_index])
            next_json_path = json_path[dot_index + 1:].strip(".")
            if next_json_path:
                json_path_keys.extend(parse_json_path(next_json_path))
            return json_path_keys

        end_index = json_path.index("]")
        if start_index > 0:
            json_path_keys.append(json_path[:start_index])
        if end_index - start_index > 0:
            json_path_key = json_path[start_index + 1: end_index]
            if json_path_key.isdigit():
                json_path_key = int(json_path_key)
            elif json_path_key and json_path_key[0] == '"' and json_path_key[-1] == '"':
                json_path_key = json_path_key[1:-1]
            json_path_keys.append(json_path_key)

        next_json_path = json_path[end_index + 1:].strip(".")
        if next_json_path:
            json_path_keys.extend(parse_json_path(next_json_path))
        return json_path_keys
    except ValueError:
        if dot_index > 0:
            json_path_keys.append(json_path[:dot_index])
            next_json_path = json_path[dot_index + 1:].strip(".")
            if next_json_path:
                json_path_keys.extend(parse_json_path(next_json_path))
        else:
            json_path_keys.append(json_path)
        return json_path_keys


def get_json_path_value(json_data, json_path):
    if json_data is None:
        return None
    if json_path and json_path[:2] == "$.":
        json_path = json_path[2:]
    elif json_path and json_path[0] == '$':
        json_path = json_path[1:]

    def get_value(json_data, json_path_keys):
        for i in range(len(json_path_keys)):
            json_path_key = json_path_keys[i]
            if isinstance(json_path_key, int):
                if isinstance(json_data, list):
                    if json_path_key >= len(json_data):
                        return None
                    json_data = json_data[json_path_key]
                    continue
            if json_path_key == "*":
                if i + 1 >= len(json_path_keys):
                    return json_data
                if not isinstance(json_data, list):
                    json_data = [json_data]
                result = []
                for d in json_data:
                    v = get_value(d, json_path_keys[i + 1:])
                    if v is None:
                        continue
                    result.append(v)
                return result
            if not isinstance(json_data, dict):
                return None
            if json_path_key not in json_data:
                return None
            json_data = json_data[json_path_key]
        return json_data
    return get_value(json_data, parse_json_path(json_path))


def mysql_json_contains(target, candidate, path):
    if isinstance(target, str):
        try:
            target = json.loads(target)
        except:
            pass
    if isinstance(candidate, str):
        try:
            candidate = json.loads(candidate)
        except:
            pass
    target_value = get_json_path_value(target, path)
    return 1 if target_value == candidate else 0


def mysql_json_contains_path(json_doc, one_or_all, *paths):
    if isinstance(json_doc, str):
        try:
            json_doc = json.loads(json_doc)
        except:
            pass
    if not one_or_all:
        one_or_all = "one"
    for path in paths:
        json_value = get_json_path_value(json_doc, path)
        if one_or_all.lower() == "one":
            if json_value is not None:
                return 1
        elif json_value is None:
            return 0
    return 0 if one_or_all.lower() == "one" else 1


def mysql_json_extract(json_doc, *paths):
    if isinstance(json_doc, str):
        try:
            json_doc = json.loads(json_doc)
        except:
            pass
    results = []
    for path in paths:
        results.append(get_json_path_value(json_doc, path))
    return results[0] if len(paths) == 1 else results


def mysql_json_depth(json_doc):
    if isinstance(json_doc, str):
        try:
            json_doc = json.loads(json_doc)
        except:
            pass
    def get_depth(json_doc):
        if not json_doc:
            return 1
        if isinstance(json_doc, list):
            return max((get_depth(json_value) for json_value in json_doc)) + 1
        if isinstance(json_doc, dict):
            return max((get_depth(json_value) for json_value in json_doc.values())) + 1
        return 1
    return get_depth(json_doc)


def mysql_json_keys(json_doc, path=None):
    if isinstance(json_doc, str):
        try:
            json_doc = json.loads(json_doc)
        except:
            pass
    if path:
        json_doc = get_json_path_value(json_doc, path)
    if not isinstance(json_doc, dict):
        return []
    return list(json_doc.keys())


def mysql_json_length(json_doc, path=None):
    if isinstance(json_doc, str):
        try:
            json_doc = json.loads(json_doc)
        except:
            pass
    if path:
        json_doc = get_json_path_value(json_doc, path)
    if not isinstance(json_doc, (list, dict)):
        return 0
    return len(json_doc)


def mysql_json_valid(val):
    if isinstance(val, str):
        try:
            json.loads(val)
            return 1
        except:
            return 0
    return 1


funcs = {key[6:]: value for key, value in globals().items() if key.startswith("mysql_")}
