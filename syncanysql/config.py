# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os
import re
import json

VIRTUAL_VIEW_ARGS_RE = re.compile("(\#\{\w+?(:.*?){0,1}\})", re.DOTALL | re.M)
CONST_CONFIG_KEYS = ("@verbose", "@limit", "@batch", "@recovery", "@join_batch", "@insert_batch")

class SessionConfig(object):
    def __init__(self):
        self.config = {
            "databases": [
                {
                    "name": "-",
                    "driver": "textline"
                },
                {
                    "name": "--",
                    "driver": "memory"
                }
            ]
        }

    def get(self):
        return self.config

    def set(self, key, value):
        if not isinstance(key, (list, tuple)):
            key = str(key).split(".")
        if not key:
            return
        if len(key) == 1:
            self.config[key[0]] = value
            return
        if key[0] == "databases":
            set_database = None
            for database in self.config["databases"]:
                if database["name"] == key[1]:
                    set_database = database
                    break
            if not set_database:
                set_database = {"name": key[1]}
                self.config["databases"].append(set_database)
            if key[2] == "virtual_views":
                if "virtual_views" not in set_database:
                    set_database["virtual_views"] = []
                set_virtual_view = None
                for virtual_view in set_database["virtual_views"]:
                    if virtual_view["name"] == key[3]:
                        set_virtual_view = virtual_view
                        break
                if not set_virtual_view:
                    set_virtual_view = {"name": key[3], "query": "", "args": []}
                    set_database["virtual_views"].append(set_virtual_view)
                if len(key) > 4:
                    try:
                        set_virtual_view[key[4]] = json.loads(value)
                    except:
                        set_virtual_view[key[4]] = value
                else:
                    set_virtual_view["query"], set_virtual_view["args"] = self.parse_virtual_view_args(value)
        elif key[0] in ("imports", "sources"):
            if key[0] not in self.config:
                self.config[key[0]][key[1]] = {}
            self.config[key[0]] = json.loads(value)
        elif key[0] in ("defines", "variables", "options", "caches"):
            if key[0] not in self.config:
                self.config[key[0]] = {}
            try:
                self.config[key[0]][key[1]] = json.loads(value)
            except:
                self.config[key[0]][key[1]] = value

    def parse_virtual_view_args(self, query):
        args = []
        exps = {"eq": "==", "neq": "!=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "in": "in"}
        for arg, default_value in VIRTUAL_VIEW_ARGS_RE.findall(query):
            arg_info = arg[2:-1].split(":")[0].split("__")
            try:
                default_value = json.loads(default_value[1:])
            except:
                default_value = default_value[1:]
            args.append([arg_info[0], exps[arg_info[1]] if len(arg_info) >= 2 else "==", default_value])
            query = query.replace(arg, '%s')
        return query, args

    def load(self):
        home_config_path = os.path.join(os.path.expanduser('~'), ".syncany")
        for filename in (os.path.join(home_config_path, "config.json"), os.path.join(home_config_path, "config.yaml"),
                         "config.json", "config.yaml"):
            if not os.path.exists(filename):
                continue
            if "extends" not in self.config or not isinstance(self.config["extends"], list):
                self.config["extends"] = []
            if filename not in self.config["extends"]:
                self.config["extends"].append(filename)