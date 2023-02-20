# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os
import copy
import re
import logging.config
import json
import pytz
from syncany.taskers.core import CoreTasker
from syncany.taskers.config import load_config
from syncany.utils import set_timezone


VIRTUAL_VIEW_ARGS_RE = re.compile("(\#\{\w+?(:.*?){0,1}\})", re.DOTALL | re.M)
CONST_CONFIG_KEYS = ("@verbose", "@limit", "@batch", "@streaming", "@recovery", "@join_batch", "@insert_batch")


class SessionConfig(object):
    def __init__(self):
        self.config = {}

    def get(self):
        return self.config

    def set(self, key, value):
        if not isinstance(key, (list, tuple)):
            key = str(key).split(".")
        if not key:
            return
        if len(key) == 1:
            if value is None:
                self.config.pop(key[0], None)
            else:
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
            if len(key) <= 2:
                if value is None:
                    self.config["databases"].remove(set_database)
                else:
                    try:
                        set_database.clear()
                        set_database["name"] = key[1]
                        set_database.update(json.loads(value))
                    except:
                        pass
            elif key[2] == "virtual_views":
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
                    if value is None:
                        set_virtual_view.pop(key[4], None)
                    else:
                        try:
                            set_virtual_view[key[4]] = json.loads(value)
                        except:
                            set_virtual_view[key[4]] = value
                else:
                    if value is None:
                        set_database["virtual_views"].remove(set_virtual_view)
                    else:
                        set_virtual_view["query"], set_virtual_view["args"] = self.parse_virtual_view_args(value)
            else:
                if value is None:
                    set_database.pop(key[2], None)
                else:
                    try:
                        set_database[key[2]] = json.loads(value)
                    except:
                        pass
        elif key[0] in ("imports", "sources"):
            if key[0] not in self.config:
                self.config[key[0]] = {}
            if value is None:
                self.config[key[0]].pop(key[1], None)
            else:
                self.config[key[0]][key[1]] = json.loads(value)
        elif key[0] in ("defines", "variables", "options", "caches"):
            if key[0] not in self.config:
                self.config[key[0]] = {}
            if value is None:
                self.config[key[0]].pop(key[1], None)
            else:
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
        self.load_config()

        if "timezone" in self.config:
            timezone = self.config.pop("timezone")
            set_timezone(pytz.timezone(timezone))
        if "databases" not in self.config:
            self.config["databases"] = []
        database_names = {database["name"] for database in self.config["databases"]}
        if "-" not in database_names:
            self.config["databases"].append({"name": "-", "driver": "textline"})
        if "--" not in database_names:
            self.config["databases"].append({"name": "--", "driver": "memory"})

    def load_config(self, filename=None):
        if filename is None:
            config, self.config = self.config, copy.deepcopy(CoreTasker.DEFAULT_CONFIG)
            for k, v in CoreTasker.DEFAULT_CONFIG.items():
                if k in config and not config[k]:
                    config.pop(k)
        else:
            config = load_config(filename)

        extends = config.pop("extends") if "extends" in config else []
        if isinstance(extends, list):
            for config_filename in extends:
                self.load_config(config_filename)
        else:
            self.load_config(extends)

        for k, v in config.items():
            if k in ("arguments", "imports", "defines", "variables", "sources", "logger", "options"):
                if not isinstance(v, dict) or not isinstance(self.config.get(k, {}), dict):
                    continue

                if k not in self.config:
                    self.config[k] = v
                else:
                    self.config[k].update(v)
            elif k in ("databases", "caches"):
                if not isinstance(v, list) or not isinstance(self.config.get(k, []), list):
                    continue

                if k not in self.config:
                    self.config[k] = v
                else:
                    vs = {c["name"]: c for c in self.config[k]}
                    for d in v:
                        if d["name"] in vs:
                            vs[d["name"]].update(d)
                        else:
                            self.config[k].append(d)
            elif k == "pipelines":
                if self.config[k]:
                    pipelines = copy.copy(self.config[k] if isinstance(self.config[k], list)
                                            and not isinstance(self.config[k][0], str) else [self.config[k]])
                else:
                    pipelines = []
                if v:
                    for pipeline in (v if isinstance(v, list) and not isinstance(v[0], str) else [v]):
                        pipelines.append(pipeline)
                self.config[k] = pipelines
            elif k == "states":
                if self.config[k]:
                    self.config[k].extend(v if isinstance(v, list) else [v])
                else:
                    self.config[k] = v if isinstance(v, list) else [v]
            else:
                self.config[k] = v

    def config_logging(self):
        logfile = self.config.pop("logfile", None)
        logformat = self.config.pop("logformat", "%(asctime)s %(process)d %(levelname)s %(message)s")
        loglevel = {"CRITICAL": logging.CRITICAL, "FATAL": logging.FATAL, "ERROR": logging.ERROR,
                    "WARN": logging.CRITICAL, "WARNING": logging.WARNING, "INFO": logging.INFO,
                    "DEBUG": logging.DEBUG}.get(self.config.pop("loglevel", "INFO"))
        if "logger" in self.config and isinstance(self.config["logger"], dict):
            logging.config.dictConfig(self.config["logger"])
        else:
            logging.basicConfig(filename=logfile, level=loglevel, format=logformat,
                                datefmt='%Y-%m-%d %H:%M:%S', filemode='a+')
