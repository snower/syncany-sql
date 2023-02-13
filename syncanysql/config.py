# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os

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
                    set_virtual_view[key[4]] = value
                else:
                    set_virtual_view["query"] = value

    def load(self):
        home_config_path = os.path.join(os.path.expanduser('~'), ".syncany")
        for filename in (os.path.join(home_config_path, "config.json"), os.path.join(home_config_path, "config.yaml"),
                         "config.json", "config.yaml"):
            if not os.path.exists(filename):
                continue
            if "extends" not in self.config or not isinstance(self.config["extends"]):
                self.config["extends"] = []
            if filename not in self.config["extends"]:
                self.config["extends"].append(filename)