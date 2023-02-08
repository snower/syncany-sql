# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os

class Config(object):
    def __init__(self):
        self.config = {
            "databases": [
                {
                    "name": "-",
                    "driver": "textline",
                    "format": "print"
                },
                {
                    "name": "=",
                    "driver": "memory"
                }
            ]
        }

    def get(self):
        return self.config

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