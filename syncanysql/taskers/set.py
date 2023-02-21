# -*- coding: utf-8 -*-
# 2023/2/13
# create by: snower

from ..config import CONST_CONFIG_KEYS


class SetCommandTasker(object):
    def __init__(self, config):
        self.config = config

    def start(self, executor, session_config, manager, arguments):
        key, is_global = self.config["key"], False
        if key[:6].lower() == "global":
            key, is_global = key[6:].strip(), True
        keys = key.split(".")[0]
        seted_config = session_config.global_config if is_global else session_config
        if key in CONST_CONFIG_KEYS:
            self.set_config(seted_config, key)
        elif keys in ("databases", "imports", "sources", "defines", "variables", "options", "caches"):
            self.set_config(seted_config, key)
        elif keys == "virtual_views":
            self.set_config(seted_config, "databases." + key)
        elif key[:7] == "@config":
            self.set_config(seted_config, key[8:].strip())
        if is_global:
            session_config.merge()
        return []

    def set_config(self, session_config, key):
        if self.config["value"][:1] in ('"', "'"):
            if self.config["value"][:3] in ("'''", '"""'):
                value = self.config["value"][3:-3].strip()
            else:
                value = self.config["value"][1:-1].strip()
        else:
            if self.config["value"].lower() == 'true':
                value = True
            elif self.config["value"].lower() == 'false':
                value = False
            elif self.config["value"].lower() == 'null':
                value = None
            else:
                digits = self.config["value"].split(".")
                if digits and len(digits) <= 2 and digits[0].isdigit():
                    value = float(self.config["value"]) if len(digits) == 2 and digits[1].isdigit() else int(self.config["value"])
                else:
                    value = self.config["value"].strip()
        session_config.set(key, value)

    def run(self, executor, session_config, manager):
        return 0

    def terminate(self):
        pass