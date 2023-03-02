# -*- coding: utf-8 -*-
# 2023/2/13
# create by: snower

class SetCommandTasker(object):
    def __init__(self, config):
        self.config = config

    def start(self, executor, session_config, manager, arguments):
        key, is_global = self.config["key"], False
        if key[:6].lower() == "global":
            key, is_global = key[6:].strip(), True
        name = key.split(".")[0][1:]
        if name in ("databases", "imports", "sources", "defines", "variables", "options", "caches"):
            self.set_config(session_config, key[1:], is_global)
        elif name == "virtual_views":
            self.set_config(session_config, "databases." + key[1:], is_global)
        elif key[:7] == "@config":
            self.set_config(session_config, key[8:].strip(), is_global)
        else:
            self.set_env_variable(executor, key, is_global)
        return []

    def set_config(self, session_config, key, is_global=False):
        if is_global:
            session_config.global_config.set(key, self.parse_value(self.config["value"].strip()))
            session_config.merge()
            return
        session_config.set(key, self.parse_value(self.config["value"].strip()))

    def set_env_variable(self, executor, key, is_global=False):
        if is_global:
            executor.global_env_variables[key] = self.parse_value(self.config["value"].strip())
            return
        executor.env_variables[key] = self.parse_value(self.config["value"].strip())

    def parse_value(self, value):
        if not isinstance(value, str):
            return value
        if value[:1] in ('"', "'"):
            if value[:3] in ("'''", '"""'):
                return value[3:-3].strip()
            return value[1:-1].strip()
        if value.lower() == 'true':
            return True
        if value.lower() == 'false':
            return False
        if value.lower() == 'null':
            return None
        digits = value.split(".")
        if digits and len(digits) <= 2 and digits[0].isdigit():
            return float(value) if len(digits) == 2 and digits[1].isdigit() else int(value)
        return value

    def run(self, executor, session_config, manager):
        return 0

    def terminate(self):
        pass