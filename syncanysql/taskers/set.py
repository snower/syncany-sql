# -*- coding: utf-8 -*-
# 2023/2/13
# create by: snower

class SetCommandTasker(object):
    def __init__(self, config):
        self.config = config

    def start(self, name, executor, session_config, manager, arguments):
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
            try:
                self.set_env_variable(executor, key, self.parse_value(self.config["value"].strip()), is_global)
            except ValueError as e:
                if key[:1] == "@":
                    try:
                        executor.compile(name, "select " + self.config["value"].strip() + " into " + key)
                        return []
                    except Exception as e:
                        raise ValueError("unknown value: %s" % self.config["value"].strip())
                raise e
        return []

    def set_config(self, session_config, key, is_global=False):
        if is_global:
            session_config.global_config.set(key, self.parse_value(self.config["value"].strip()))
            session_config.merge()
            return
        session_config.set(key, self.parse_value(self.config["value"].strip()))

    def set_env_variable(self, executor, key, value, is_global=False):
        if is_global:
            executor.global_env_variables[key] = value
            return
        executor.env_variables[key] = value

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
        if value.isdigit() or (value[0] == "-" and value[1:].isdigit()):
            return int(value)
        value_info = value.split(".")
        if len(value_info) == 2 and (value_info[0].isdigit() or (value[0][0] == "-" and value[0][1:].isdigit())) \
                and value_info[1].isdigit():
            return float(value)
        raise ValueError("unknown value: %s" % value)

    def run(self, executor, session_config, manager):
        return 0

    def terminate(self):
        pass