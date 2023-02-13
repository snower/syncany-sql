# -*- coding: utf-8 -*-
# 2023/2/13
# create by: snower

import json

class SetCommandTasker(object):
    def __init__(self, config):
        self.config = config

    def run(self, session_config, manager, arguments):
        if self.config["key"][:7] == "@config":
            self.set_config(session_config)
        return 0

    def set_config(self, session_config):
        key = self.config["key"][8:].strip()
        if self.config["value"][:1] in ('"', "'"):
            value = self.config["value"][3:-3].strip() if self.config["value"][:3] in ("'''", '"""') else self.config["value"][1:-1].strip()
            try:
                value = json.loads(value)
            except: pass
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


