# -*- coding: utf-8 -*-
# 2023/5/26
# create by: snower

from syncany.calculaters.calculater import Calculater


class CurrentEnvVariableCalculater(Calculater):
    def __init__(self, *args):
        super(CurrentEnvVariableCalculater, self).__init__(*args)

        if self.name[22:] == "get_value":
            self.func = self.get_value
        elif self.name[22:] == "set_value":
            self.func = self.set_value
        else:
            self.func = lambda *args: None

    def calculate(self, *args):
        return self.func(*args)

    def get_value(self, key):
        from ..executor import Executor
        current_executor = Executor.current()
        if not current_executor:
            return None
        try:
            return current_executor.env_variables.get_value(key)
        except KeyError:
            return None

    def set_value(self, key, value):
        from ..executor import Executor
        current_executor = Executor.current()
        if current_executor:
            current_executor.env_variables[key] = value
        return value
