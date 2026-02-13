# -*- coding: utf-8 -*-
# 2025/4/3
# create by: snower

import traceback
from syncany.logger import get_logger
from syncany.calculaters.calculater import Calculater


class PyEvalCalculater(Calculater):
    globals = None

    @classmethod
    def init_globals(cls):
        import sys
        import os
        import datetime
        import time
        import math
        import random
        import string
        import uuid
        import base64
        import hashlib
        import pickle
        import json
        import re
        try:
            import requests
        except:
            requests = None
        from syncany.taskers.tasker import current_tasker
        from ..executor import Executor

        cls.globals = {
            "sys": sys,
            "os": os,
            "datetime": datetime,
            "time": time,
            "math": math,
            "random": random,
            "string": string,
            "uuid": uuid,
            "base64": base64,
            "hashlib": hashlib,
            "pickle": pickle,
            "json": json,
            "re": re,
            "current_tasker": lambda : current_tasker(),
            "current_executor": lambda : Executor.current(),
            "current_manager": lambda: Executor.current().manager,
            "current_session": lambda: Executor.current().session_config,
            "current_env_variables": lambda: Executor.current().env_variables,
        }
        if requests is not None:
            cls.globals["requests"] = requests

    def __init__(self, *args, **kwargs):
        Calculater.__init__(self, *args, **kwargs)

        if self.globals is None:
            self.init_globals()
        if self.name == "pyevalt":
            self.calculate = self.calculate_eval_this

    def calculate(self, *args):
        if not args or not args[0]:
            return None
        try:
            return eval(args[0], self.globals, {"args": args[1:]})
        except Exception as e:
            get_logger().warning("pyeval calculater execute %s error: %s\n%s", args, e, traceback.format_exc())
            return None

    def calculate_eval_this(self, *args):
        if not args or len(args) < 2 or not args[0] or not args[1]:
            return None
        try:
            return eval(args[1], self.globals, {"this": args[0], "args": args[2:]})
        except Exception as e:
            get_logger().warning("pyeval calculater execute %s error: %s\n%s", args, e, traceback.format_exc())
            return None


def register_pyeval_module(name, module):
    if PyEvalCalculater.globals is None:
        PyEvalCalculater.init_globals()
    PyEvalCalculater.globals[name] = module
eval('[{"v": i["v"] * args[0]} for i in this]', {}, {"this": [{"v": 0}, {"v": 1}], "args": (2,)})