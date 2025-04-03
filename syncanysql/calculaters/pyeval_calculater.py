# -*- coding: utf-8 -*-
# 2025/4/3
# create by: snower

import traceback
from syncany.logger import get_logger
from syncany.calculaters.calculater import Calculater


class PyEvalCalculater(Calculater):
    globals = None

    def __init__(self, *args, **kwargs):
        super(PyEvalCalculater, self).__init__(*args, **kwargs)

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

        self.__class__.globals = {
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
        }

    def calculate(self, *args):
        if not args:
            return None
        try:
            return eval(args[0], self.globals, {"args": args[1:]})
        except Exception as e:
            get_logger().warning("pyeval calculater execute %s error: %s\n%s", args, e, traceback.format_exc())
            return None


def register_pyeval_module(name, module):
    if PyEvalCalculater.globals is None:
        PyEvalCalculater.globals = {}
    PyEvalCalculater.globals[name] = module
