# -*- coding: utf-8 -*-
# 2025/3/24
# create by: snower

import copy
from syncany.taskers.tasker import _thread_local
from syncany.calculaters.calculater import LoaderCalculater


class ExecuteQueryTaskerCalculater(LoaderCalculater):
    def __init__(self, *args, **kwargs):
        super(ExecuteQueryTaskerCalculater, self).__init__(*args, **kwargs)

        self.config = None
        self.arguments = None
        self.executor = None
        self.tasker = None

    def start(self, tasker, loader, arguments, task_config, **kwargs):
        current_tasker = _thread_local.current_tasker
        try:
            self.config = task_config
            kn, knl = (task_config["name"] + "@"), len(task_config["name"] + "@")
            self.arguments = {}
            for key, value in arguments.items():
                if key[:knl] != kn:
                    continue
                self.arguments[key[knl:]] = value
            self.create_executor_tasker()
        finally:
            _thread_local.current_tasker = current_tasker

    def calculate(self, primary_keys, query, task_config, *args):
        current_tasker = _thread_local.current_tasker
        try:
            if self.executor is None:
                self.create_executor_tasker()
            with self.executor as executor:
                for exp, values in query["filters"].items():
                    if not exp:
                        continue
                    for key, value in values:
                        if not key or not isinstance(key, str):
                            continue
                        executor.env_variables["@" + key] = value
                database, collection_name = self.tasker.tasker.outputer.db, self.tasker.tasker.outputer.name
                executor.execute()
                query = database.query(collection_name, ["id"])
                datas = query.commit()
                delete = database.delete(collection_name, ["id"])
                delete.commit()
                return datas
        finally:
            self.executor, self.tasker = None, None
            _thread_local.current_tasker = current_tasker

    def create_executor_tasker(self):
        from ..executor import Executor
        from ..taskers.query import QueryTasker

        current_executor = Executor.current()
        with Executor(current_executor.manager, current_executor.session_config, current_executor) as executor:
            config = copy.deepcopy(self.config)
            config["output"] = "&.--.__queryTasker_" + str(id(executor)) + "::" + config["output"].split("::")[-1]
            config["name"] = config["name"] + "#queryTasker"
            tasker = QueryTasker(config, is_inner_subquery=True)
            executor.runners.extend(tasker.start(config.get("name"), executor, executor.session_config,
                                                 executor.manager, self.arguments))
            self.tasker, self.executor = tasker, executor