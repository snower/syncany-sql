# -*- coding: utf-8 -*-
# 2025/3/24
# create by: snower

import copy
from syncany.taskers.tasker import _thread_local
from syncany.calculaters.calculater import Calculater


class ExecuteQueryTaskerCalculater(Calculater):
    def calculate(self, primary_keys, query, task_config):
        from ..executor import Executor
        from ..taskers.query import QueryTasker

        current_tasker = _thread_local.current_tasker
        current_executor = Executor.current()
        try:
            with Executor(current_executor.manager, current_executor.session_config, current_executor) as executor:
                task_config = copy.deepcopy(task_config)
                for exp, values in query["filters"].items():
                    if not exp:
                        continue
                    for key, value in values:
                        if not key or not isinstance(key, str):
                            continue
                        executor.env_variables["@" + key] = value
                collection_name = "--.__queryTasker_" + str(id(executor))
                task_config["output"] = "&." + collection_name + "::" + task_config["output"].split("::")[-1]
                task_config["name"] = task_config["name"] + "#queryTasker"
                tasker = QueryTasker(task_config, is_inner_subquery=True)
                executor.runners.extend(tasker.start(task_config.get("name"), executor, executor.session_config,
                                                     executor.manager, current_tasker.arguments))
                database = tasker.tasker.outputer.db
                executor.execute()
                query = database.query(collection_name, ["id"])
                datas = query.commit()
                delete = database.delete(collection_name, ["id"])
                delete.commit()
                return datas
        finally:
            _thread_local.current_tasker = current_tasker