# -*- coding: utf-8 -*-
# 2025/3/24
# create by: snower

import copy
from syncany.database import find_database, DatabaseUnknownException
from syncany.taskers.tasker import _thread_local
from syncany.calculaters.calculater import Calculater


class ExecuteQueryTaskerCalculater(Calculater):
    def calculate(self, primary_keys, query, task_config):
        from ..executor import Executor
        from ..taskers.query import QueryTasker

        current_tasker = _thread_local.current_tasker
        current_executor = Executor.current()
        try:
            with Executor(current_executor.manager, current_executor.session_config.session(), current_executor) as executor:
                if "in" in query["filters"]:
                    for value in query["filters"]["in"]:
                        if not isinstance(value, tuple):
                            continue
                        if not value[1]:
                            return []
                        executor.env_variables["@" + value[0]] = value[1]
                task_config = copy.deepcopy(task_config)
                collection_name = "--.__queryTasker_" + str(id(executor))
                task_config["output"] = "&." + collection_name + "::" + task_config["output"].split("::")[-1].split(" ")[0]
                task_config["name"] = task_config["name"] + "#queryTasker"
                tasker = QueryTasker(task_config, is_inner_subquery=True)
                executor.runners.extend(tasker.start(task_config.get("name"), executor, executor.session_config,
                                                     executor.manager, current_tasker.arguments))
                executor.execute()

                try:
                    database_config = dict(**[database for database in current_executor.session_config.get()["databases"]
                                              if database["name"] == "--"][0])
                except (TypeError, KeyError, IndexError):
                    raise DatabaseUnknownException("memory db is unknown")
                database_cls = find_database(database_config.pop("driver"))
                if not database_cls:
                    return
                database = database_cls(executor.manager.database_manager, database_config).build()
                try:
                    query = database.query(collection_name, ["id"])
                    datas = query.commit()
                    delete = database.delete(collection_name, ["id"])
                    delete.commit()
                    return datas
                finally:
                    database.close()
        finally:
            _thread_local.current_tasker = current_tasker