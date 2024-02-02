# -*- coding: utf-8 -*-
# 2024/2/2
# create by: snower

from syncany.calculaters.calculater import Calculater


class RowIndexCalculater(Calculater):
    def calculate(self, *args):
        from syncany.taskers.context import TaskerContext
        tasker_context = TaskerContext.current()
        if not tasker_context:
            return None
        tasker_context_cache = tasker_context.cache("sql_row")
        row_index = tasker_context_cache.get("row_index", 0) + 1
        tasker_context_cache["row_index"] = row_index
        return row_index


class RowLastCalculater(Calculater):
    def calculate(self, *args):
        from syncany.taskers.context import TaskerContext
        tasker_context = TaskerContext.current()
        if not tasker_context:
            return None
        tasker_context_cache = tasker_context.cache("sql_row")
        row_last = tasker_context_cache.get("row_last")
        if args:
            tasker_context_cache["row_last"] = args[0] if len(args) == 1 else args
        return row_last
