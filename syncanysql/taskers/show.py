# -*- coding: utf-8 -*-
# 2023/2/27
# create by: snower

import copy


class ShowCommandTasker(object):
    def __init__(self, config):
        self.config = config

    def start(self, name, executor, session_config, manager, arguments):
        if self.config.get("key").lower() == "databases":
            self.show_databases(session_config)
        elif self.config.get("key").lower() == "imports":
            self.show_imports(session_config)
        return []

    def run(self, executor, session_config, manager):
        pass

    def terminate(self):
        pass

    def show_databases(self, session_config):
        databases = copy.deepcopy(session_config.get().get("databases", []))
        fields, datas = ["name", "driver", "params", "virtual_views"], []
        for database in databases:
            virtual_views = database.pop("virtual_views", None)
            datas.append({
                "name": database.pop("name"),
                "driver": database.pop("driver"),
                "params": database,
                "virtual_views": virtual_views if virtual_views else "",
            })
        self.print(fields, datas)

    def show_imports(self, session_config):
        imports = copy.deepcopy(session_config.get().get("imports", {}))
        fields, datas = ["name", "alias"], []
        for alias, name in imports.items():
            datas.append({
                "name": name,
                "alias": alias
            })
        self.print(fields, datas)

    def print(self, fields, datas):
        try:
            import rich
            from rich.table import Table
        except ImportError:
            print("\t".join(fields))
            for data in datas:
                print("\t".join([str(data[field]) for field in fields]))
            return

        table = Table(show_header=True, collapse_padding=True, expand=True, highlight=True)
        for field in fields:
            table.add_column(field)
        for data in datas:
            table.add_row(*(str(data[field]) for field in fields))
        rich.print(table)