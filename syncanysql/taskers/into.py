# -*- coding: utf-8 -*-
# 2023/3/17
# create by: snower


class IntoTasker(object):
    def __init__(self, tasker, config):
        self.tasker = tasker
        self.config = config

    def start(self, name, executor, session_config, manager, arguments):
        for variable in self.config["variables"]:
            if variable in executor.env_variables:
                continue
            executor.env_variables[variable] = None
        self.tasker.config["output"] = "&.--.__into_" + str(id(self)) + "::" + self.tasker.config["output"].split("::")[-1]
        self.tasker.config["name"] = self.tasker.config["name"] + "#into"
        self.tasker.start(name, executor, session_config, manager, arguments)
        return [self]

    def run(self, executor, session_config, manager):
        try:
            core_tasker = self.tasker.tasker
            if not core_tasker:
                return 1
            database = core_tasker.outputer.db
            name = core_tasker.outputer.name
            schema_keys = tuple(core_tasker.schema.keys()) if isinstance(core_tasker.schema, dict) else None
            self.tasker.run(executor, session_config, manager)

            query = database.query(name, ["id"])
            datas = query.commit()
            delete = database.delete(name, ["id"])
            delete.commit()

            if not schema_keys:
                value = datas[0] if len(datas) == 1 else datas
                if len(self.config["variables"]) == 1 and isinstance(value, dict) and len(value) == 1:
                    value = list(value.values())[0]
            elif len(schema_keys) == 1:
                if len(datas) == 1:
                    value = datas[0][schema_keys[0]] if schema_keys[0] in datas[0] else None
                else:
                    value = [data[schema_keys[0]] for data in datas if schema_keys[0] in data]
            else:
                if len(datas) == 1:
                    value = {key: datas[0].get(key) for key in schema_keys}
                else:
                    value = [{key: data.get(key) for key in schema_keys} for data in datas]

            if len(self.config["variables"]) == 1:
                if value is None:
                    if self.config["variables"][0] in executor.env_variables:
                        executor.env_variables[self.config["variables"][0]] = None
                else:
                    executor.env_variables[self.config["variables"][0]] = value
            else:
                for i in range(len(self.config["variables"])):
                    if i >= len(schema_keys):
                        if self.config["variables"][i] in executor.env_variables:
                            executor.env_variables[self.config["variables"][i]] = None
                        continue
                    if isinstance(value, dict):
                        executor.env_variables[self.config["variables"][i]] = value[schema_keys[i]]
                    else:
                        executor.env_variables[self.config["variables"][i]] = [v[schema_keys[i]] for v in value]
        finally:
            self.tasker = None

    def terminate(self):
        if not self.tasker:
            return
        self.tasker.terminate()
        self.tasker = None
