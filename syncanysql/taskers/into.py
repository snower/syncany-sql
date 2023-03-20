# -*- coding: utf-8 -*-
# 2023/3/17
# create by: snower

from syncany.database.memory import MemoryDBFactory, MemoryDBCollection


class IntoTasker(object):
    def __init__(self, tasker, config):
        self.tasker = tasker
        self.config = config

    def start(self, executor, session_config, manager, arguments):
        for variable in self.config["variables"]:
            if variable in executor.env_variables:
                continue
            executor.env_variables[variable] = None
        self.tasker.config["output"] = "&.--.__into_" + str(id(self)) + "::" + self.tasker.config["output"].split("::")[-1].split(" ")[0]
        self.tasker.config["name"] = self.tasker.config["name"] + "#into"
        self.tasker.start(executor, session_config, manager, arguments)
        return [self]

    def run(self, executor, session_config, manager):
        try:
            exit_code = self.tasker.run(executor, session_config, manager)
            if exit_code is not None and exit_code != 0:
                return exit_code
            collection_key = "--.__into_" + str(id(self))
            for config_key, factory in manager.database_manager.factorys.items():
                if not isinstance(factory, MemoryDBFactory):
                    continue
                for driver in factory.drivers:
                    if not isinstance(driver.instance, MemoryDBCollection):
                        continue
                    for key in list(driver.instance.keys()):
                        if collection_key != key:
                            continue
                        collection_instance = driver.instance[key]
                        driver.instance.remove(key)
                        if len(collection_instance) == 1:
                            collection_instance = collection_instance[0]
                        if len(self.config["variables"]) == 1:
                            if not collection_instance:
                                if self.config["variables"][0] in executor.env_variables:
                                    executor.env_variables[self.config["variables"][0]] = None
                            elif isinstance(collection_instance, dict):
                                executor.env_variables[self.config["variables"][0]] = list(collection_instance.values())[0] \
                                    if len(collection_instance) == 1 else collection_instance
                            else:
                                executor.env_variables[self.config["variables"][0]] = collection_instance
                        else:
                            collection_keys = list(collection_instance.keys()) if isinstance(collection_instance, dict) else []
                            for i in range(len(self.config["variables"])):
                                if i >= len(collection_keys):
                                    if self.config["variables"][i] in executor.env_variables:
                                        executor.env_variables[self.config["variables"][i]] = None
                                    continue
                                executor.env_variables[self.config["variables"][i]] = collection_instance[collection_keys[i]]
                        return exit_code
        finally:
            self.tasker = None

    def terminate(self):
        if not self.tasker:
            return
        self.tasker.terminate()
        self.tasker = None
