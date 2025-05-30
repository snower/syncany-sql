# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os
import copy
import time
import traceback
import uuid
from collections import defaultdict
from syncany.logger import get_logger
from syncany.taskers.core import CoreTasker
from syncany.hook import Hooker
from syncany.taskers.tasker import _thread_local
from syncany.main import TaskerYieldNext, warp_database_logging


DEAULT_AGGREGATE = {"key": None, "schema": {}, "window_schema": {}, "having_columns": set([]),
                    "distinct_keys": [], "distinct_aggregates": set([])}


class ReduceHooker(Hooker):
    def __init__(self, executor, session_config, manager, arguments, tasker, config, batch, aggregate):
        self.executor = executor
        self.session_config = session_config
        self.manager = manager
        self.arguments = arguments
        self.tasker = tasker
        self.config = config
        self.batch = batch
        self.aggregate = aggregate
        self.count = 0
        self.batch_count = 0

    def outputed(self, tasker, datas):
        if not datas:
            return
        if self.batch > 0 and self.count > 0 and self.count + len(datas) >= self.batch:
            self.count, self.batch_count = 0, 1
            self.tasker.run_reduce(self.executor, self.session_config, self.manager, self.arguments, False)
        else:
            self.count += len(datas)
            self.batch_count += 1
            limit = tasker.arguments["@limit"] if "@limit" in tasker.arguments and tasker.arguments["@limit"] > 0 else 0
            if 0 < limit <= tasker.status["store_count"]:
                self.count, self.batch_count = 0, 1
                self.tasker.run_reduce(self.executor, self.session_config, self.manager, self.arguments, False)

    def finaled(self, tasker, e=None):
        if e is not None or self.batch_count <= 1:
            return
        self.tasker.run_reduce(self.executor, self.session_config, self.manager, self.arguments, False)


class QueryTaskerTemporaryMemoryManager(object):
    def __init__(self):
        self.collections = defaultdict(int)

    def add_collection(self, name):
        if name not in self.collections:
            self.collections[name] = 0

    def use_collection(self, name):
        self.collections[name] += 1

    def unuse_collection(self, name):
        if name not in self.collections:
            return True
        self.collections[name] -= 1
        return self.collections[name] <= 0

    def free_collection(self, name):
        if name not in self.collections:
            return True
        if self.collections[name] > 0:
            return False
        self.collections.pop(name, None)
        return True

    def get_names(self):
        return list(self.collections.keys())


class QueryTasker(object):
    ENV_ARGUMENTS = {key.lower(): value for key, value in os.environ.items()}

    def __init__(self, config, sql_expression=None, temporary_memory_manager=None, is_inner_subquery=False):
        self.name = config.get("name", "")
        self.config = config
        self.sql_expression = sql_expression
        self.reduce_config = None
        self.tasker = None
        self.dependency_taskers = []
        self.arguments = None
        self.tasker_generator = None
        self.updaters = []
        self.temporary_memory_manager = temporary_memory_manager or QueryTaskerTemporaryMemoryManager()
        self.is_inner_subquery = is_inner_subquery
        self.run_start_time = 0
        self.tasker_index = 0

    def start(self, name, executor, session_config, manager, arguments):
        dependency_taskers, aggregate = [], self.config.pop("aggregate", None)
        if aggregate and aggregate.get("distinct_keys"):
            self.config, distinct_config = self.compile_distinct_config(aggregate), self.config
            distinct_config["name"] = distinct_config["name"] + "#select@distinct"
            distinct_tasker = QueryTasker(distinct_config, temporary_memory_manager=self.temporary_memory_manager, is_inner_subquery=True)
            distinct_tasker.start(name, executor, session_config, manager, copy.deepcopy(arguments))
            dependency_taskers.append(distinct_tasker)
            arguments["@limit"] = 0

        for dependency_config in self.config.get("dependencys", []):
            kn, knl = (dependency_config["name"] + "@"), len(dependency_config["name"] + "@")
            dependency_arguments = {}
            for key, value in arguments.items():
                if key[:knl] != kn:
                    continue
                dependency_arguments[key[knl:]] = value
            dependency_tasker = QueryTasker(dependency_config, temporary_memory_manager=self.temporary_memory_manager, is_inner_subquery=True)
            dependency_tasker.start(name, executor, session_config, manager, dependency_arguments)
            dependency_taskers.append(dependency_tasker)

        limit, batch = int(arguments.get("@limit", 0)), int(arguments.get("@batch", 0))
        require_reduce = False
        if aggregate and (aggregate.get("schema") or aggregate["window_schema"]):
            if batch > 0:
                require_reduce = True
            elif [aggregate_column["final_value"] for aggregate_column in aggregate["schema"].values()
                  if aggregate_column["final_value"]]:
                require_reduce = True
            elif [aggregate_column["final_value"] for aggregate_column in aggregate["window_schema"].values()
                  if aggregate_column["final_value"]]:
                require_reduce = True
        if require_reduce and isinstance(self.config["schema"], dict) and not arguments.get("@streaming"):
            if not aggregate:
                aggregate = copy.deepcopy(DEAULT_AGGREGATE)
            self.compile_reduce_config(aggregate)
        elif 0 < limit < batch:
            arguments["@batch"] = limit

        if self.tasker_index <= 0:
            self.tasker_index = executor.distribute_tasker_index()
        tasker = CoreTasker(self.config, manager)
        if "#" not in tasker.config["name"]:
            tasker.config["name"] = tasker.config["name"] + "#select"
        if self.reduce_config and "_aggregate_key_" in self.reduce_config["schema"]:
            tasker.add_hooker(ReduceHooker(executor, session_config, manager, arguments,
                                           self, copy.deepcopy(self.config), batch, aggregate))
        arguments = self.compile_tasker(arguments, tasker)
        self.tasker, self.dependency_taskers, self.arguments = tasker, dependency_taskers, arguments
        return [self]

    def run(self, executor, session_config, manager):
        try:
            self.execute_updater(executor, session_config, manager)
            if not self.tasker_generator:
                self.tasker_generator = self.run_tasker(executor, session_config, manager, self.tasker, self.dependency_taskers)
                self.run_start_time = time.time()
            else:
                _thread_local.current_tasker = self.tasker
            while True:
                try:
                    value = self.tasker_generator.send(None)
                    if isinstance(value, TaskerYieldNext):
                        executor.add_runner(self)
                        return
                except StopIteration:
                    break
                except Exception as e:
                    if self.sql_expression:
                        get_logger().info("execute SQL %s finish with Exception %s:%s %.2fms", self.name, e.__class__.__name__, e,
                                          (time.time() - self.run_start_time) * 1000)
                    raise e
            if self.reduce_config:
                try:
                    self.run_reduce(executor, session_config, manager, self.arguments, True)
                except Exception as e:
                    if self.sql_expression:
                        get_logger().info("execute SQL %s finish with Exception %s:%s %.2fms", self.name, e.__class__.__name__, e,
                                          (time.time() - self.run_start_time) * 1000)
                    raise e
                if self.sql_expression:
                    get_logger().info("execute SQL %s finish %.2fms", self.name, (time.time() - self.run_start_time) * 1000)
        finally:
            names = self.get_temporary_memory_collections()
            if names:
                executor.clear_temporary_memory_collection(names)

    def run_yield(self, executor, session_config, manager, tasker, dependency_taskers):
        tasker_generator = tasker.run_yield()
        dependency_tasker_generators = [(dependency_tasker, self.run_tasker(executor, session_config, manager,
                                                                            dependency_tasker.tasker, dependency_tasker.dependency_taskers))
                                        for dependency_tasker in dependency_taskers]
        while True:
            for dependency_tasker, dependency_tasker_generator in tuple(dependency_tasker_generators):
                try:
                    _thread_local.current_tasker = dependency_tasker.tasker
                    dependency_tasker_generator.send(None)
                except StopIteration:
                    if dependency_tasker.reduce_config:
                        dependency_tasker.run_reduce(executor, session_config, manager, dependency_tasker.arguments, True)
                    dependency_tasker_generators.remove((dependency_tasker, dependency_tasker_generator))

            try:
                _thread_local.current_tasker = self.tasker
                tasker_generator.send(None)
            except StopIteration:
                break
            if executor.env_variables.get("@streaming", False):
                yield TaskerYieldNext()

    def terminate(self):
        if not self.tasker:
            return
        self.tasker.terminate()
        self.tasker = None

    def compile_distinct_config(self, aggregate):
        subquery_name = "__subquery_" + str(uuid.uuid1().int) + "_distinct"
        config = copy.deepcopy(self.config)
        config.pop("loader", None)
        config.pop("loader_arguments", None)
        config.update({
            "input": "&.--." + subquery_name + "::" + self.config["output"].split("::")[-1].split(" ")[0],
            "output": self.config["output"],
            "querys": [],
            "schema": {},
            "intercept": None,
            "orders": [],
            "pipelines": [],
            "options": {},
            "dependencys": [],
            "states": [],
        })

        is_multi_group_key = True
        if not aggregate["key"]:
            if len(aggregate["distinct_keys"]) == 1:
                group_column, is_multi_group_key = aggregate["distinct_keys"][0], False
            else:
                group_column = ["#make", aggregate["distinct_keys"], [":@aggregate_key", "$.*"]]
        elif isinstance(aggregate["key"], list) and aggregate["key"][0] == "#make":
            group_column = copy.deepcopy(aggregate["key"])
            group_column[1].extend(aggregate["distinct_keys"])
        else:
            group_column = ["#make", [copy.deepcopy(aggregate["key"])] + aggregate["distinct_keys"], [":@aggregate_key", "$.*"]]
        distinct_aggregate = copy.deepcopy(DEAULT_AGGREGATE)
        for key, column in tuple(self.config["schema"].items()):
            if key in aggregate["distinct_aggregates"]:
                config["schema"][key] = ["#make", {
                    "key": ["@aggregate_key", "$._aggregate_distinct_key_"] if is_multi_group_key else "$._aggregate_distinct_key_",
                    "value": "$." + key
                }, aggregate["schema"][key]["aggregate"]]
                self.config["schema"][key] = aggregate["schema"][key]["value"]
            elif key in aggregate["schema"]:
                self.config["schema"][key] = ["#make", {
                    "key": copy.deepcopy(group_column),
                    "value": aggregate["schema"][key]["value"]
                }, aggregate["schema"][key]["aggregate"]]
                distinct_aggregate["schema"][key] = copy.deepcopy(aggregate["schema"][key])
                distinct_aggregate["schema"][key]["final_value"] = None
                if is_multi_group_key:
                    config["schema"][key] = ["#aggregate", ["@aggregate_key", "$._aggregate_distinct_key_"],
                                             aggregate["schema"][key]["reduce"]]
                else:
                    config["schema"][key] = ["#aggregate", "$._aggregate_distinct_key_", aggregate["schema"][key]["reduce"]]
            else:
                config["schema"][key] = "$." + key

        if aggregate["key"]:
            self.config["schema"]["_aggregate_distinct_key_"] = aggregate["key"]
            if is_multi_group_key:
                aggregate["key"] = ["@aggregate_key", "$._aggregate_distinct_key_"]
            else:
                aggregate["key"] = "$._aggregate_distinct_key_"
        self.config["schema"]["_aggregate_distinct_aggregate_key_"] = ["#make", {
            "key": copy.deepcopy(group_column),
            "value": ["#const", 0]
        }, [":#aggregate", "$.key", "$$.value"]]
        distinct_aggregate["key"] = copy.deepcopy(group_column)
        distinct_aggregate["schema"]["_aggregate_distinct_aggregate_key_"] = {
            "key": group_column,
            "value": ["#const", 0],
            "calculate": ["#const", 0],
            "aggregate": [":#aggregate", "$.key", ["#const", 0]],
            "reduce": ["#const", 0],
            "final_value": None
        }
        self.config["aggregate"] = distinct_aggregate
        config["predicate"] = self.config.pop("predicate", None)
        self.config["output"] = "&.--." + subquery_name + "::" + self.config["output"].split("::")[-1].split(" ")[0] + " use I"
        self.config.pop("outputer", None)
        self.config.pop("outputer_arguments", None)
        return config

    def compile_reduce_config(self, aggregate):
        subquery_name = "__subquery_" + str(uuid.uuid1().int) + "_reduce"
        config = copy.deepcopy(self.config)
        config.pop("loader", None)
        config.pop("loader_arguments", None)
        config.update({
            "input": "&.--." + subquery_name + "::" + self.config["output"].split("::")[-1].split(" ")[0],
            "output": self.config["output"],
            "querys": [],
            "predicate": None,
            "schema": {},
            "orders": [],
            "pipelines": [],
            "options": {},
            "dependencys": [],
            "states": [],
        })

        is_multi_group_key = False
        if isinstance(aggregate["key"], list):
            if aggregate["key"] and aggregate["key"][0] == "@aggregate_key":
                is_multi_group_key = True
            elif len(aggregate["key"]) == 3 and aggregate["key"][0] == "#make" and aggregate["key"][2][0] == ":@aggregate_key":
                is_multi_group_key = True
        for key, column in self.config["schema"].items():
            if key in aggregate["schema"]:
                if is_multi_group_key:
                    config["schema"][key] = ["#aggregate", ["@aggregate_key", "$._aggregate_key_"], aggregate["schema"][key]["reduce"]]
                else:
                    config["schema"][key] = ["#aggregate", "$._aggregate_key_", aggregate["schema"][key]["reduce"]]
            else:
                config["schema"][key] = "$." + key
        if aggregate["key"]:
            config["schema"]["_aggregate_key_"] = "$._aggregate_key_"
            self.config["schema"]["_aggregate_key_"] = aggregate["key"]
        config["aggregate"] = aggregate
        config["intercept"] = self.config.pop("intercept", None)
        config["pipelines"] = self.config.pop("pipelines", [])
        self.config["output"] = "&.--." + subquery_name + "::" + self.config["output"].split("::")[-1].split(" ")[0] + " use I"
        self.config.pop("outputer", None)
        self.config.pop("outputer_arguments", None)
        self.reduce_config = config

    def run_reduce(self, executor, session_config, manager, arguments, final_reduce=False):
        config, arguments = copy.deepcopy(self.reduce_config), copy.deepcopy(arguments)
        aggregate = config.pop("aggregate", None)
        if final_reduce:
            config["schema"].pop("_aggregate_key_", None)
            for key, column in config["schema"].items():
                if key in aggregate["schema"] and aggregate["schema"][key]["final_value"]:
                    config["schema"][key] = aggregate["schema"][key]["final_value"]
                else:
                    config["schema"][key] = "$." + key
            for key, column in aggregate["window_schema"].items():
                if isinstance(column["aggregate"], dict):
                    for window_aggregate_key in column["aggregate"]:
                        config["schema"].pop(window_aggregate_key, None)
                    config["schema"][key] = column["final_value"]
            config["name"] = config["name"] + "#select@final"
        else:
            config["output"] = config["input"] + " use DI"
            config["name"] = config["name"] + "#select@reduce"
            config["intercept"] = None
            config["pipelines"] = []

        tasker = CoreTasker(config, manager)
        arguments["@primary_order"] = False
        arguments["@limit"] = 0
        self.compile_tasker(arguments, tasker)
        tasker_generator = self.run_tasker(executor, session_config, manager, tasker, [])
        while True:
            try:
                tasker_generator.send(None)
            except StopIteration:
                break
        if not final_reduce:
            self.tasker.status["store_count"] = tasker.status["store_count"]

    def compile_tasker(self, arguments, tasker):
        tasker.load()
        compile_arguments = dict(**self.ENV_ARGUMENTS)
        compile_arguments.update(arguments)
        if self.is_local_memory_database(tasker.config):
            if isinstance(tasker.config["input"], str) and "&.--." in tasker.config["input"] and not arguments.get("@loop", False):
                compile_arguments["@batch"] = 0
            if isinstance(tasker.config["output"], str) and "&.--." in tasker.config["output"]:
                compile_arguments["@insert_batch"] = 0

        tasker.compile(compile_arguments)
        if "@verbose" in compile_arguments and compile_arguments["@verbose"]:
            warp_database_logging(tasker)

        if hasattr(tasker.loader, "name"):
            if tasker.loader.name.startswith("--.__subquery_") or tasker.loader.name.startswith("--.__unionquery_"):
                self.temporary_memory_manager.use_collection(tasker.loader.name)

        if hasattr(tasker.outputer, "name"):
            if tasker.outputer.name.startswith("--.__subquery_") or tasker.outputer.name.startswith("--.__unionquery_"):
                self.temporary_memory_manager.add_collection(tasker.outputer.name)

        loader_or_outputers = [tasker.loader, tasker.outputer] + list(tasker.join_loaders.values())
        for loader_or_outputer in loader_or_outputers:
            if not hasattr(loader_or_outputer, "name"):
                continue
            info = loader_or_outputer.name.split(".")
            if len(info) <= 1:
                continue
            db, table_name = info[0], info[1]
            if not table_name.startswith("#{env_variable__@") or table_name[-1] != "}":
                continue
            if not loader_or_outputer.name.startswith(db + "."):
                continue

            def env_variable_setter(ctasker, ctable_name, obj, prefix, key):
                def _(executor, session_config, manager):
                    table_name_value = str(executor.env_variables.get_value(key))
                    if ctable_name in ctasker.input:
                        ctasker.input = ctasker.input.replace(ctable_name, table_name_value)
                    if ctable_name in ctasker.output:
                        ctasker.output = ctasker.output.replace(ctable_name, table_name_value)
                    obj.name = prefix + table_name_value
                return _
            self.updaters.append(env_variable_setter(tasker, table_name, loader_or_outputer, db + ".", table_name[16:-1]))
        tasker.name = "[%s]%s" % (self.tasker_index, tasker.name)
        setattr(tasker, "running_id", self.tasker_index)
        return compile_arguments

    def run_tasker(self, executor, session_config, manager, tasker, dependency_taskers):
        try:
            tasker_generator = self.run_yield(executor, session_config, manager, tasker, dependency_taskers)
            while True:
                try:
                    value = tasker_generator.send(None)
                    if isinstance(value, TaskerYieldNext):
                        yield value
                except StopIteration:
                    break
        except SystemError as e:
            self.check_free_temporary_memory_collection(executor, tasker)
            tasker.close(False, "signal terminaled")
            get_logger().error("signal exited")
            raise e
        except KeyboardInterrupt as e:
            self.check_free_temporary_memory_collection(executor, tasker)
            tasker.close(False, "user terminaled")
            get_logger().error("Crtl+C exited")
            raise e
        except Exception as e:
            self.check_free_temporary_memory_collection(executor, tasker)
            tasker.close(False, "Error: " + repr(e), traceback.format_exc())
            get_logger().error("tasker %s error: %s\n%s", tasker.name, e, traceback.format_exc())
            raise e
        else:
            self.check_free_temporary_memory_collection(executor, tasker)
            tasker.close()

    def execute_updater(self, executor, session_config, manager):
        for updater in self.updaters:
            updater(executor, session_config, manager)

        for dependency_tasker in self.dependency_taskers:
            dependency_tasker.execute_updater(executor, session_config, manager)

    def get_temporary_memory_collections(self):
        names = self.temporary_memory_manager.get_names()
        for dependency_tasker in self.dependency_taskers:
            names.extend(dependency_tasker.get_temporary_memory_collections())
        return list(set(names))

    def check_free_temporary_memory_collection(self, executor, tasker):
        try:
            if not hasattr(tasker.loader, "name"):
                return
            if not tasker.loader.name.startswith("--.__subquery_") and not tasker.loader.name.startswith("--.__unionquery_"):
                return
            if hasattr(tasker.outputer, "name") and tasker.loader.name == tasker.outputer.name:
                return
            name = tasker.loader.name
            if self.temporary_memory_manager.unuse_collection(name):
                database = tasker.databases.instance("--")
                if executor.runners and database.is_streaming(name):
                    return
                delete = database.delete(name, ["id"])
                delete.commit()
                self.temporary_memory_manager.free_collection(name)
        except:
            pass

    def is_local_memory_database(self, config, name="--"):
        try:
            database_config = dict(**[database for database in config["databases"]
                                      if database["name"] == name][0])
            return database_config["driver"] == "memory"
        except (TypeError, KeyError, IndexError):
            return False
