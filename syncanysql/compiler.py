# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os
import copy
import uuid
from sqlglot import maybe_parse
from sqlglot import expressions as sqlglot_expressions
from sqlglot.dialects import Dialect
from sqlglot import tokens
from .errors import SyncanySqlCompileException
from .config import CONST_CONFIG_KEYS
from .parser import SqlParser
from .taskers.delete import DeleteTasker
from .taskers.query import QueryTasker
from .taskers.explain import ExplainTasker
from .taskers.set import SetCommandTasker
from .taskers.execute import ExecuteTasker
from .taskers.use import UseCommandTasker


class CompilerDialect(Dialect):
    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"']
        COMMENTS = ["--", "#", ("/*", "*/")]
        IDENTIFIERS = ["`"]
        ESCAPES = ["'", "\\"]
        BIT_STRINGS = [("b'", "'"), ("B'", "'"), ("0b", "")]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", "")]


class Compiler(object):
    def __init__(self, config):
        self.config = config
        self.mapping = {}

    def compile(self, sql, arguments):
        sql = self.parse_mapping(sql)
        expression = maybe_parse(sql, dialect=CompilerDialect)
        if isinstance(expression, sqlglot_expressions.Delete):
            return DeleteTasker(self.compile_delete(expression, arguments))
        elif isinstance(expression, (sqlglot_expressions.Union, sqlglot_expressions.Insert, sqlglot_expressions.Select)):
            return QueryTasker(self.compile_query(expression, arguments))
        elif isinstance(expression, sqlglot_expressions.Command):
            if expression.args["this"].lower() == "explain" and self.is_const(expression.args["expression"]):
                return ExplainTasker(self.compile(self.parse_const(expression.args["expression"])["value"], arguments))
            if expression.args["this"].lower() == "set" and self.is_const(expression.args["expression"]):
                value = self.parse_const(expression.args["expression"])["value"].split("=")
                config = {"key": value[0].strip(), "value": "=".join(value[1:]).strip()}
                return SetCommandTasker(config)
            if expression.args["this"].lower() == "execute" and self.is_const(expression.args["expression"]):
                filename = self.parse_const(expression.args["expression"])["value"]
                if filename and filename[0] == '`':
                    filename = filename[1:-1]
                if filename in self.mapping:
                    filename = self.mapping[filename]
                return ExecuteTasker({"filename": filename})
        elif isinstance(expression, sqlglot_expressions.Use):
            use_info = expression.args["this"].args["this"].name
            if use_info in self.mapping:
                use_info = self.mapping[use_info]
            return UseCommandTasker({"use": use_info})
        raise SyncanySqlCompileException("unkonw sql: " + self.to_sql(expression))

    def compile_delete(self, expression, arguments):
        config = copy.deepcopy(self.config)
        config.update({
            "input": "&.--.__subquery_null_" + str(uuid.uuid1().int) + "::id",
            "output": "&.-.&1::id",
            "querys": {},
            "schema": "$.*",
        })

        table_info = self.parse_table(expression.args["this"], config)
        where_expression = expression.args.get("where")
        if where_expression and isinstance(where_expression, sqlglot_expressions.Where):
            self.compile_where_condition(table_info, where_expression.args["this"], config)
            self.parse_condition_typing_filter(config)
        config["output"] = "".join(["&.", table_info["db"], ".", table_info["name"], "::id use DI"])
        return config

    def compile_query(self, expression, arguments):
        config = copy.deepcopy(self.config)
        config.update({
            "input": "&.-.&1::id",
            "output": "&.-.&1::id",
            "querys": {},
            "schema": "$.*",
            "intercepts": [],
            "orders": [],
            "dependencys": [],
            "pipelines": []
        })
        if isinstance(expression, sqlglot_expressions.Union):
            self.compile_union(expression, config, arguments)
        elif isinstance(expression, sqlglot_expressions.Insert):
            self.compile_insert_into(expression, config, arguments)
        elif isinstance(expression, sqlglot_expressions.Select):
            self.compile_select(expression, config, arguments)
        else:
            raise SyncanySqlCompileException("unkonw sql: " + self.to_sql(expression))
        return config

    def compile_subquery(self, expression, arguments):
        table_name = expression.args["alias"].args["this"].name if "alias" in expression.args and expression.args["alias"] else "anonymous"
        subquery_name = "__subquery_" + str(uuid.uuid1().int) + "_" + table_name
        subquery_arguments = {key: arguments[key] for key in CONST_CONFIG_KEYS if key in arguments}
        subquery_config = self.compile_query(expression.args["this"], subquery_arguments)
        subquery_config["output"] = "&.--." + subquery_name + "::" + subquery_config["output"].split("::")[-1]
        subquery_config["name"] = subquery_config["name"] + "#" + subquery_name[2:]
        arguments.update({subquery_config["name"] + "@" + key: value for key, value in subquery_arguments.items()})
        arguments["@primary_order"] = False
        return subquery_name, subquery_config

    def compile_insert_into(self, expression, config, arguments):
        columns = []
        if isinstance(expression.args["this"], sqlglot_expressions.Table):
            table_info = self.parse_table(expression.args["this"], config)
        elif isinstance(expression.args["this"], sqlglot_expressions.Schema):
            schema_expression = expression.args["this"]
            if not isinstance(schema_expression.args["this"], sqlglot_expressions.Table) or not schema_expression.args.get("expressions"):
                raise SyncanySqlCompileException("unkonw insert info schema: " + self.to_sql(expression))
            table_info = self.parse_table(schema_expression.args["this"], config)
            for column_expression in schema_expression.args["expressions"]:
                column_name = column_expression.name
                if column_name in self.mapping:
                    column_name = self.mapping[column_name]
                try:
                    start_index, end_index = column_name.index("["), column_name.rindex("]")
                    typing_filters = column_name[start_index + 1: end_index].split(";")
                    column_name = column_name[:start_index]
                except ValueError:
                    typing_filters = []
                columns.append([column_name, typing_filters[0] if typing_filters else None])
        else:
            raise SyncanySqlCompileException("unkonw insert into expression: " + self.to_sql(expression))
        if not expression.args.get("expression"):
            raise SyncanySqlCompileException("unkonw insert expression: " + self.to_sql(expression))

        if table_info["db"] == "-" and table_info["name"] == "_":
            config["output"] = "&.-.&1::id use I"
        else:
            update_types = [o for o in table_info["typing_options"] if o.upper() in ("I", "UI", "UDI", "DI")] if table_info["typing_options"] else []
            config["output"] = "".join(["&.", table_info["db"], ".", table_info["name"], "::",
                                        "id", (" use " + update_types[0].upper()) if update_types else ""])

        if isinstance(expression.args["expression"], (sqlglot_expressions.Select, sqlglot_expressions.Union)):
            select_expression = expression.args["expression"]
            if isinstance(select_expression, sqlglot_expressions.Union):
                self.compile_union(select_expression, config, arguments)
            else:
                self.compile_select(select_expression, config, arguments)
        elif isinstance(expression.args["expression"], sqlglot_expressions.Values):
            values_expression = expression.args["expression"]
            if not values_expression.args.get("expressions"):
                raise SyncanySqlCompileException("unkonw insert expression: " + self.to_sql(expression))
            datas = []
            for data_expression in values_expression.args["expressions"]:
                data = {}
                for i in range(len(data_expression.args["expressions"])):
                    value_expression = data_expression.args["expressions"][i]
                    value = self.parse_const(value_expression)["value"]
                    if columns[i][1] is None:
                        columns[i][1] = str(type(value).__name__) if isinstance(value, (int, float, bool)) else None
                    data[columns[i][0]] = value
                datas.append(data)
            config["schema"] = {column_name: "$." + column_name + (("|" + column_type) if column_type else "")
                                for column_name, column_type in columns}
            config["input"] = "&.-.-::" + columns[0][0]
            config["loader"] = "const_loader"
            config["loader_arguments"] = {"datas": datas}
        else:
            raise SyncanySqlCompileException("unkonw insert expression: " + self.to_sql(expression))

    def compile_union(self, expression, config, arguments):
        select_expressions = []
        def parse(union_expression):
            if isinstance(union_expression.args["this"], sqlglot_expressions.Select):
                select_expressions.append(union_expression.args["this"])
            else:
                parse(union_expression.args["this"])
            if isinstance(union_expression.args["expression"], sqlglot_expressions.Select):
                select_expressions.append(union_expression.args["expression"])
            else:
                parse(union_expression.args["expression"])
        parse(expression)

        query_name = "__unionquery_" + str(uuid.uuid1().int)
        for select_expression in select_expressions:
            subquery_name = "__unionquery_" + str(uuid.uuid1().int)
            subquery_arguments = {key: arguments[key] for key in CONST_CONFIG_KEYS if key in arguments}
            subquery_config = self.compile_query(select_expression, subquery_arguments)
            subquery_config["output"] = "&.--." + query_name + "::" + subquery_config["output"].split("::")[-1].split(" use ")[0] + " use I"
            subquery_config["name"] = subquery_config["name"] + "#" + subquery_name[2:]
            arguments.update({subquery_config["name"] + "@" + key: value for key, value in subquery_arguments.items()})
            config["dependencys"].append(subquery_config)
        config["input"] = "&.--." + query_name + "::" + config["dependencys"][0]["output"].split("::")[-1].split(" ")[0]
        config["output"] = config["output"].split("::")[0] + "::" + config["input"].split("::")[-1].split(" ")[0]
        arguments["@primary_order"] = False
        arguments["@limit"] = 0

    def compile_select(self, expression, config, arguments):
        primary_table = {"db": None, "name": None, "table_name": None, "table_alias": None, "seted_primary_keys": False,
                         "loader_primary_keys": [], "outputer_primary_keys": [], "columns": {}}

        from_expression = expression.args.get("from")
        if not from_expression:
            primary_table["db"] = "--"
            primary_table["name"] = "--"
            primary_table["table_alias"] = "--"
            primary_table["table_name"] = "--"
            arguments["@limit"] = 1
            arguments["@batch"] = 0
        else:
            if not isinstance(from_expression, sqlglot_expressions.From) or not from_expression.expressions:
                raise SyncanySqlCompileException("unkonw table: " + self.to_sql(expression))
            if len(from_expression.expressions) > 1:
                raise SyncanySqlCompileException("noly query one table: " + self.to_sql(expression))
            from_expression = from_expression.expressions[0]
            if isinstance(from_expression, sqlglot_expressions.Table):
                table_info = self.parse_table(from_expression, config)
                primary_table["db"] = table_info["db"]
                primary_table["name"] = table_info["name"]
                primary_table["table_alias"] = table_info["table_alias"]
                primary_table["table_name"] = table_info["table_name"]
            elif isinstance(from_expression, sqlglot_expressions.Subquery):
                if "alias" not in from_expression.args:
                    raise SyncanySqlCompileException("subquery must be alias name: " + self.to_sql(expression))
                primary_table["table_alias"] = from_expression.args["alias"].args["this"].name \
                    if "alias" in from_expression.args and from_expression.args["alias"] else None
                primary_table["table_name"] = primary_table["table_alias"]
                subquery_name, subquery_config = self.compile_subquery(from_expression, arguments)
                primary_table["db"] = "--"
                primary_table["name"] = subquery_name
                config["dependencys"].append(subquery_config)
                if self.compile_pipleline_select(expression, config, arguments, primary_table):
                    return config
                if not primary_table["table_alias"]:
                    raise SyncanySqlCompileException("subquery must be alias: " + self.to_sql(expression))
            else:
                raise SyncanySqlCompileException("unkonw table: " + self.to_sql(expression))

        join_tables = self.parse_joins(primary_table, config, expression.args["joins"], arguments) \
            if "joins" in expression.args and expression.args["joins"] else {}
        group_fields = []
        group_expression = expression.args.get("group")
        if group_expression:
            self.parse_group(primary_table, group_expression, group_fields)

        select_expressions = expression.args.get("expressions")
        if not select_expressions:
            raise SyncanySqlCompileException("unkonw table select fields: " + self.to_sql(expression))
        config["schema"] = {}
        for select_expression in select_expressions:
            if isinstance(select_expression, sqlglot_expressions.Star):
                if len(select_expressions) != 1:
                    raise SyncanySqlCompileException("* query can only query the master table: " + self.to_sql(expression))
                config["schema"] = "$.*"
                break

            column_expression, aggregate_expression, calculate_expression, column_alias = None, None, None, None
            if self.is_column(select_expression):
                column_expression = select_expression
            elif isinstance(select_expression, sqlglot_expressions.Alias):
                column_alias = select_expression.args["alias"].name if "alias" in select_expression.args else None
                if self.is_const(select_expression.args["this"]):
                    const_info = self.parse_const(select_expression.args["this"])
                    config["schema"][column_alias] = self.compile_const(const_info)
                    continue
                elif self.is_column(select_expression.args["this"]):
                    column_expression = select_expression.args["this"]
                elif isinstance(select_expression.args["this"], (sqlglot_expressions.Count, sqlglot_expressions.Sum, sqlglot_expressions.Min, sqlglot_expressions.Max)):
                    aggregate_expression = select_expression.args["this"]
                else:
                    calculate_expression = select_expression.args["this"]
            elif self.is_const(select_expression):
                const_info = self.parse_const(select_expression)
                column_alias = str(select_expression)
                config["schema"][column_alias] = self.compile_const(const_info)
                continue
            elif isinstance(select_expression, (sqlglot_expressions.Anonymous, sqlglot_expressions.Binary, sqlglot_expressions.If, sqlglot_expressions.Case)):
                column_alias = str(select_expression)
                calculate_expression = select_expression
            elif isinstance(select_expression, (sqlglot_expressions.Count, sqlglot_expressions.Sum, sqlglot_expressions.Min, sqlglot_expressions.Max)):
                column_alias = str(select_expression)
                aggregate_expression = select_expression
            else:
                raise SyncanySqlCompileException("table select field must be alias: " + self.to_sql(expression))
            if column_expression:
                self.compile_select_column(primary_table, column_expression, column_alias, config, join_tables)
                continue
            if aggregate_expression:
                self.compile_aggregate_column(primary_table, column_alias, config, group_expression, aggregate_expression,
                                              group_fields, join_tables)
                continue

            calculate_fields = []
            self.parse_calculate(primary_table, calculate_expression, calculate_fields)
            calculate_table_names = {calculate_field["table_name"] for calculate_field in calculate_fields
                                     if calculate_field["table_name"] and calculate_field["table_name"] != primary_table["table_name"]}
            if calculate_table_names:
                column_join_tables = []
                self.compile_join_column_tables(primary_table, [join_tables[calculate_table_name] for calculate_table_name in calculate_table_names],
                                                                        join_tables, column_join_tables)
                calculate_column = self.compile_calculate(primary_table, calculate_expression, column_join_tables)
                config["schema"][column_alias] = self.compile_join_column(primary_table, calculate_column, column_join_tables)
            else:
                config["schema"][column_alias] = self.compile_calculate(primary_table, calculate_expression, [])
                if not primary_table["outputer_primary_keys"]:
                    primary_table["loader_primary_keys"] = [calculate_field["column_name"] for calculate_field in calculate_fields]
                    primary_table["outputer_primary_keys"] = [column_alias]

        where_expression = expression.args.get("where")
        if where_expression and isinstance(where_expression, sqlglot_expressions.Where):
            self.compile_where_condition(primary_table, where_expression.args["this"], config)
            self.parse_condition_typing_filter(config)

        having_expression = expression.args.get("having")
        if having_expression:
            config["intercepts"].append(self.compile_having_condition(primary_table, having_expression.args["this"], config))

        order_expression = expression.args.get("order")
        if order_expression:
            self.compile_order(primary_table, order_expression.args["expressions"], config)

        limit_expression = expression.args.get("limit")
        if limit_expression:
            arguments["@limit"] = int(limit_expression.args["expression"].args["this"])

        if group_expression and ("aggregate" not in config or not config["aggregate"] or not config["aggregate"]["reduces"]):
            self.compile_group_column(primary_table, config, group_expression, group_fields, join_tables)
        config["input"] = "".join(["&.", primary_table["db"], ".", primary_table["name"], "::",
                                   "+".join(primary_table["loader_primary_keys"]) if primary_table["loader_primary_keys"] else "id"])
        outputer_type = ""
        if " use " in config["output"]:
            outputer_type = " use " + config["output"].split(" use ")[-1]
        elif not isinstance(config["schema"], dict) or [key for key in primary_table["outputer_primary_keys"] if
                                                      key not in config["schema"]] or not config.get("querys"):
            outputer_type = " use I"
        elif [key for key in config["querys"] if key not in config["schema"]]:
            outputer_type = " use UI"
        config["output"] = "".join([config["output"].split("::")[0], "::",
                                    "+".join(primary_table["outputer_primary_keys"]) if primary_table["outputer_primary_keys"] else "id",
                                    outputer_type])

    def compile_pipleline_select(self, expression, config, arguments, primary_table):
        select_expressions = expression.args.get("expressions")
        if not select_expressions or len(select_expressions) != 1 or not isinstance(select_expressions[0], sqlglot_expressions.Anonymous):
            return None
        if expression.args.get("group") or expression.args.get("where") or expression.args.get("having") \
                or expression.args.get("order") or expression.args.get("limit"):
            return None
        calculate_fields = []
        self.parse_calculate(primary_table, select_expressions[0], calculate_fields)
        if calculate_fields:
            return None
        pipeline = self.compile_calculate(primary_table, select_expressions[0], [])
        if isinstance(pipeline, str):
            pipeline = [">>$.*|array", ":" + pipeline]
        else:
            pipeline[0] = ">>" + pipeline[0]
            pipeline = pipeline[:1] + ["$.*|array"] + pipeline[1:]
        config["pipelines"].append(pipeline)
        config["input"] = "".join(["&.", primary_table["db"], ".", primary_table["name"], "::",
                                   "+".join(primary_table["loader_primary_keys"]) if primary_table["loader_primary_keys"] else "id"])
        outputer_type = ""
        if " use " in config["output"]:
            outputer_type = " use " + config["output"].split(" use ")[-1]
        elif not isinstance(config["schema"], dict) or [key for key in primary_table["outputer_primary_keys"] if
                                                      key not in config["schema"]] or not config.get("querys"):
            outputer_type = " use I"
        elif [key for key in config["querys"] if key not in config["schema"]]:
            outputer_type = " use UI"
        config["output"] = "".join([config["output"].split("::")[0], "::",
                                    "+".join(primary_table["outputer_primary_keys"]) if primary_table["outputer_primary_keys"] else "id",
                                    outputer_type])
        arguments["@primary_order"] = False
        return config

    def compile_select_column(self, primary_table, column_expression, column_alias, config, join_tables):
        column_info = self.parse_column(column_expression)
        if not column_alias:
            column_alias = column_info["column_name"].replace(".", "_")
        if column_info["table_name"] and column_info["table_name"] != primary_table["table_name"] and column_info["table_name"] in join_tables:
            column_join_tables = []
            self.compile_join_column_tables(primary_table, [join_tables[column_info["table_name"]]], join_tables,
                                            column_join_tables)
            config["schema"][column_alias] = self.compile_join_column(primary_table, self.compile_column(column_info),
                                                                                   column_join_tables)
        else:
            config["schema"][column_alias] = self.compile_column(column_info)
        if not column_info["table_name"] or column_info["table_name"] == primary_table["table_name"]:
            primary_table["columns"][column_info["column_name"]] = column_info
        if "pk" in column_info["typing_options"]:
            if not primary_table["seted_primary_keys"]:
                primary_table["loader_primary_keys"], primary_table["outputer_primary_keys"], primary_table["seted_primary_keys"] = [], [], True
            primary_table["loader_primary_keys"].append(column_info["column_name"])
            primary_table["outputer_primary_keys"].append(column_alias)
        elif not primary_table["seted_primary_keys"] and (not primary_table["outputer_primary_keys"] or (column_alias and column_alias == "id")):
            if not column_info["table_name"] or column_info["table_name"] == primary_table["table_name"]:
                primary_table["loader_primary_keys"], primary_table["outputer_primary_keys"] = [column_info["column_name"]], [column_alias]

    def compile_join_column_tables(self, primary_table, current_join_tables, join_tables, column_join_tables):
        if not current_join_tables:
            return
        current_join_table_names = {current_join_table["name"] for current_join_table in current_join_tables}
        dup_column_join_tables = []
        for column_join_table in column_join_tables:
            if column_join_table["name"] in current_join_table_names:
                dup_column_join_tables.append(column_join_table)
        for dup_column_join_table in dup_column_join_tables:
            column_join_tables.remove(dup_column_join_table)
        column_join_tables.extend(current_join_tables)
        column_join_names = sorted(list({join_column["table_name"] for join_table in current_join_tables
                                         for join_column in join_table["join_columns"] if join_column["table_name"] and join_column["table_name"] != primary_table["table_name"]}),
                                   key=lambda x: 0xffffff if x == primary_table["table_name"] else join_tables[x]["ref_count"])
        self.compile_join_column_tables(primary_table, [join_tables[column_join_name] for column_join_name in column_join_names
                          if column_join_name != primary_table["table_name"]], join_tables, column_join_tables)

    def compile_join_column(self, primary_table, column, column_join_tables):
        for i in range(len(column_join_tables)):
            join_table = column_join_tables[i]
            if isinstance(column, str):
                column = ":" + column
            elif isinstance(column, list):
                if column and isinstance(column[0], str):
                    column[0] = ":" + column[0]
                else:
                    column = [":"] + column
            else:
                column = [":", column]
            if len(join_table["calculate_expressions"]) == 1:
                join_columns = self.compile_calculate(primary_table, join_table["calculate_expressions"][0], column_join_tables, i)
            else:
                join_columns = [self.compile_calculate(primary_table, calculate_expression, column_join_tables, i)
                               for calculate_expression in join_table["calculate_expressions"]]
            join_db_table = "&." + join_table["db"] + "." + join_table["table"] + "::" + "+".join(join_table["primary_keys"])
            if join_table["querys"]:
                join_db_table = [join_db_table, join_table["querys"]]
            column = [join_columns, join_db_table, column]
        return column

    def compile_join_column_field(self, primary_table, ci, join_column, column_join_tables):
        if not join_column["table_name"] or join_column["table_name"] == primary_table["table_name"]:
            return self.compile_column(join_column, len(column_join_tables) - ci)
        ji = [j for j in range(len(column_join_tables)) if join_column["table_name"] == column_join_tables[j]["name"]][0]
        return self.compile_column(join_column, ji - ci)

    def compile_where_condition(self, primary_table, expression, config):
        if not expression:
            return
        if isinstance(expression, sqlglot_expressions.And):
            self.compile_where_condition(primary_table, expression.args.get("this"), config)
            self.compile_where_condition(primary_table, expression.args.get("expression"), config)
            return

        def parse(expression):
            if expression.args.get("query"):
                table_expression, value_expression = expression.args["this"], expression.args["query"]
            elif expression.args.get("expressions"):
                table_expression, value_expression = expression.args["this"], expression.args["expressions"]
            elif isinstance(expression.args["expression"], sqlglot_expressions.Subquery):
                table_expression, value_expression = expression.args["this"], expression.args["expression"].args["this"]
            else:
                table_expression, value_expression = expression.args["this"], expression.args["expression"]
            if not self.is_column(table_expression):
                raise SyncanySqlCompileException("unkonw where condition: " + self.to_sql(expression))
            condition_table = table_expression.args["table"].name if "table" in table_expression.args else None
            if condition_table and condition_table != primary_table["table_name"]:
                raise SyncanySqlCompileException("unkonw where condition: " + self.to_sql(expression))

            condition_column = self.parse_column(table_expression)
            if isinstance(value_expression, sqlglot_expressions.Select):
                value_column = self.compile_query_condition(primary_table, config, value_expression,
                                                            condition_column["typing_filters"])
                if isinstance(expression, sqlglot_expressions.In):
                    value_column[1] = [":" + value_column[1], ":$.*|array"]
                else:
                    value_column[1] = [":" + value_column[1], [":$.*|array", ":$.:0"]]
            elif isinstance(value_expression, list):
                value_column = []
                for value_expression_item in value_expression:
                    if not self.is_const(value_expression_item):
                        raise SyncanySqlCompileException("unkonw where condition: " + self.to_sql(expression))
                    value_column.append(self.parse_const(value_expression_item)["value"])
            else:
                calculate_fields = []
                self.parse_calculate(primary_table, value_expression, calculate_fields)
                if calculate_fields:
                    raise SyncanySqlCompileException("unkonw where condition: " + self.to_sql(expression))
                value_column = self.compile_calculate(primary_table, value_expression, [])
            if condition_column["typing_name"] not in config["querys"]:
                config["querys"][condition_column["typing_name"]] = {}
            return condition_column, value_column

        if isinstance(expression, sqlglot_expressions.EQ):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["typing_name"]]["=="] = value_column
        elif isinstance(expression, sqlglot_expressions.NEQ):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["typing_name"]]["!="] = value_column
        elif isinstance(expression, sqlglot_expressions.GT):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["typing_name"]][">"] = value_column
        elif isinstance(expression, sqlglot_expressions.GTE):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["typing_name"]][">="] = value_column
        elif isinstance(expression, sqlglot_expressions.LT):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["typing_name"]]["<"] = value_column
        elif isinstance(expression, sqlglot_expressions.LTE):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["typing_name"]]["<="] = value_column
        elif isinstance(expression, sqlglot_expressions.In):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["typing_name"]]["in"] = value_column
        else:
            raise SyncanySqlCompileException("unkonw where condition: " + self.to_sql(expression))

    def compile_query_condition(self, primary_table, config, expression, typing_filters):
        select_expressions = expression.args.get("expressions")
        if not select_expressions or len(select_expressions) != 1 or not self.is_column(select_expressions[0]):
            raise SyncanySqlCompileException("unkonw query condition: " + self.to_sql(expression))
        if expression.args.get("group") or expression.args.get("having") \
                or expression.args.get("order") or expression.args.get("limit"):
            raise SyncanySqlCompileException("unkonw query condition: " + self.to_sql(expression))
        from_expression = expression.args.get("from")
        if not isinstance(from_expression, sqlglot_expressions.From) or not from_expression.expressions:
            raise SyncanySqlCompileException("unkonw table: " + self.to_sql(expression))
        if len(from_expression.expressions) > 1:
            raise SyncanySqlCompileException("noly query one table: " + self.to_sql(expression))
        table_info = self.parse_table(from_expression.expressions[0], config)
        column_info = self.parse_column(select_expressions[0])
        if column_info["typing_filters"] and typing_filters:
            column_info["typing_filters"] = typing_filters
        querys = {"querys": {}}
        where_expression = expression.args.get("where")
        if where_expression and isinstance(where_expression, sqlglot_expressions.Where):
            self.compile_where_condition(primary_table, where_expression.args["this"], querys)
            self.parse_condition_typing_filter(querys)
        db_table = "&." + table_info["db"] + "." + table_info["name"] + "::" + column_info["column_name"]
        if querys.get("querys"):
            return [[db_table, querys["querys"]], self.compile_column(column_info)]
        return [db_table, self.compile_column(column_info)]

    def compile_group_column(self, primary_table, config, group_expression, group_fields, join_tables):
        if not group_expression or not isinstance(config["schema"], dict) or not primary_table["outputer_primary_keys"]:
            raise SyncanySqlCompileException("group unkonw primary_key: " + self.to_sql(group_expression))
        column_alias = primary_table["outputer_primary_keys"][0]
        if column_alias not in config["schema"]:
            raise SyncanySqlCompileException("group unkonw primary_key: " + self.to_sql(group_expression))
        calculate_fields = [group_field for group_field in group_fields if group_field["table_name"]
                            and group_field["table_name"] != primary_table["table_name"]]
        column_join_tables = []
        if calculate_fields:
            calculate_table_names = {calculate_field["table_name"] for calculate_field in calculate_fields}
            self.compile_join_column_tables(primary_table, [join_tables[calculate_table_name] for calculate_table_name in calculate_table_names],
                                            join_tables, column_join_tables)
        group_column = ["@add", ["#const", "k_"]]
        if len(group_expression.args["expressions"]) > 1:
            for i in range(len(group_expression.args["expressions"]) - 1):
                group_column.append(self.compile_calculate(primary_table, group_expression.args["expressions"][i], column_join_tables, -1))
                group_column.append(["#const", "-"])
            group_column.append(self.compile_calculate(primary_table, group_expression.args["expressions"][-1], column_join_tables, -1))
        else:
            group_column.append(self.compile_calculate(primary_table, group_expression.args["expressions"][0], column_join_tables, -1))
        if "aggregate" not in config:
            config["aggregate"] = {"key": None, "reduces": {}, "having_columns": set([])}
        if calculate_fields:
            group_column = self.compile_join_column(primary_table, group_column, column_join_tables)
        config["schema"][column_alias] = ["#make", {
            "key": group_column,
            "value": config["schema"][column_alias]
        }, [":#aggregate", "$.key", "$$.value"]]
        config["aggregate"]["key"] = copy.deepcopy(group_column)
        config["aggregate"]["reduces"][column_alias] = "$$." + column_alias

    def compile_aggregate_column(self, primary_table, column_alias, config, group_expression, aggregate_expression, group_fields, join_tables):
        calculate_fields = [group_field for group_field in group_fields]
        self.parse_aggregate(primary_table, aggregate_expression, calculate_fields)
        calculate_fields = [calculate_field for calculate_field in calculate_fields if calculate_field["table_name"]
                            and calculate_field["table_name"] != primary_table["table_name"]]
        column_join_tables = []
        if calculate_fields:
            calculate_table_names = {calculate_field["table_name"] for calculate_field in calculate_fields}
            self.compile_join_column_tables(primary_table, [join_tables[calculate_table_name] for calculate_table_name in calculate_table_names],
                                            join_tables, column_join_tables)
        group_column = ["@add", ["#const", "k_"]]
        if not group_expression:
            group_column = ["@add", ["#const", "k_g"]]
        elif len(group_expression.args["expressions"]) > 1:
            for i in range(len(group_expression.args["expressions"]) - 1):
                group_column.append(self.compile_calculate(primary_table, group_expression.args["expressions"][i], column_join_tables, -1))
                group_column.append(["#const", "-"])
            group_column.append(self.compile_calculate(primary_table, group_expression.args["expressions"][-1], column_join_tables, -1))
        else:
            group_column.append(self.compile_calculate(primary_table, group_expression.args["expressions"][0], column_join_tables, -1))
        calculate_column = self.compile_aggregate(primary_table, column_alias, aggregate_expression, column_join_tables)
        if "aggregate" not in config:
            config["aggregate"] = {"key": None, "reduces": {}, "having_columns": set([])}
        if calculate_fields:
            config["schema"][column_alias] = self.compile_join_column(primary_table, ["#aggregate", group_column, calculate_column],
                                                                      column_join_tables)
            config["aggregate"]["key"] = self.compile_join_column(primary_table, copy.deepcopy(group_column), column_join_tables)
            config["aggregate"]["reduces"][column_alias] = [calculate_column[0], calculate_column[1], "$" + calculate_column[1]]
        else:
            config["schema"][column_alias] = ["#aggregate", group_column, calculate_column]
            config["aggregate"]["key"] = copy.deepcopy(group_column)
            config["aggregate"]["reduces"][column_alias] = [calculate_column[0], calculate_column[1], "$" + calculate_column[1]]

    def compile_aggregate(self, primary_table, column_alias, expression, column_join_tables, join_index=-1):
        if isinstance(expression, sqlglot_expressions.Count):
            return ["@add", "$." + column_alias, 1]
        elif isinstance(expression, sqlglot_expressions.Sum):
            return [
                "@add", "$." + column_alias,
                self.compile_calculate(primary_table, expression.args["this"], column_join_tables, join_index - 1)
            ]
        elif isinstance(expression, sqlglot_expressions.Min):
            return [
                "@min", "$." + column_alias,
                self.compile_calculate(primary_table, expression.args["this"], column_join_tables, join_index - 1)
            ]
        elif isinstance(expression, sqlglot_expressions.Max):
            return [
                "@max", "$." + column_alias,
                self.compile_calculate(primary_table, expression.args["this"], column_join_tables, join_index - 1)
            ]
        else:
            raise SyncanySqlCompileException("unkonw calculate: " + self.to_sql(expression))
        
    def compile_calculate(self, primary_table, expression, column_join_tables, join_index=-1):
        if isinstance(expression, sqlglot_expressions.Anonymous):
            calculater_name = expression.args["this"].lower()
            if calculater_name == "get_value":
                get_value_expressions = expression.args.get("expressions")
                if not get_value_expressions or len(get_value_expressions) < 2:
                    raise SyncanySqlCompileException("get_value args error: " + self.to_sql(expression))
                column = [self.compile_calculate(primary_table, get_value_expressions[0], column_join_tables, join_index)]

                def get_value_parse(get_value_expressions):
                    column = []
                    if self.is_const(get_value_expressions[0]):
                        get_value_key = self.parse_const(get_value_expressions[0])["value"]
                        column.append((":$.:" + str(int(get_value_key))) if isinstance(get_value_key, (int, float)) else (":$." + str(get_value_key)))
                    elif isinstance(get_value_expressions[0], sqlglot_expressions.Tuple):
                        get_value_indexs = [self.parse_const(tuple_expression)["value"] for tuple_expression in get_value_expressions[0].args["expressions"]
                                          if self.is_const(tuple_expression)]
                        column.append(":$.:" + ":".join([str(int(i)) for i in get_value_indexs if isinstance(i, (float, int))][:3]))
                    else:
                        raise SyncanySqlCompileException("get_value key must by string or int tuple: " + self.to_sql(get_value_expressions[0]))
                    if len(get_value_expressions) >= 2:
                        column.append(get_value_parse(get_value_expressions[1:]))
                    return column[0] if len(column) == 1 else column
                if not isinstance(column[0], str):
                    column = ["#if", True, column[0], None]
                column.append(get_value_parse(get_value_expressions[1:]))
                return column

            if calculater_name == "call_define":
                column = ["#call"]
                for arg_expression in expression.args.get("expressions", []):
                    if self.is_const(arg_expression):
                        column.append(self.parse_const(arg_expression)["value"])
                    else:
                        column.append(self.compile_calculate(primary_table, arg_expression, column_join_tables, join_index))
                return column

            if calculater_name == "yield_data":
                column = ["#yield"]
                for arg_expression in expression.args.get("expressions", []):
                    if self.is_const(arg_expression):
                        column.append(self.parse_const(arg_expression)["value"])
                    else:
                        column.append(self.compile_calculate(primary_table, arg_expression, column_join_tables, join_index))
                return column

            column = ["@" + "::".join(calculater_name.split("$"))]
            for arg_expression in expression.args.get("expressions", []):
                if self.is_const(arg_expression):
                    column.append(self.parse_const(arg_expression)["value"])
                else:
                    column.append(self.compile_calculate(primary_table, arg_expression, column_join_tables, join_index))
            return column
        elif isinstance(expression, sqlglot_expressions.Binary):
            return [
                "@" + expression.key.lower(),
                self.compile_calculate(primary_table, expression.args["this"], column_join_tables, join_index),
                self.compile_calculate(primary_table, expression.args["expression"], column_join_tables, join_index)
            ]
        elif isinstance(expression, sqlglot_expressions.If):
            return [
                "#if",
                self.compile_calculate(primary_table, expression.args["this"], column_join_tables, join_index),
                self.compile_calculate(primary_table, expression.args["true"], column_join_tables, join_index) if expression.args.get("true") else False,
                self.compile_calculate(primary_table, expression.args["false"], column_join_tables, join_index) if expression.args.get("false") else False,
            ]
        elif isinstance(expression, sqlglot_expressions.Case):
            cases = {}
            cases["#case"] = self.compile_calculate(primary_table, expression.args["this"], column_join_tables, join_index) \
                                 if expression.args.get("this") else ["#const", True]
            if expression.args.get("ifs"):
                for case_expression in expression.args["ifs"]:
                    if not isinstance(case_expression, sqlglot_expressions.If) or not self.is_const(case_expression.args["this"]):
                        raise SyncanySqlCompileException("unkonw calculate: " + self.to_sql(expression))
                    case_value = self.parse_const(case_expression.args["this"])["value"]
                    cases[(":" + str(case_value)) if isinstance(case_value, (int, float)) and not isinstance(case_value, bool) else case_value] = \
                        self.compile_calculate(primary_table, case_expression.args["true"], column_join_tables, join_index)
            cases["#end"] = self.compile_calculate(primary_table, expression.args["default"], column_join_tables, join_index) \
                                if expression.args.get("default") else None
            return cases
        elif isinstance(expression, sqlglot_expressions.Func):
            func_args = {
                "substring": ["this", "start", "length"],
            }
            func_name = expression.key.lower()
            column, arg_names = ["@" + func_name], (func_args.get(func_name) or ["this", "expression", "expressions"])
            for arg_name in arg_names:
                if not expression.args.get(arg_name):
                    continue
                column.append(self.compile_calculate(primary_table, expression.args[arg_name], column_join_tables, join_index))
            return column
        elif isinstance(expression, sqlglot_expressions.Paren):
            return self.compile_calculate(primary_table, expression.args["this"], column_join_tables, join_index)
        elif self.is_column(expression):
            join_column = self.parse_column(expression)
            return self.compile_join_column_field(primary_table, join_index, join_column, column_join_tables)
        elif isinstance(expression, sqlglot_expressions.Star):
            return "$.*"
        elif isinstance(expression, sqlglot_expressions.Tuple):
            return ["#const", [self.parse_const(tuple_expression)["value"] for tuple_expression in expression.args["expressions"]
                               if self.is_const(tuple_expression)]]
        elif self.is_const(expression):
            return self.compile_const(self.parse_const(expression))
        else:
            raise SyncanySqlCompileException("unkonw calculate: " + self.to_sql(expression))

    def compile_having_condition(self, primary_table, expression, config):
        if isinstance(expression, sqlglot_expressions.And):
            return [
                "@and",
                self.compile_having_condition(primary_table, expression.args.get("this"), config),
                self.compile_having_condition(primary_table, expression.args.get("expression"), config)
            ]
        if isinstance(expression, sqlglot_expressions.Or):
            return [
                "@or",
                self.compile_having_condition(primary_table, expression.args.get("this"), config),
                self.compile_having_condition(primary_table, expression.args.get("expression"), config)
            ]

        def parse(expression):
            if "aggregate" not in config:
                config["aggregate"] = {"key": None, "reduces": {}, "having_columns": set([])}
            if expression.args.get("expressions"):
                left_expression, right_expression = expression.args["this"], expression.args["expressions"]
            else:
                left_expression, right_expression = expression.args["this"], expression.args["expression"]
            if self.is_column(left_expression):
                left_column = self.parse_column(left_expression)
                if left_column["table_name"] or left_column["column_name"] not in config["schema"]:
                    raise SyncanySqlCompileException("unkonw having condition: " + self.to_sql(expression))
                config["aggregate"]["having_columns"].add(left_column["column_name"])
                left_calculater = self.compile_column(left_column)
            else:
                calculate_fields = []
                self.parse_calculate(primary_table, left_expression, calculate_fields)
                if [calculate_field for calculate_field in calculate_fields if calculate_field["column_name"] not in config["schema"]]:
                    raise SyncanySqlCompileException("unkonw having condition: " + self.to_sql(expression))
                for calculate_field in calculate_fields:
                    if calculate_field["column_name"] not in config["schema"]:
                        continue
                    config["aggregate"]["having_columns"].add(calculate_field["column_name"])
                left_calculater = self.compile_calculate(primary_table, left_expression, [])

            if isinstance(right_expression, list):
                value_items = []
                for value_expression_item in right_expression:
                    if not self.is_const(value_expression_item):
                        raise SyncanySqlCompileException("unkonw having condition: " + self.to_sql(expression))
                    value_items.append(self.parse_const(value_expression_item)["value"])
                return left_calculater, value_items
            if self.is_column(right_expression):
                right_column = self.parse_column(right_expression)
                if right_column["table_name"] or right_column["column_name"] not in config["schema"]:
                    raise SyncanySqlCompileException("unkonw having condition: " + self.to_sql(expression))
                config["aggregate"]["having_columns"].add(right_column["column_name"])
                return left_calculater, self.compile_column(right_column)
            calculate_fields = []
            self.parse_calculate(primary_table, right_expression, calculate_fields)
            if [calculate_field for calculate_field in calculate_fields if calculate_field["column_name"] not in config["schema"]]:
                raise SyncanySqlCompileException("unkonw having condition: " + self.to_sql(expression))
            for calculate_field in calculate_fields:
                if calculate_field["column_name"] not in config["schema"]:
                    continue
                config["aggregate"]["having_columns"].add(calculate_field["column_name"])
            return left_calculater, self.compile_calculate(primary_table, right_expression, [])

        if isinstance(expression, sqlglot_expressions.EQ):
            left_calculater, right_calculater = parse(expression)
            return ["@eq", left_calculater, right_calculater]
        elif isinstance(expression, sqlglot_expressions.NEQ):
            left_calculater, right_calculater = parse(expression)
            return ["@neq", left_calculater, right_calculater]
        elif isinstance(expression, sqlglot_expressions.GT):
            left_calculater, right_calculater = parse(expression)
            return ["@gt", left_calculater, right_calculater]
        elif isinstance(expression, sqlglot_expressions.GTE):
            left_calculater, right_calculater = parse(expression)
            return ["@gte", left_calculater, right_calculater]
        elif isinstance(expression, sqlglot_expressions.LT):
            left_calculater, right_calculater = parse(expression)
            return ["@lt", left_calculater, right_calculater]
        elif isinstance(expression, sqlglot_expressions.LTE):
            left_calculater, right_calculater = parse(expression)
            return ["@lte", left_calculater, right_calculater]
        elif isinstance(expression, sqlglot_expressions.In):
            left_calculater, right_calculater = parse(expression)
            return ["@in", left_calculater, right_calculater]
        elif isinstance(expression, sqlglot_expressions.Paren):
            return self.compile_having_condition(primary_table, expression.args.get("this"), config)
        else:
            raise SyncanySqlCompileException("unkonw having condition: " + self.to_sql(expression))

    def compile_order(self, primary_table, expressions, config):
        primary_sort_keys, sort_keys = [], []
        for expression in expressions:
            column = self.parse_column(expression.args["this"])
            if (column["table_name"] and primary_table["table_alias"] == column["table_name"]) or \
                    (not column["table_name"] and column["column_name"] in primary_table["columns"]):
                primary_sort_keys.append([column["column_name"], True if expression.args["desc"] else False])
            sort_keys.append((column["column_name"], True if expression.args["desc"] else False))
        if sort_keys and len(primary_sort_keys) < len(sort_keys):
            if isinstance(config["schema"], dict):
                for sort_key, _ in sort_keys:
                    if sort_key not in config["schema"]:
                        raise SyncanySqlCompileException("unkonw order by key: " + str(sort_key))
            config["pipelines"].append([">>@sort", "$.*|array", False, sort_keys])
        elif primary_sort_keys:
            config["orders"].extend(primary_sort_keys)
        
    def compile_column(self, column, scope_depth=1):
        if column["typing_filters"]:
            typing_filter_column = ("$" * scope_depth) + "." + column["column_name"] + "|" + column["typing_filters"][0]
            if len(column["typing_filters"]) == 1:
                return typing_filter_column

            def compile_column_filter(typing_filters):
                child_filter_column = ":$.*|" + typing_filters[0]
                if len(typing_filters) == 1:
                    return child_filter_column
                return [typing_filter_column, compile_column_filter(typing_filters[1:])]
            return [typing_filter_column, compile_column_filter(column["typing_filters"][1:])]
        return ("$" * scope_depth) + "." + column["column_name"]
    
    def compile_const(self, literal):
        return ["#const", literal["value"]]

    def parse_joins(self, primary_table, config, join_expressions, arguments):
        join_tables = {}
        for join_expression in join_expressions:
            if "alias" not in join_expression.args["this"].args:
                raise SyncanySqlCompileException("join table must be alias name: " + self.to_sql(join_expression))
            name = join_expression.args["this"].args["alias"].args["this"].name
            if isinstance(join_expression.args["this"], sqlglot_expressions.Table):
                table_info = self.parse_table(join_expression.args["this"], config)
                db, table = table_info["db"], table_info["name"]
            elif isinstance(join_expression.args["this"], sqlglot_expressions.Subquery):
                subquery_name, subquery_config = self.compile_subquery(join_expression.args["this"], arguments)
                db, table = "--", subquery_name
                config["dependencys"].append(subquery_config)
            else:
                raise SyncanySqlCompileException("unkonw join table: " + self.to_sql(join_expression))
            if "on" not in join_expression.args:
                raise SyncanySqlCompileException("unkonw join on: " + self.to_sql(join_expression))
            join_table = {
                "db": db, "table": table, "name": name, "primary_keys": [],
                "join_columns": [], "calculate_expressions": [], "querys": {}, "ref_count": 0
            }
            self.parse_on_condition(primary_table, config, join_expression.args["on"], join_table)
            self.parse_condition_typing_filter(join_table)
            if not join_table["primary_keys"] or not join_table["join_columns"]:
                raise SyncanySqlCompileException("empty join table: " + self.to_sql(join_expression))
            join_tables[join_table["name"]] = join_table

        for name, join_table in join_tables.items():
            for join_column in join_table["join_columns"]:
                if not join_column["table_name"] or join_column["table_name"] == primary_table["table_name"]:
                    continue
                if join_column["table_name"] not in join_tables:
                    raise SyncanySqlCompileException("unknown join table: " + join_column["table_name"])
                join_tables[join_column["table_name"]]["ref_count"] += 1
        return join_tables

    def parse_on_condition(self, primary_table, config, expression, join_table):
        if not expression:
            return
        if isinstance(expression, sqlglot_expressions.And):
            self.parse_on_condition(primary_table, config, expression.args.get("this"), join_table)
            self.parse_on_condition(primary_table, config, expression.args.get("expression"), join_table)
            return

        def parse(expression):
            table_expression, value_expression = None, None
            if isinstance(expression, sqlglot_expressions.EQ) and expression.args.get("expression"):
                for arg_expression in (expression.args["this"], expression.args["expression"]):
                    if not isinstance(arg_expression, sqlglot_expressions.Column):
                        if not self.is_column(arg_expression):
                            continue
                        arg_column = self.parse_column(arg_expression)
                        condition_table = arg_column["table_name"]
                    else:
                        condition_table = arg_expression.args["table"].name if "table" in arg_expression.args else None
                    if condition_table and condition_table == join_table["name"]:
                        table_expression = arg_expression
                        break
                if table_expression is None:
                    raise SyncanySqlCompileException("unkonw join on condition: " + self.to_sql(expression))
                value_expression = expression.args["expression"] if table_expression == expression.args["this"] else expression.args["this"]
            elif expression.args.get("expressions"):
                table_expression, value_expression = expression.args["this"], expression.args["expressions"]
            elif expression.args.get("query"):
                table_expression, value_expression = expression.args["this"], expression.args["query"]
            elif isinstance(expression.args["expression"], sqlglot_expressions.Subquery):
                table_expression, value_expression = expression.args["this"], expression.args["expression"].args["this"]
            else:
                table_expression, value_expression = expression.args["this"], expression.args["expression"]

            condition_column = self.parse_column(table_expression)
            if not isinstance(expression, sqlglot_expressions.EQ) or self.is_const(value_expression) \
                    or isinstance(value_expression, list) or isinstance(value_expression, sqlglot_expressions.Select):
                if not self.is_column(table_expression) or not condition_column["table_name"]:
                    raise SyncanySqlCompileException("unkonw join on condition: " + self.to_sql(expression))
                if isinstance(value_expression, sqlglot_expressions.Select):
                    value_column = self.compile_query_condition(primary_table, config, value_expression,
                                                                condition_column["typing_filters"])
                    if isinstance(expression, sqlglot_expressions.In):
                        value_column[1] = [":" + value_column[1], ":$.*|array"]
                    else:
                        value_column[1] = [":" + value_column[1], [":$.*|array", ":$.:0"]]
                    return False, condition_column, value_column
                if isinstance(value_expression, list):
                    value_items = []
                    for value_expression_item in value_expression:
                        if not self.is_const(value_expression_item):
                            raise SyncanySqlCompileException("unkonw join on condition: " + self.to_sql(expression))
                        value_items.append(self.parse_const(value_expression_item)["value"])
                    return False, condition_column, value_items
                calculate_fields = []
                self.parse_calculate(primary_table, value_expression, calculate_fields)
                if calculate_fields:
                    raise SyncanySqlCompileException("unkonw join on condition: " + self.to_sql(expression))
                return False, condition_column, self.compile_calculate(primary_table, value_expression, [])

            if self.is_column(value_expression):
                if condition_column["column_name"] in join_table["primary_keys"]:
                    raise SyncanySqlCompileException("join on primary_key duplicate: " + self.to_sql(expression))
                join_table["join_columns"].append(self.parse_column(value_expression))
                join_table["primary_keys"].append(condition_column["column_name"])
                join_table["calculate_expressions"].append(value_expression)
                return True, condition_column, None
            calculate_fields = []
            self.parse_calculate(primary_table, value_expression, calculate_fields)
            if not calculate_fields and condition_column["table_name"] == join_table["name"]:
                return False, condition_column, self.compile_calculate(primary_table, value_expression, [])
            if condition_column["column_name"] in join_table["primary_keys"]:
                raise SyncanySqlCompileException("join on primary_key duplicate: " + self.to_sql(expression))
            join_table["join_columns"].extend(calculate_fields)
            join_table["primary_keys"].append(condition_column["column_name"])
            join_table["calculate_expressions"].append(value_expression)
            return True, condition_column, None

        if isinstance(expression, sqlglot_expressions.EQ):
            is_column, condition_column, value_column = parse(expression)
            if not is_column:
                if condition_column["typing_name"] not in join_table["querys"]:
                    join_table["querys"][condition_column["typing_name"]] = {}
                join_table["querys"][condition_column["typing_name"]]["=="] = value_column
        elif isinstance(expression, sqlglot_expressions.NEQ):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + self.to_sql(expression))
            if condition_column["typing_name"] not in join_table["querys"]:
                join_table["querys"][condition_column["typing_name"]] = {}
            join_table["querys"][condition_column["typing_name"]]["!="] = value_column
        elif isinstance(expression, sqlglot_expressions.GT):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + self.to_sql(expression))
            if condition_column["typing_name"] not in join_table["querys"]:
                join_table["querys"][condition_column["typing_name"]] = {}
            join_table["querys"][condition_column["typing_name"]][">"] = value_column
        elif isinstance(expression, sqlglot_expressions.GTE):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + self.to_sql(expression))
            if condition_column["typing_name"] not in join_table["querys"]:
                join_table["querys"][condition_column["typing_name"]] = {}
            join_table["querys"][condition_column["typing_name"]][">="] = value_column
        elif isinstance(expression, sqlglot_expressions.LT):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + self.to_sql(expression))
            if condition_column["typing_name"] not in join_table["querys"]:
                join_table["querys"][condition_column["typing_name"]] = {}
            join_table["querys"][condition_column["typing_name"]]["<"] = value_column
        elif isinstance(expression, sqlglot_expressions.LTE):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + self.to_sql(expression))
            if condition_column["typing_name"] not in join_table["querys"]:
                join_table["querys"][condition_column["typing_name"]] = {}
            join_table["querys"][condition_column["typing_name"]]["<="] = value_column
        elif isinstance(expression, sqlglot_expressions.In):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + self.to_sql(expression))
            if condition_column["typing_name"] not in join_table["querys"]:
                join_table["querys"][condition_column["typing_name"]] = {}
            join_table["querys"][condition_column["typing_name"]]["in"] = value_column
        else:
            raise SyncanySqlCompileException("unkonw join on condition: " + self.to_sql(expression))

    def parse_group(self, primary_table, expression, group_fields):
        for group_expression in expression.args["expressions"]:
            self.parse_calculate(primary_table, group_expression, group_fields)

    def parse_aggregate(self, primary_table, expression, calculate_fields):
        if isinstance(expression, sqlglot_expressions.Count):
            self.parse_calculate(primary_table, expression.args["this"], calculate_fields)
        elif isinstance(expression, sqlglot_expressions.Sum):
            self.parse_calculate(primary_table, expression.args["this"], calculate_fields)
        elif isinstance(expression, sqlglot_expressions.Min):
            self.parse_calculate(primary_table, expression.args["this"], calculate_fields)
        elif isinstance(expression, sqlglot_expressions.Max):
            self.parse_calculate(primary_table, expression.args["this"], calculate_fields)
        else:
            raise SyncanySqlCompileException("unkonw aggregate calculate: " + self.to_sql(expression))

    def parse_calculate(self, primary_table, expression, calculate_fields):
        if isinstance(expression, sqlglot_expressions.Anonymous):
            for arg_expression in expression.args.get("expressions", []):
                self.parse_calculate(primary_table, arg_expression, calculate_fields)
        elif isinstance(expression, sqlglot_expressions.If):
            self.parse_calculate(primary_table, expression.args["this"], calculate_fields)
            if expression.args.get("true"):
                self.parse_calculate(primary_table, expression.args["true"], calculate_fields)
            if expression.args.get("false"):
                self.parse_calculate(primary_table, expression.args["false"], calculate_fields)
        elif isinstance(expression, sqlglot_expressions.Case):
            if expression.args.get("this"):
                self.parse_calculate(primary_table, expression.args["this"], calculate_fields)
            if expression.args.get("default"):
                self.parse_calculate(primary_table, expression.args["default"], calculate_fields)
            if "ifs" in expression.args:
                for case_expression in expression.args["ifs"]:
                    self.parse_calculate(primary_table, case_expression, calculate_fields)
        elif isinstance(expression, sqlglot_expressions.Paren):
            self.parse_calculate(primary_table, expression.args["this"], calculate_fields)
        elif self.is_column(expression):
            column = self.parse_column(expression)
            if not column["table_name"] or primary_table["table_name"] == column["table_name"]:
                primary_table["columns"][column["column_name"]] = column
            calculate_fields.append(column)
        elif isinstance(expression, sqlglot_expressions.Star):
            pass
        elif self.is_const(expression):
            pass
        else:
            if "this" in expression.args and expression.args["this"]:
                self.parse_calculate(primary_table, expression.args["this"], calculate_fields)
            if "expression" in expression.args and expression.args["expression"]:
                self.parse_calculate(primary_table, expression.args["expression"], calculate_fields)
            if "expressions" in expression.args and expression.args["expressions"] and isinstance(expression.args["expressions"], list):
                for arg_expression in expression.args.get("expressions", []):
                    self.parse_calculate(primary_table, arg_expression, calculate_fields)

    def parse_table(self, expression, config):
        db_name = expression.args["db"].name if "db" in expression.args and expression.args["db"] else None
        if isinstance(expression.args["this"], sqlglot_expressions.Dot):
            def parse(expression):
                return (parse(expression.args["this"]) if isinstance(expression.args["this"],
                                                                     sqlglot_expressions.Dot) else expression.args["this"].name) \
                       + "." + (parse(expression.args["expression"]) if isinstance(expression.args["expression"],
                                                                                   sqlglot_expressions.Dot) else expression.args["expression"].name)
            table_name = parse(expression.args["this"])
        else:
            table_name = expression.args["this"].name
        origin_name = self.mapping[table_name] if table_name in self.mapping else table_name
        try:
            start_index, end_index = origin_name.index("<"), origin_name.rindex(">")
            table_name = origin_name[:start_index]
            typing_options = origin_name[start_index + 1: end_index].split(",")
        except ValueError:
            table_name = origin_name
            typing_options = []
        if "catalog" in expression.args and expression.args["catalog"]:
            db_name, table_name = expression.args["catalog"].name, ((db_name + ".") if db_name else "") + table_name
        table_alias = expression.args["alias"].args["this"].name if "alias" in expression.args else None

        if table_name == "_":
            db_name = "-"
        elif not db_name:
            if table_name.lower().startswith("file://"):
                from urllib.parse import unquote_plus, urlparse
                try:
                    url_info = urlparse(table_name)
                    url_path = url_info.netloc + url_info.path + (("#" + url_info.fragment) if url_info.fragment else "")
                    db_driver, db_params, path_info = "textline", {}, os.path.split(unquote_plus(url_path))
                    for url_param in unquote_plus(url_info.query).split("&"):
                        url_param = url_param.split("=")
                        if len(url_param) >= 2:
                            db_params[url_param[0]] = url_param[1]
                except:
                    db_driver, db_params, path_info = "textline", {}, os.path.split(table_name[7:])
            else:
                db_driver, db_params, path_info = None, {}, os.path.split(table_name)

            db_driver = {".txt": "textline", ".json": "json", ".csv": "csv", ".xls": "execl",
                         ".xlsx": "execl"}.get(os.path.splitext(path_info[-1])[-1], db_driver)
            if db_driver:
                path, table_name = (os.path.abspath(path_info[0]) if path_info[0] else os.getcwd()), path_info[1]
                path_db_name = "dir__" + "".join([c for c in path if c.isalpha() or c.isdigit() or c == os.path.sep]).replace(os.path.sep, "_")
                if db_params:
                    path_db_name = path_db_name + "_" + str(uuid.uuid1().int)
                database = None
                if "databases" not in config:
                    config["databases"] = []
                for d in config["databases"]:
                    if d["name"] == path_db_name:
                        d.update({"name": path_db_name, "driver": db_driver, "path": path})
                        database = d
                        break
                    if d.get("driver") == database and d.get("path") == path and not db_params:
                        database = d
                        break
                if not database:
                    database = {"name": path_db_name, "driver": db_driver, "path": path}
                    config["databases"].append(database)
                if db_params:
                    database.update(db_params)
                if db_driver == "textline":
                    formats = [f for f in ("csv", "json", "richtable", "print") if f in typing_options]
                    if formats:
                        database["format"] = formats[0]
                db_name = database["name"]
            else:
                db_name = "--"

        return {
            "db": db_name,
            "name": table_name,
            "table_name": table_alias if table_alias else table_name,
            "table_alias": table_alias,
            "origin_name": origin_name,
            "typing_options": typing_options,
        }
        
    def parse_column(self, expression):
        dot_keys = []
        if isinstance(expression, sqlglot_expressions.Dot):
            def parse_dot(dot_expression):
                if isinstance(dot_expression.args["this"], sqlglot_expressions.Column):
                    if dot_expression.args.get("expression"):
                        dot_keys.append(dot_expression.args["expression"].name)
                    return dot_expression.args["this"]
                column_expression = parse_dot(dot_expression.args["this"])
                if dot_expression.args.get("expression"):
                    dot_keys.append(dot_expression.args["expression"].name)
                return column_expression
            expression = parse_dot(expression)

        table_name = expression.args["table"].name if "table" in expression.args else None
        column_name = expression.args["this"].name
        origin_name = self.mapping[column_name] if column_name in self.mapping else column_name
        try:
            start_index, end_index = origin_name.index("["), origin_name.rindex("]")
            typing_filters = origin_name[start_index+1: end_index].split(";")
            column_name = origin_name[:start_index]
            typing_name = (column_name + "|" + typing_filters[0]) if typing_filters else column_name
        except ValueError:
            typing_filters = []
            column_name = origin_name
            typing_name = column_name
        try:
            start_index, end_index = origin_name.index("<"), origin_name.rindex(">")
            typing_options = origin_name[start_index+1: end_index].split(",")
        except ValueError:
            typing_options = []
        return {
            "table_name": table_name,
            "column_name": (column_name + "." + ".".join(dot_keys)) if dot_keys else column_name,
            "origin_name": origin_name,
            "typing_name": typing_name,
            "dot_keys": dot_keys,
            "typing_filters": typing_filters,
            "typing_options": typing_options,
            "expression": expression
        }
    
    def parse_const(self, expression):
        is_neg, typing_filter = False, None
        if isinstance(expression, sqlglot_expressions.Neg):
            is_neg, expression = True, expression.args["this"]
        if isinstance(expression, sqlglot_expressions.Literal):
            value = expression.args["this"]
            if expression.is_number:
                value = int(value) if expression.is_int else float(value)
                if is_neg:
                    value = -value
                typing_filter = "int" if expression.is_int else "float"
        elif isinstance(expression, sqlglot_expressions.Boolean):
            value = expression.args["this"]
            typing_filter = "bool"
        else:
            value = None
        return {
            "value": value,
            "typing_filter": typing_filter,
            "expression": expression
        }

    def parse_condition_typing_filter(self, config):
        for key in list(config["querys"].keys()):
            if "|" in key:
                continue
            typing_filter = None
            for exp_key, exp_value in config["querys"][key].items():
                if isinstance(exp_value, list) and len(exp_value) == 2:
                    if (isinstance(exp_value[0], str) and exp_value[0][:1] == "&") \
                            or (isinstance(exp_value[0], list) and exp_value[0] and isinstance(exp_value[0][0], str)
                                and exp_value[0][0][:1] == "&"):
                        if isinstance(exp_value[1], list) and exp_value:
                            exp_value = exp_value[1][0]
                        if isinstance(exp_value, str):
                            typing_filter = "|".join(exp_value.split("|")[1:])
                            continue

                if exp_key == "in":
                    if not exp_value:
                        continue
                    exp_value = exp_value[0]
                if (isinstance(exp_value, str) and exp_value == "@now") or (isinstance(exp_value, list) and exp_value and exp_value[0] == "@now"):
                    if typing_filter and typing_filter != "datetime":
                        typing_filter = None
                        break
                    typing_filter = "datetime"
                    continue
                if isinstance(exp_value, list) and len(exp_value) == 2 and exp_value[0] == "#const":
                    exp_value = exp_value[1]
                type_name = str(type(exp_value).__name__)
                if type_name in ("int", "float", "bool"):
                    if typing_filter and typing_filter != type_name:
                        typing_filter = None
                        break
                    typing_filter = type_name
            if typing_filter:
                config["querys"][key + "|" + typing_filter] = config["querys"][key]
                config["querys"].pop(key)

    def parse_mapping(self, sql):
        parser = SqlParser(sql)
        segments = []
        last_start_index, last_end_index = 0, 0

        def get_allow_segment(segment):
            allow_segment = []
            for c in segment:
                if c.isalpha() or c.isdigit() or c == "_":
                    allow_segment.append(c)
                    continue
                break
            return "".join(allow_segment)

        while True:
            try:
                start_index, end_index, segment = parser.read_segment()
                allow_segment = get_allow_segment(segment)
                if allow_segment != segment:
                    allow_segment = allow_segment + "_" + str(uuid.uuid1().int)
                    self.mapping[allow_segment] = segment
                    segment = allow_segment
                segments.append(parser.sql[last_end_index: start_index])
                segments.append(segment)
                last_start_index, last_end_index = start_index, end_index
            except EOFError:
                segments.append(parser.sql[last_end_index:])
                break
        return "".join(segments)

    def is_column(self, expression):
        if isinstance(expression, sqlglot_expressions.Column):
            return True
        if not isinstance(expression, sqlglot_expressions.Dot):
            return False

        def parse_dot(dot_expression):
            if isinstance(dot_expression.args["this"], sqlglot_expressions.Column):
                return True
            if not isinstance(dot_expression.args["this"], sqlglot_expressions.Dot):
                return False
            return parse_dot(dot_expression.args["this"])
        return parse_dot(expression)

    def is_const(self, expression):
        return isinstance(expression, (sqlglot_expressions.Neg, sqlglot_expressions.Literal, sqlglot_expressions.Boolean,
                                       sqlglot_expressions.Null))

    def to_sql(self, expression_sql):
        if not isinstance(expression_sql, str):
            expression_sql = str(expression_sql)
        parser = SqlParser(expression_sql)
        segments = []
        last_start_index, last_end_index = 0, 0
        while True:
            try:
                start_index, end_index, segment = parser.read_segment()
                if segment in self.mapping:
                    segment = self.mapping[segment]
                segments.append(parser.sql[last_end_index: start_index])
                segments.append(segment)
                last_start_index, last_end_index = start_index, end_index
            except EOFError:
                segments.append(parser.sql[last_end_index:])
                break
        return "".join(segments)
