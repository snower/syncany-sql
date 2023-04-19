# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os
import copy
import uuid
from sqlglot import maybe_parse, ParseError
from sqlglot import expressions as sqlglot_expressions
from sqlglot.dialects import Dialect
from sqlglot import tokens
from .errors import SyncanySqlCompileException
from .calculaters import is_mysql_func, find_aggregate_calculater, CalculaterUnknownException
from .config import CONST_CONFIG_KEYS
from .parser import SqlParser
from .taskers.delete import DeleteTasker
from .taskers.query import QueryTasker, DEAULT_AGGREGATE
from .taskers.into import IntoTasker
from .taskers.explain import ExplainTasker
from .taskers.set import SetCommandTasker
from .taskers.execute import ExecuteTasker
from .taskers.use import UseCommandTasker
from .taskers.show import ShowCommandTasker


class EnvVariableGetter(object):
    def __init__(self, env_variables, key):
        self.env_variables = env_variables
        self.key = key

    def get(self, key):
        return self.env_variables.get_value(key)

    def __deepcopy__(self, memodict=None):
        return self

    def __copy__(self):
        return self


class CompilerDialect(Dialect):
    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"']
        COMMENTS = ["--", "#", ("/*", "*/")]
        IDENTIFIERS = ["`"]
        ESCAPES = ["'", "\\"]
        BIT_STRINGS = [("b'", "'"), ("B'", "'"), ("0b", "")]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", "")]


class Compiler(object):
    ESCAPE_CHARS = ['\\\\a', '\\\\b', '\\\\f', '\\\\n', '\\\\r', '\\\\t', '\\\\v', '\\\\0']

    def __init__(self, config, env_variables):
        self.config = config
        self.env_variables = env_variables
        self.mapping = {}

    def compile(self, sql, arguments):
        escape_sql = sql
        if sql[:4].lower() not in ("set ", "use ") and "\\\\" in sql:
            for escape_char in self.ESCAPE_CHARS:
                escape_sql = escape_sql.replace(escape_char, "\\\\\\" + escape_char)
        try:
            expression = maybe_parse(self.parse_mapping(escape_sql), dialect=CompilerDialect)
            return self.compile_expression(expression, arguments)
        except ParseError as e:
            if not e.errors:
                raise e
            error_info = e.errors[0]
            if not error_info or not error_info.get("description") or not error_info.get("line") \
                    or not error_info.get("col"):
                raise e
            sql_lines = sql.split("\n")
            if len(sql_lines) <= error_info["line"] - 1:
                raise e
            sql_lines[error_info["line"] - 1] = sql_lines[error_info["line"] - 1][error_info["col"] - 1:]
            error_sql = "\n".join(sql_lines)
            raise ParseError('syntax error "%s" near "%s"' % (error_info["description"], error_sql), [error_info]) from None

    def compile_expression(self, expression, arguments):
        if isinstance(expression, sqlglot_expressions.Delete):
            return DeleteTasker(self.compile_delete(expression, arguments))
        elif isinstance(expression, (sqlglot_expressions.Union, sqlglot_expressions.Insert, sqlglot_expressions.Select)):
            query_tasker = QueryTasker(self.compile_query(expression, arguments))
            if not expression.args.get("into"):
                return query_tasker
            if not isinstance(expression, sqlglot_expressions.Select):
                raise SyncanySqlCompileException('unknown into, related sql "%s"' % self.to_sql(expression))
            return IntoTasker(query_tasker, self.compile_select_into(expression.args.get("into"), arguments))
        elif isinstance(expression, sqlglot_expressions.Command):
            if expression.args["this"].lower() == "explain" and self.is_const(expression.args["expression"], {}, arguments):
                return ExplainTasker(self.compile(self.parse_const(expression.args["expression"], {}, arguments)["value"], arguments))
            if expression.args["this"].lower() == "set" and self.is_const(expression.args["expression"], {}, arguments):
                value = self.parse_const(expression.args["expression"], {}, arguments)["value"].split("=")
                config = {"key": value[0].strip(), "value": "=".join(value[1:]).strip()}
                return SetCommandTasker(config)
            if expression.args["this"].lower() == "execute" and self.is_const(expression.args["expression"], {}, arguments):
                filename = self.parse_const(expression.args["expression"], {}, arguments)["value"]
                if filename and filename[0] == '`':
                    filename = filename[1:-1]
                if filename in self.mapping:
                    filename = self.mapping[filename]
                return ExecuteTasker({"filename": filename})
            if expression.args["this"].lower() == "show" and self.is_const(expression.args["expression"], {}, arguments):
                return ShowCommandTasker({"key": self.parse_const(expression.args["expression"], {}, arguments)["value"]})
        elif isinstance(expression, sqlglot_expressions.Use):
            use_info = expression.args["this"].args["this"].name
            if use_info in self.mapping:
                use_info = self.mapping[use_info]
            return UseCommandTasker({"use": use_info})
        raise SyncanySqlCompileException('unknown sql "%s"' % self.to_sql(expression))

    def compile_delete(self, expression, arguments):
        config = copy.deepcopy(self.config)
        config.update({
            "input": "&.--.__subquery_null_" + str(uuid.uuid1().int) + "::id",
            "output": "&.-.&1::id",
            "querys": {},
            "schema": "$.*",
        })

        table_info = self.parse_table(expression.args["this"], config, arguments)
        where_expression = expression.args.get("where")
        if where_expression and isinstance(where_expression, sqlglot_expressions.Where):
            self.compile_where_condition(where_expression.args["this"], config, arguments, table_info)
            self.parse_condition_typing_filter(expression, config, arguments)
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
            raise SyncanySqlCompileException('unknown sql "%s"' % self.to_sql(expression))
        return config

    def compile_subquery(self, expression, arguments):
        if not isinstance(expression, sqlglot_expressions.Subquery):
            table_name = "anonymous"
        else:
            table_name = expression.args["alias"].args["this"].name if "alias" in expression.args \
                                                                       and expression.args["alias"] else "anonymous"
        subquery_name = "__subquery_" + str(uuid.uuid1().int) + "_" + table_name
        subquery_arguments = {key: arguments[key] for key in CONST_CONFIG_KEYS if key in arguments}
        subquery_config = self.compile_query(expression if not isinstance(expression, sqlglot_expressions.Subquery)
                                             else expression.args["this"], subquery_arguments)
        subquery_config["output"] = "&.--." + subquery_name + "::" + subquery_config["output"].split("::")[-1]
        subquery_config["name"] = subquery_config["name"] + "#" + subquery_name[2:]
        arguments.update({subquery_config["name"] + "@" + key: value for key, value in subquery_arguments.items()})
        arguments["@primary_order"] = False
        return subquery_name, subquery_config

    def compile_insert_into(self, expression, config, arguments):
        columns = []
        if isinstance(expression.args["this"], sqlglot_expressions.Table):
            table_info = self.parse_table(expression.args["this"], config, arguments)
        elif isinstance(expression.args["this"], sqlglot_expressions.Schema):
            schema_expression = expression.args["this"]
            if not isinstance(schema_expression.args["this"], sqlglot_expressions.Table):
                raise SyncanySqlCompileException('unknown insert into table, related sql "%s"' % self.to_sql(expression))
            if not schema_expression.args.get("expressions"):
                raise SyncanySqlCompileException('unknown insert into columns, related sql "%s"' % self.to_sql(expression))
            table_info = self.parse_table(schema_expression.args["this"], config, arguments)
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
            raise SyncanySqlCompileException('unkonw insert into, related sql "%s"' % self.to_sql(expression))
        if not expression.args.get("expression"):
            raise SyncanySqlCompileException('unkonw insert into, related sql "%s"' % self.to_sql(expression))

        if table_info["db"] == "-" and table_info["name"] == "_":
            config["output"] = "&.-.&1::id use I"
        else:
            update_type = "I"
            if table_info["typing_options"]:
                for typing_option in table_info["typing_options"]:
                    if typing_option.upper() not in ("I", "UI", "UDI", "DI"):
                        continue
                    update_type = typing_option
                    break
            config["output"] = "".join(["&.", table_info["db"], ".", table_info["name"], "::",
                                        "id", " use ", update_type])

        if isinstance(expression.args["expression"], (sqlglot_expressions.Select, sqlglot_expressions.Union)):
            select_expression = expression.args["expression"]
            if isinstance(select_expression, sqlglot_expressions.Union):
                self.compile_union(select_expression, config, arguments)
            else:
                self.compile_select(select_expression, config, arguments)
        elif isinstance(expression.args["expression"], sqlglot_expressions.Values):
            values_expression = expression.args["expression"]
            if not values_expression.args.get("expressions"):
                raise SyncanySqlCompileException('unkonw insert into, related sql "%s"' % self.to_sql(expression))
            datas = []
            for data_expression in values_expression.args["expressions"]:
                data = {}
                for i in range(len(data_expression.args["expressions"])):
                    value_expression = data_expression.args["expressions"][i]
                    value = self.parse_const(value_expression, config, arguments)["value"]
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
            raise SyncanySqlCompileException('unkonw insert into, related sql "%s"' % self.to_sql(expression))

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
            if isinstance(subquery_config["schema"], dict):
                if not isinstance(config["schema"], dict):
                    config["schema"] = {}
                for name in subquery_config["schema"]:
                    config["schema"][name] = "$." + name
            config["dependencys"].append(subquery_config)
        config["input"] = "&.--." + query_name + "::" + config["dependencys"][0]["output"].split("::")[-1].split(" ")[0]
        config["output"] = config["output"].split("::")[0] + "::" + config["input"].split("::")[-1].split(" ")[0]
        arguments["@primary_order"] = False
        arguments["@limit"] = 0
        arguments["@batch"] = 0

    def compile_select(self, expression, config, arguments):
        primary_table = {"db": None, "name": None, "table_name": None, "table_alias": None, "seted_primary_keys": False,
                         "loader_primary_keys": [], "outputer_primary_keys": [], "columns": {}, "subquery": None}

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
                raise SyncanySqlCompileException('unknown select table, related sql "%s"' % self.to_sql(expression))
            if len(from_expression.expressions) > 1:
                raise SyncanySqlCompileException('error select table, only select from one table, related sql "%s"'
                                                 % self.to_sql(expression))
            from_expression = from_expression.expressions[0]
            if isinstance(from_expression, sqlglot_expressions.Table):
                table_info = self.parse_table(from_expression, config, arguments)
                primary_table["db"] = table_info["db"]
                primary_table["name"] = table_info["name"]
                primary_table["table_alias"] = table_info["table_alias"]
                primary_table["table_name"] = table_info["table_name"]
            elif isinstance(from_expression, sqlglot_expressions.Subquery):
                if "alias" not in from_expression.args:
                    raise SyncanySqlCompileException('error subquery, must have an alias name, related sql "%s"'
                                                     % self.to_sql(expression))
                primary_table["table_alias"] = from_expression.args["alias"].args["this"].name \
                    if "alias" in from_expression.args and from_expression.args["alias"] else None
                primary_table["table_name"] = primary_table["table_alias"]
                subquery_name, subquery_config = self.compile_subquery(from_expression, arguments)
                primary_table["db"] = "--"
                primary_table["name"] = subquery_name
                primary_table["subquery"] = subquery_config
                config["dependencys"].append(subquery_config)
                if self.compile_pipleline_select(expression, config, arguments, primary_table):
                    return config
                if not primary_table["table_alias"]:
                    raise SyncanySqlCompileException('error subquery, must have an alias name, related sql "%s"'
                                                     % self.to_sql(expression))
            else:
                raise SyncanySqlCompileException('unknown select table, related sql "%s"' % self.to_sql(expression))

        join_tables = self.parse_joins(expression, config, arguments, primary_table, expression.args["joins"]) \
            if "joins" in expression.args and expression.args["joins"] else {}
        group_expression = expression.args.get("group")

        select_expressions = expression.args.get("expressions")
        if not select_expressions:
            raise SyncanySqlCompileException('unknown select columns, related sql "%s"' % self.to_sql(expression))
        config["schema"] = {}
        for select_expression in select_expressions:
            if isinstance(select_expression, sqlglot_expressions.Star):
                if not self.compile_select_star_column(select_expression, config, arguments, primary_table, join_tables):
                    config["schema"] = "$.*"
                continue
            elif isinstance(select_expression, sqlglot_expressions.Column) \
                    and isinstance(select_expression.args.get("this"), sqlglot_expressions.Star):
                if not self.compile_select_star_column(select_expression, config, arguments, primary_table, join_tables):
                    raise SyncanySqlCompileException('unknown select * columns, related sql "%s"' % self.to_sql(expression))
                continue
            if not isinstance(config["schema"], dict):
                raise SyncanySqlCompileException('unknown select * columns, related sql "%s"' % self.to_sql(expression))

            column_expression, aggregate_expression, calculate_expression, column_alias = None, None, None, None
            if self.is_column(select_expression, config, arguments):
                column_expression = select_expression
            elif isinstance(select_expression, sqlglot_expressions.Alias):
                column_alias = select_expression.args["alias"].name if "alias" in select_expression.args else None
                if column_alias and column_alias in self.mapping:
                    column_alias = self.mapping[column_alias]
                if self.is_const(select_expression.args["this"], config, arguments):
                    const_info = self.parse_const(select_expression.args["this"], config, arguments)
                    config["schema"][column_alias] = self.compile_const(select_expression.args["this"], config, arguments, const_info)
                    continue
                elif self.is_column(select_expression.args["this"], config, arguments):
                    column_expression = select_expression.args["this"]
                elif self.is_aggregate(select_expression.args["this"], config, arguments):
                    aggregate_expression = select_expression.args["this"]
                else:
                    calculate_expression = select_expression.args["this"]
            elif self.is_const(select_expression, config, arguments):
                const_info = self.parse_const(select_expression, config, arguments)
                column_alias = str(select_expression)
                config["schema"][column_alias] = self.compile_const(select_expression, config, arguments, const_info)
                continue
            elif self.is_aggregate(select_expression, config, arguments):
                column_alias = str(select_expression)
                aggregate_expression = select_expression
            elif self.is_calculate(select_expression, config, arguments):
                column_alias = str(select_expression)
                calculate_expression = select_expression
            else:
                raise SyncanySqlCompileException('error select column, must have an alias name, related sql "%s"'
                                                 % self.to_sql(expression))
            if column_expression:
                self.compile_select_column(column_expression, config, arguments, primary_table, column_alias,
                                           self.parse_column(column_expression, config, arguments), join_tables)
                continue
            if aggregate_expression:
                self.compile_aggregate_column(aggregate_expression, config, arguments, primary_table, column_alias,
                                              group_expression, join_tables)
                continue

            calculate_fields = []
            self.parse_calculate(calculate_expression, config, arguments, primary_table, calculate_fields)
            calculate_table_names = set([])
            for calculate_field in calculate_fields:
                if not calculate_field["table_name"] or calculate_field["table_name"] == primary_table["table_name"]:
                    continue
                if calculate_field["table_name"] not in join_tables:
                    raise SyncanySqlCompileException('error select column, join table %s is unknown, related sql "%s"'
                                                     % (calculate_field["table_name"], self.to_sql(expression)))
                calculate_table_names.add(calculate_field["table_name"])
            if calculate_table_names:
                column_join_tables = []
                self.compile_join_column_tables(calculate_expression, config, arguments, primary_table,
                                                [join_tables[calculate_table_name] for calculate_table_name in calculate_table_names],
                                                join_tables, column_join_tables)
                calculate_column = self.compile_calculate(calculate_expression, config, arguments, primary_table, column_join_tables)
                config["schema"][column_alias] = self.compile_join_column(calculate_expression, config, arguments, primary_table, 
                                                                          calculate_column, column_join_tables)
            else:
                config["schema"][column_alias] = self.compile_calculate(calculate_expression, config, arguments, primary_table, [])
            if not primary_table["seted_primary_keys"] and not primary_table["outputer_primary_keys"] and column_alias.isidentifier():
                loader_primary_keys = [calculate_field["column_name"] for calculate_field in calculate_fields
                                       if calculate_field["column_name"].isidentifier() and
                                       (not calculate_field["table_name"] or calculate_field["table_name"] == primary_table["table_name"])]
                if loader_primary_keys:
                    primary_table["loader_primary_keys"], primary_table["outputer_primary_keys"] = loader_primary_keys, [column_alias]

        distinct_expression = expression.args.get("distinct")
        if distinct_expression and not config.get("aggregate", {}).get("distinct_keys"):
            if not isinstance(config["schema"], dict):
                raise SyncanySqlCompileException('error distinct, select columns is unknown, related sql "%s"' % self.to_sql(expression))
            if "aggregate" not in config:
                config["aggregate"] = copy.deepcopy(DEAULT_AGGREGATE)
            for name, column in config["schema"].items():
                if name in config["aggregate"]["schema"]:
                    continue
                config["aggregate"]["distinct_keys"].append(copy.deepcopy(column))

        where_expression = expression.args.get("where")
        if where_expression and isinstance(where_expression, sqlglot_expressions.Where):
            self.compile_where_condition(where_expression.args["this"], config, arguments, primary_table)
            self.parse_condition_typing_filter(expression, config, arguments)

        having_expression = expression.args.get("having")
        if having_expression:
            config["intercepts"].append(self.compile_having_condition(having_expression.args["this"], config, arguments,
                                                                      primary_table))
            if config.get("aggregate") and config["aggregate"].get("schema") and config["aggregate"].get("having_columns"):
                if len({True if having_column in config["aggregate"]["schema"] else False
                        for having_column in config["aggregate"]["having_columns"]}) != 1:
                    raise SyncanySqlCompileException('error having condition, cannot contain the values before and after the aggregate calculation at the same time, related sql "%s"'
                                                     % self.to_sql(expression))

        limit_expression = expression.args.get("limit")
        if limit_expression:
            arguments["@limit"] = int(limit_expression.args["expression"].args["this"])

        order_expression = expression.args.get("order")
        if order_expression:
            self.compile_order(order_expression.args["expressions"], config, arguments, primary_table)

        if group_expression and ("aggregate" not in config or not config["aggregate"] or not config["aggregate"]["schema"]):
            self.compile_group_column(group_expression, config, arguments, primary_table, join_tables)
        if not from_expression and not primary_table["outputer_primary_keys"] and isinstance(config["schema"], dict):
            for column_alias in config["schema"]:
                if not column_alias.isidentifier():
                    continue
                primary_table["outputer_primary_keys"] = [column_alias]
                break
        config["input"] = "".join(["&.", primary_table["db"], ".", primary_table["name"], "::",
                                   "+".join(primary_table["loader_primary_keys"]) if primary_table["loader_primary_keys"] else "id"])
        config["output"] = "".join([config["output"].split("::")[0], "::",
                                    "+".join(primary_table["outputer_primary_keys"]) if primary_table["outputer_primary_keys"] else "id",
                                    (" use " + config["output"].split(" use ")[-1]) if " use " in config["output"] else " use I"])

    def compile_pipleline_select(self, expression, config, arguments, primary_table):
        select_expressions = expression.args.get("expressions")
        if not select_expressions or len(select_expressions) != 1 or not isinstance(select_expressions[0], sqlglot_expressions.Anonymous):
            return None
        if expression.args.get("group") or expression.args.get("where") or expression.args.get("having") \
                or expression.args.get("order") or expression.args.get("limit"):
            return None
        calculate_fields = []
        self.parse_calculate(select_expressions[0], config, arguments, primary_table, calculate_fields)
        if calculate_fields:
            return None
        pipeline = self.compile_calculate(select_expressions[0], config, arguments, primary_table, [])
        if isinstance(pipeline, str):
            pipeline = [">>$.*|array", ":" + pipeline]
        else:
            pipeline[0] = ">>" + pipeline[0]
            pipeline = pipeline[:1] + ["$.*|array"] + pipeline[1:]
        config["pipelines"].append(pipeline)
        config["input"] = "".join(["&.", primary_table["db"], ".", primary_table["name"], "::",
                                   "+".join(primary_table["loader_primary_keys"]) if primary_table["loader_primary_keys"] else "id"])
        config["output"] = "".join([config["output"].split("::")[0], "::",
                                    "+".join(primary_table["outputer_primary_keys"]) if primary_table["outputer_primary_keys"] else "id",
                                    (" use " + config["output"].split(" use ")[-1]) if " use " in config["output"] else " use I"])
        arguments["@primary_order"] = False
        arguments["@limit"] = 0
        arguments["@batch"] = 0
        return config

    def compile_select_into(self, expression, arguments):
        config = {"variables": []}
        if not isinstance(expression.args["this"], sqlglot_expressions.Table):
            raise SyncanySqlCompileException('unknown select into variable, related sql "%s"' % self.to_sql(expression))
        if not isinstance(expression.args["this"].args["this"], sqlglot_expressions.Parameter):
            raise SyncanySqlCompileException('unknown select into variable, related sql "%s"' % self.to_sql(expression))
        if not isinstance(expression.args["this"].args["this"].args["this"], sqlglot_expressions.Var):
            raise SyncanySqlCompileException('unknown select into variable, related sql "%s"' % self.to_sql(expression))
        config["variables"].append("@" + expression.args["this"].args["this"].args["this"].args["this"])
        return config

    def compile_select_star_column(self, expression, config, arguments, primary_table, join_tables):
        if isinstance(expression, sqlglot_expressions.Star):
            subquery_config = primary_table["subquery"]
            table_name = primary_table["table_alias"] or primary_table["table_name"]
        else:
            table_name = expression.args["table"].name
            if table_name == primary_table["table_alias"]:
                subquery_config = primary_table["subquery"]
            else:
                subquery_config = join_tables[table_name]["subquery"] if table_name in join_tables else None
        if not subquery_config or not isinstance(subquery_config["schema"], dict):
            return False

        for name, column in subquery_config["schema"].items():
            if name in config["schema"]:
                continue
            column_info = {
                "table_name": table_name,
                "column_name": name,
                "origin_name": name,
                "typing_name": "",
                "dot_keys": [],
                "typing_filters": [],
                "typing_options": [],
                "expression": expression
            }
            self.compile_select_column(expression, config, arguments, primary_table, name, column_info, join_tables)
        return True

    def compile_select_column(self, expression, config, arguments, primary_table, column_alias, column_info, join_tables):
        if not column_alias:
            column_alias = column_info["column_name"].replace(".", "_")
            if column_info["table_name"] and column_alias in config["schema"]:
                column_alias = column_info["table_name"] + "." + column_info["column_name"].replace(".", "_")
        if column_info["table_name"] and column_info["table_name"] != primary_table["table_name"]:
            if column_info["table_name"] not in join_tables:
                raise SyncanySqlCompileException('error join, table %s is unknown, related sql "%s"' %
                                                 (column_info["table_name"], self.to_sql(expression)))
            column_join_tables = []
            self.compile_join_column_tables(expression, config, arguments, primary_table,
                                            [join_tables[column_info["table_name"]]], join_tables,
                                            column_join_tables)
            config["schema"][column_alias] = self.compile_join_column(expression, config, arguments, primary_table, 
                                                                      self.compile_column(expression, config, arguments, 
                                                                                          column_info), column_join_tables)
        else:
            config["schema"][column_alias] = self.compile_column(expression, config, arguments, column_info)
        if not column_info["table_name"] or column_info["table_name"] == primary_table["table_name"]:
            primary_table["columns"][column_info["column_name"]] = column_info

        if not column_info["column_name"].isidentifier() or not column_alias.isidentifier():
            return None
        if "pk" in column_info["typing_options"]:
            if not primary_table["seted_primary_keys"]:
                primary_table["loader_primary_keys"], primary_table["outputer_primary_keys"], primary_table["seted_primary_keys"] = [], [], True
            primary_table["loader_primary_keys"].append(column_info["column_name"])
            primary_table["outputer_primary_keys"].append(column_alias)
        elif not primary_table["seted_primary_keys"] and not primary_table["outputer_primary_keys"]:
            if not column_info["table_name"] or column_info["table_name"] == primary_table["table_name"]:
                primary_table["loader_primary_keys"], primary_table["outputer_primary_keys"] = [column_info["column_name"]], [column_alias]

    def compile_join_column_tables(self, expression, config, arguments, primary_table, current_join_tables, join_tables,
                                   column_join_tables):
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
        self.compile_join_column_tables(expression, config, arguments, primary_table,
                                        [join_tables[column_join_name] for column_join_name in column_join_names
                                         if column_join_name != primary_table["table_name"]], join_tables, column_join_tables)

    def compile_join_column(self, expression, config, arguments, primary_table, column, column_join_tables):
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
                join_columns = self.compile_calculate(join_table["calculate_expressions"][0], config, arguments, primary_table, column_join_tables, i)
            else:
                join_columns = [self.compile_calculate(calculate_expression, config, arguments, primary_table, column_join_tables, i)
                               for calculate_expression in join_table["calculate_expressions"]]
            join_db_table = "&." + join_table["db"] + "." + join_table["table"] + "::" + "+".join(join_table["primary_keys"])
            if join_table["querys"]:
                join_db_table = [join_db_table, join_table["querys"]]
            column = [join_columns, join_db_table, column]
        return column

    def compile_join_column_field(self, expression, config, arguments, primary_table, ci, join_column, column_join_tables):
        if not join_column["table_name"] or join_column["table_name"] == primary_table["table_name"]:
            return self.compile_column(expression, config, arguments, join_column, len(column_join_tables) - ci)
        ji = [j for j in range(len(column_join_tables)) if join_column["table_name"] == column_join_tables[j]["name"]][0]
        return self.compile_column(expression, config, arguments, join_column, ji - ci)

    def compile_where_condition(self, expression, config, arguments, primary_table):
        if not expression:
            return
        if isinstance(expression, sqlglot_expressions.And):
            self.compile_where_condition(expression.args.get("this"), config, arguments, primary_table)
            self.compile_where_condition(expression.args.get("expression"), config, arguments, primary_table)
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
            if not self.is_column(table_expression, config, arguments):
                raise SyncanySqlCompileException('unknown where condition, related sql "%s"' % self.to_sql(expression))
            condition_table = table_expression.args["table"].name if "table" in table_expression.args else None
            if condition_table and condition_table != primary_table["table_name"]:
                raise SyncanySqlCompileException('error where condition, Only the primary table query conditions can be added, related sql "%s"'
                                                 % self.to_sql(expression))

            condition_column = self.parse_column(table_expression, config, arguments)
            if isinstance(value_expression, (sqlglot_expressions.Select, sqlglot_expressions.Subquery, sqlglot_expressions.Union)):
                value_column = self.compile_query_condition(value_expression, config, arguments, primary_table,
                                                            condition_column["typing_filters"])
                if isinstance(expression, sqlglot_expressions.In):
                    value_column = ["@convert_array", value_column]
                else:
                    value_column = ["@convert_array", value_column, ":$.:0"]
            elif isinstance(value_expression, list):
                value_column = []
                for value_expression_item in value_expression:
                    if not self.is_const(value_expression_item, config, arguments):
                        raise SyncanySqlCompileException('error where condition, array must be const value, related sql "%s"' % self.to_sql(expression))
                    value_column.append(self.parse_const(value_expression_item, config, arguments)["value"])
            else:
                calculate_fields = []
                self.parse_calculate(value_expression, config, arguments, primary_table, calculate_fields)
                if calculate_fields:
                    raise SyncanySqlCompileException('error where condition, the value of other tables when the query condition cannot, related sql "%s"'
                                                     % self.to_sql(expression))
                value_column = self.compile_calculate(value_expression, config, arguments, primary_table, [])
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
            raise SyncanySqlCompileException('error where condition, only "=,!=,>,>=,<,<=,in" operations are supported, related sql "%s"'
                                             % self.to_sql(expression))

    def compile_query_condition(self, expression, config, arguments, primary_table, typing_filters):
        if isinstance(expression, sqlglot_expressions.Select):
            select_expressions = expression.args.get("expressions")
            if not select_expressions or len(select_expressions) != 1 or not self.is_column(select_expressions[0], config, arguments):
                raise SyncanySqlCompileException('error subquery, there must be only one query field, related sql "%s"'
                                                 % self.to_sql(expression))
            if expression.args.get("group") or expression.args.get("having") \
                    or expression.args.get("order") or expression.args.get("limit") or expression.args.get("distinct"):
                is_subquery = True
            else:
                is_subquery = False
        else:
            select_expressions, is_subquery = None, True

        if is_subquery:
            subquery_name, subquery_config = self.compile_subquery(expression, arguments)
            if not isinstance(subquery_config, dict):
                raise SyncanySqlCompileException('error subquery, unknown select columns, related sql "%s"' % self.to_sql(expression))
            if isinstance(subquery_config["schema"], dict):
                subquery_schema = subquery_config["schema"]
            else:
                subquery_schema = {}
                for dependency in subquery_config.get("dependencys", []):
                    if not isinstance(dependency, dict) or not isinstance(dependency["schema"], dict):
                        continue
                    subquery_schema.update(dependency["schema"])
            if len(subquery_schema) != 1:
                raise SyncanySqlCompileException('error subquery, there must be only one query field, related sql "%s"'
                                                 % self.to_sql(expression))
            column_name = list(subquery_schema.keys())[0]
            db_table = "&.--." + subquery_name + "::" + column_name
            config["dependencys"].append(subquery_config)
            return [db_table, "$." + column_name]

        if not select_expressions:
            raise SyncanySqlCompileException('error subquery, there must be only one query field, related sql "%s"'
                                             % self.to_sql(expression))
        from_expression = expression.args.get("from")
        if not isinstance(from_expression, sqlglot_expressions.From) or not from_expression.expressions:
            raise SyncanySqlCompileException('error subquery, unknown select from table, related sql "%s"'
                                             % self.to_sql(expression))
        if len(from_expression.expressions) > 1:
            raise SyncanySqlCompileException('error subquery, there must be only one select from table, related sql "%s"'
                                             % self.to_sql(expression))
        table_info = self.parse_table(from_expression.expressions[0], config, arguments)
        column_info = self.parse_column(select_expressions[0], config, arguments)
        if column_info["typing_filters"] and typing_filters:
            column_info["typing_filters"] = typing_filters
        querys = {"querys": {}}
        where_expression = expression.args.get("where")
        if where_expression and isinstance(where_expression, sqlglot_expressions.Where):
            self.compile_where_condition(where_expression.args["this"], querys, arguments, primary_table)
            self.parse_condition_typing_filter(expression, querys, arguments)
        db_table = "&." + table_info["db"] + "." + table_info["name"] + "::" + column_info["column_name"]
        if querys.get("querys"):
            return [[db_table, querys["querys"]], self.compile_column(select_expressions[0], config, arguments, column_info)]
        return [db_table, self.compile_column(select_expressions[0], config, arguments, column_info)]

    def compile_group_column(self, expression, config, arguments, primary_table, join_tables):
        column_alias = None
        if primary_table["outputer_primary_keys"]:
            if isinstance(config["schema"], dict) and primary_table["outputer_primary_keys"][0] in config["schema"]:
                column_alias = primary_table["outputer_primary_keys"][0]
        if not column_alias:
            if isinstance(config["schema"], dict) and config["schema"]:
                column_alias = list(config["schema"].keys())[0]
            else:
                raise SyncanySqlCompileException('error group by, unknown column, related sql "%s"' % self.to_sql(expression))
        group_column = self.compile_aggregate_key(expression, config, arguments, primary_table, join_tables)
        if "aggregate" not in config:
            config["aggregate"] = copy.deepcopy(DEAULT_AGGREGATE)
        aggregate_column = {
            "key": group_column,
            "value": config["schema"][column_alias],
            "calculate": "$$.value",
            "aggregate": [":#aggregate", "$.key", "$$.value"],
            "reduce": "$$." + column_alias,
            "final_value": None
        }
        config["schema"][column_alias] = ["#make", {
            "key": copy.deepcopy(group_column),
            "value": copy.deepcopy(config["schema"][column_alias])
        }, [":#aggregate", "$.key", "$$.value"]]
        config["aggregate"]["key"] = copy.deepcopy(group_column)
        config["aggregate"]["schema"][column_alias] = aggregate_column

    def compile_aggregate_column(self, expression, config, arguments, primary_table, column_alias, group_expression, join_tables):
        group_column = self.compile_aggregate_key(group_expression, config, arguments, primary_table, join_tables)
        is_distinct = False
        if isinstance(expression, sqlglot_expressions.Anonymous):
            if expression.args.get("expressions") and isinstance(expression.args["expressions"][0], sqlglot_expressions.Distinct):
                is_distinct = True
                value_expressions = expression.args["expressions"][0].args.get("expressions")
            else:
                value_expressions = expression.args.get("expressions")
        else:
            if isinstance(expression.args["this"], sqlglot_expressions.Distinct):
                is_distinct = True
                value_expressions = expression.args.get("this").args.get("expressions")
            else:
                value_expressions = [expression.args.get("this")]
        value_column = self.compile_aggregate_value(expression, config, arguments, primary_table, join_tables,
                                                    value_expressions)

        if "aggregate" not in config:
            config["aggregate"] = copy.deepcopy(DEAULT_AGGREGATE)
        if is_distinct and value_column:
            if len(value_column) == 2:
                config["aggregate"]["distinct_keys"].append(copy.deepcopy(value_column[1]))
            else:
                config["aggregate"]["distinct_keys"].extend(copy.deepcopy(value_column[1:]))
            config["aggregate"]["distinct_aggregates"].add(column_alias)
        if value_column and len(value_column) == 2:
            value_column = value_column[1]

        aggregate_column = self.compile_aggregate(expression, config, arguments, column_alias, group_column, value_column)
        config["schema"][column_alias] = ["#make", {
            "key": copy.deepcopy(group_column),
            "value": copy.deepcopy(value_column),
        }, copy.deepcopy(aggregate_column["aggregate"])]
        config["aggregate"]["key"] = copy.deepcopy(aggregate_column["key"])
        config["aggregate"]["schema"][column_alias] = aggregate_column

    def compile_aggregate_key(self, expression, config, arguments, primary_table, join_tables):
        if not expression:
            return ["@aggregate_key", ["#const", "__k_g__"]]

        group_column = ["@aggregate_key"]
        for group_expression in expression.args["expressions"]:
            calculate_fields = []
            self.parse_calculate(group_expression, config, arguments, primary_table, calculate_fields)
            calculate_fields = [calculate_field for calculate_field in calculate_fields if calculate_field["table_name"]
                                and calculate_field["table_name"] != primary_table["table_name"]]
            if not calculate_fields:
                group_column.append(self.compile_calculate(group_expression, config, arguments, primary_table, []))
                continue

            column_join_tables = []
            calculate_table_names = set([])
            for calculate_field in calculate_fields:
                if calculate_field["table_name"] not in join_tables:
                    raise SyncanySqlCompileException('error join column, join table %s unknown, related sql "%s"' %
                                                     (calculate_field["table_name"], self.to_sql(group_expression)))
                calculate_table_names.add(calculate_field["table_name"])
            self.compile_join_column_tables(group_expression, config, arguments, primary_table,
                                            [join_tables[calculate_table_name] for calculate_table_name in
                                             calculate_table_names], join_tables, column_join_tables)
            calculate_column = self.compile_calculate(group_expression, config, arguments, primary_table,
                                                      column_join_tables)
            group_column.append(self.compile_join_column(group_expression, config, arguments, primary_table,
                                                         calculate_column, column_join_tables))
        return group_column

    def compile_aggregate_value(self, expression, config, arguments, primary_table, join_tables, value_expressions):
        if isinstance(expression, sqlglot_expressions.Count) and isinstance(expression.args["this"],
                                                                            sqlglot_expressions.Star):
            return ["@make", ["#const", 0]]
        if not value_expressions:
            return ["@make", ["#const", None]]

        value_column = ["@make"]
        for value_expression in value_expressions:
            calculate_fields = []
            self.parse_calculate(value_expression, config, arguments, primary_table, calculate_fields)
            calculate_fields = [calculate_field for calculate_field in calculate_fields if calculate_field["table_name"]
                                and calculate_field["table_name"] != primary_table["table_name"]]
            if not calculate_fields:
                value_column.append(self.compile_calculate(value_expression, config, arguments, primary_table, []))
                continue

            column_join_tables = []
            calculate_table_names = set([])
            for calculate_field in calculate_fields:
                if calculate_field["table_name"] not in join_tables:
                    raise SyncanySqlCompileException('error join column, join table %s unknown, related sql "%s"' %
                                                     (calculate_field["table_name"], self.to_sql(value_expression)))
                calculate_table_names.add(calculate_field["table_name"])
            self.compile_join_column_tables(value_expression, config, arguments, primary_table,
                                            [join_tables[calculate_table_name] for calculate_table_name in
                                             calculate_table_names], join_tables, column_join_tables)
            calculate_column = self.compile_calculate(value_expression, config, arguments, primary_table,
                                                      column_join_tables)
            value_column.append(self.compile_join_column(value_expression, config, arguments, primary_table,
                                                         calculate_column, column_join_tables))
        return value_column

    def compile_aggregate(self, expression, config, arguments, column_alias, key_column, value_column):
        if isinstance(expression, sqlglot_expressions.Count):
            calculate = ["@aggregate_count::aggregate", "$." + column_alias, "$$.value"]
            return {
                "key": key_column,
                "value": value_column,
                "calculate": calculate,
                "aggregate": [":#aggregate", "$.key", copy.deepcopy(calculate)],
                "reduce": ["@aggregate_count::reduce", "$." + column_alias, "$$." + column_alias],
                "final_value": None
            }
        elif isinstance(expression, sqlglot_expressions.Sum):
            calculate = ["@aggregate_sum::aggregate", "$." + column_alias, "$$.value"]
            return {
                "key": key_column,
                "value": value_column,
                "calculate": calculate,
                "aggregate": [":#aggregate", "$.key", copy.deepcopy(calculate)],
                "reduce": ["@aggregate_sum::reduce", "$." + column_alias, "$$." + column_alias],
                "final_value": None
            }
        elif isinstance(expression, sqlglot_expressions.Min):
            calculate = ["@aggregate_min::aggregate", "$." + column_alias, "$$.value"]
            return {
                "key": key_column,
                "value": value_column,
                "calculate": calculate,
                "aggregate": [":#aggregate", "$.key", copy.deepcopy(calculate)],
                "reduce": ["@aggregate_min::reduce", "$." + column_alias, "$$." + column_alias],
                "final_value": None
            }
        elif isinstance(expression, sqlglot_expressions.Max):
            calculate = ["@aggregate_max::aggregate", "$." + column_alias, "$$.value"]
            return {
                "key": key_column,
                "value": value_column,
                "calculate": calculate,
                "aggregate": [":#aggregate", "$.key", copy.deepcopy(calculate)],
                "reduce": ["@aggregate_max::reduce", "$." + column_alias, "$$." + column_alias],
                "final_value": None
            }
        elif isinstance(expression, sqlglot_expressions.Avg):
            calculate = ["@aggregate_avg::aggregate", "$." + column_alias, "$$.value"]
            return {
                "key": key_column,
                "value": value_column,
                "calculate": calculate,
                "aggregate": [":#aggregate", "$.key", copy.deepcopy(calculate)],
                "reduce": ["@aggregate_avg::reduce", "$." + column_alias, "$$." + column_alias],
                "final_value": ["@aggregate_avg::final_value", "$." + column_alias]
            }
        elif isinstance(expression, sqlglot_expressions.Anonymous):
            calculater_name = expression.args["this"].lower()
            try:
                aggregate_calculater = find_aggregate_calculater(calculater_name)
                calculate = ["@" + calculater_name + "::aggregate", "$." + column_alias, "$$.value"]
                return {
                    "key": key_column,
                    "value": value_column,
                    "calculate": calculate,
                    "aggregate": [":#aggregate", "$.key", copy.deepcopy(calculate)],
                    "reduce": ["@" + calculater_name + "::reduce", "$." + column_alias, "$$." + column_alias],
                    "final_value": ["@" + calculater_name + "::final_value", "$." + column_alias]
                                    if hasattr(aggregate_calculater, "final_value") else None
                }
            except CalculaterUnknownException:
                raise SyncanySqlCompileException('unknown aggregate calculate, related sql "%s"' % self.to_sql(expression))
        else:
            raise SyncanySqlCompileException('unknown aggregate calculate, related sql "%s"' % self.to_sql(expression))
        
    def compile_calculate(self, expression, config, arguments, primary_table, column_join_tables, join_index=-1):
        if isinstance(expression, sqlglot_expressions.Neg):
            return ["@neg", self.compile_calculate(expression.args["this"], config, arguments, primary_table, 
                                                   column_join_tables, join_index)]
        elif isinstance(expression, sqlglot_expressions.Anonymous):
            calculater_name = expression.args["this"].lower()
            if calculater_name == "get_value":
                get_value_expressions = expression.args.get("expressions")
                if not get_value_expressions:
                    raise SyncanySqlCompileException('error get_value, args unknown, related sql "%s"' % self.to_sql(expression))
                if len(get_value_expressions) == 1:
                    return self.compile_calculate(get_value_expressions[0], config, arguments, primary_table, 
                                                  column_join_tables, join_index)
                column = [self.compile_calculate(get_value_expressions[0], config, arguments, primary_table, 
                                                 column_join_tables, join_index)]

                def get_value_parse(get_value_expressions):
                    column = []
                    if self.is_const(get_value_expressions[0], config, arguments):
                        get_value_key = self.parse_const(get_value_expressions[0], config, arguments)["value"]
                        column.append((":$.:" + str(int(get_value_key))) if isinstance(get_value_key, (int, float)) else (":$." + str(get_value_key)))
                    elif isinstance(get_value_expressions[0], sqlglot_expressions.Tuple):
                        get_value_indexs = [self.parse_const(tuple_expression, config, arguments)["value"]
                                            for tuple_expression in get_value_expressions[0].args["expressions"]
                                          if self.is_const(tuple_expression, config, arguments)]
                        column.append(":$.:" + ":".join([str(int(i)) for i in get_value_indexs if isinstance(i, (float, int))][:3]))
                    else:
                        raise SyncanySqlCompileException('error get_value, args must by string or int tuple, related sql "%s"' % self.to_sql(get_value_expressions[0]))
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
                    if self.is_const(arg_expression, config, arguments):
                        column.append(self.compile_const(arg_expression, config, arguments,
                                                         self.parse_const(arg_expression, config, arguments)))
                    else:
                        column.append(self.compile_calculate(arg_expression, config, arguments, primary_table, column_join_tables, join_index))
                return column

            if calculater_name == "yield_data":
                column = ["#yield"]
                for arg_expression in expression.args.get("expressions", []):
                    if self.is_const(arg_expression, config, arguments):
                        column.append(self.compile_const(arg_expression, config, arguments,
                                                         self.parse_const(arg_expression, config, arguments)))
                    else:
                        column.append(self.compile_calculate(arg_expression, config, arguments, primary_table, 
                                                             column_join_tables, join_index))
                return column

            if is_mysql_func(calculater_name):
                column = ["@mysql::" + calculater_name]
            else:
                if calculater_name[:8] == "convert_":
                    column = ["@" + "::".join(calculater_name.split("$")) + "|" + calculater_name[8:]]
                else:
                    column = ["@" + "::".join(calculater_name.split("$"))]
            for arg_expression in expression.args.get("expressions", []):
                if self.is_const(arg_expression, config, arguments):
                    column.append(self.compile_const(arg_expression, config, arguments,
                                                     self.parse_const(arg_expression, config, arguments)))
                else:
                    column.append(self.compile_calculate(arg_expression, config, arguments, primary_table, column_join_tables, join_index))
            return column
        elif isinstance(expression, sqlglot_expressions.JSONExtract):
            return [
                "@mysql::json_extract" if is_mysql_func("json_extract") else "@json_extract",
                self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index),
                self.compile_calculate(expression.args["expression"], config, arguments, primary_table, column_join_tables, join_index),
            ]
        elif isinstance(expression, sqlglot_expressions.Cast):
            to_type = expression.args["to"].args["this"]
            if to_type in sqlglot_expressions.DataType.FLOAT_TYPES:
                typing_filter = "float"
            elif to_type in sqlglot_expressions.DataType.INTEGER_TYPES:
                typing_filter = "int"
            elif to_type == sqlglot_expressions.DataType.Type.DATE:
                typing_filter = "date"
            elif to_type in sqlglot_expressions.DataType.TEMPORAL_TYPES:
                typing_filter = "datetime"
            elif to_type == sqlglot_expressions.DataType.TEXT_TYPES:
                typing_filter = "str"
            else:
                typing_filter = None
            value_column = self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index)
            if typing_filter:
                return ["#if", ["#const", True], value_column, None, ":$.*|" + typing_filter]
            return value_column
        elif isinstance(expression, sqlglot_expressions.Binary):
            func_name = expression.key.lower()
            return [
                ("@mysql::" + func_name) if is_mysql_func(func_name) else ("@" + func_name),
                self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index),
                self.compile_calculate(expression.args["expression"], config, arguments, primary_table, column_join_tables, join_index)
            ]
        elif isinstance(expression, sqlglot_expressions.BitwiseNot):
            func_name = expression.key.lower()
            return [
                ("@mysql::" + func_name) if is_mysql_func(func_name) else ("@" + func_name),
                self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index)
            ]
        elif isinstance(expression, sqlglot_expressions.If):
            return [
                "#if",
                self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index),
                self.compile_calculate(expression.args["true"], config, arguments, primary_table, column_join_tables, 
                                       join_index) if expression.args.get("true") else False,
                self.compile_calculate(expression.args["false"], config, arguments, primary_table, column_join_tables, 
                                       join_index) if expression.args.get("false") else False,
            ]
        elif isinstance(expression, (sqlglot_expressions.IfNull, sqlglot_expressions.Coalesce)):
            condition_column = self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index)
            if isinstance(expression, sqlglot_expressions.Coalesce):
                def parse_coalesce(coalesce_expressions):
                    if not coalesce_expressions:
                        return None
                    coalesce_column = self.compile_calculate(coalesce_expressions[0], config, arguments, primary_table, column_join_tables, join_index)
                    if len(coalesce_expressions) == 1:
                        return coalesce_column
                    if len(coalesce_expressions) > 2:
                        coalesce_value_column = parse_coalesce(coalesce_expressions[1:])
                    else:
                        coalesce_value_column = self.compile_calculate(coalesce_expressions[1], config, arguments, primary_table, column_join_tables, join_index)
                    return ["#if", ["@is_null", coalesce_column], coalesce_value_column, coalesce_column]
                value_column = parse_coalesce(expression.args.get("expressions"))
            else:
                value_column = self.compile_calculate(expression.args["expression"], config, arguments, primary_table, column_join_tables, join_index)
            return ["#if", ["@is_null", condition_column], value_column, condition_column]
        elif isinstance(expression, sqlglot_expressions.Case):
            cases = {}
            cases["#case"] = self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index) \
                                 if expression.args.get("this") else ["#const", True]
            if expression.args.get("ifs"):
                for case_expression in expression.args["ifs"]:
                    if not isinstance(case_expression, sqlglot_expressions.If) or not self.is_const(case_expression.args["this"], config, arguments):
                        raise SyncanySqlCompileException('unknown calculate function, related sql "%s"' % self.to_sql(expression))
                    case_value = self.parse_const(case_expression.args["this"], config, arguments)["value"]
                    cases[(":" + str(case_value)) if isinstance(case_value, (int, float)) and not isinstance(case_value, bool) else case_value] = \
                        self.compile_calculate(case_expression.args["true"], config, arguments, primary_table, column_join_tables, join_index)
            cases["#end"] = self.compile_calculate(expression.args["default"], config, arguments, primary_table, column_join_tables, join_index) \
                                if expression.args.get("default") else None
            return cases
        elif isinstance(expression, sqlglot_expressions.Func):
            func_args = {
                "substring": ["this", "start", "length"],
            }
            func_name = expression.key.lower()
            column = ["@mysql::" + func_name] if is_mysql_func(func_name) else ["@" + func_name]
            arg_names = func_args.get(func_name) or [key for key in expression.arg_types] or ["this", "expression", "expressions"]
            for arg_name in arg_names:
                if not expression.args.get(arg_name):
                    continue
                if isinstance(expression.args[arg_name], list):
                    for item_expression in expression.args[arg_name]:
                        column.append(self.compile_calculate(item_expression, config, arguments, primary_table, column_join_tables, join_index))
                else:
                    column.append(self.compile_calculate(expression.args[arg_name], config, arguments, primary_table, column_join_tables, join_index))
            return column
        elif isinstance(expression, sqlglot_expressions.Distinct):
            return [self.compile_calculate(distinct_expression, config, arguments, primary_table, column_join_tables, join_index)
                    for distinct_expression in expression.args["expressions"]]
        elif isinstance(expression, (sqlglot_expressions.Select, sqlglot_expressions.Subquery, sqlglot_expressions.Union)):
            return self.compile_query_condition(expression, config, arguments, primary_table, None)
        elif isinstance(expression, sqlglot_expressions.Paren):
            return self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index)
        elif isinstance(expression, sqlglot_expressions.Not):
            return ["@not", self.compile_calculate(expression.args["this"], config, arguments, primary_table,
                                                   column_join_tables, join_index)]
        elif self.is_column(expression, config, arguments):
            join_column = self.parse_column(expression, config, arguments)
            return self.compile_join_column_field(expression, config, arguments, primary_table, join_index, 
                                                  join_column, column_join_tables)
        elif isinstance(expression, sqlglot_expressions.Star):
            return "$.*"
        elif isinstance(expression, sqlglot_expressions.Tuple):
            tuple_column = ["@make"]
            for tuple_expression in expression.args["expressions"]:
                tuple_column.append(self.compile_calculate(tuple_expression, config, arguments, primary_table,
                                                   column_join_tables, join_index))
            return ["@convert_array", tuple_column]
        elif isinstance(expression, sqlglot_expressions.Interval):
            return ["#const", {"value": expression.args["this"].args["this"], "unit": expression.args["unit"].args["this"]}]
        elif self.is_const(expression, config, arguments):
            return self.compile_const(expression, config, arguments, self.parse_const(expression, config, arguments))
        else:
            raise SyncanySqlCompileException('unknown calculate expression, related sql "%s"' % self.to_sql(expression))

    def compile_having_condition(self, expression, config, arguments, primary_table):
        if isinstance(expression, sqlglot_expressions.And):
            return [
                "@and",
                self.compile_having_condition(expression.args.get("this"), config, arguments, primary_table),
                self.compile_having_condition(expression.args.get("expression"), config, arguments, primary_table)
            ]
        if isinstance(expression, sqlglot_expressions.Or):
            return [
                "@or",
                self.compile_having_condition(expression.args.get("this"), config, arguments, primary_table),
                self.compile_having_condition(expression.args.get("expression"), config, arguments, primary_table)
            ]

        def parse(expression):
            left_column = None
            if "aggregate" not in config:
                config["aggregate"] = copy.deepcopy(DEAULT_AGGREGATE)
            if expression.args.get("expressions"):
                left_expression, right_expression = expression.args["this"], expression.args["expressions"]
            elif expression.args.get("query"):
                left_expression, right_expression = expression.args["this"], expression.args["query"]
            else:
                left_expression, right_expression = expression.args["this"], expression.args["expression"]
            if self.is_column(left_expression, config, arguments):
                left_column = self.parse_column(left_expression, config, arguments)
                if isinstance(config["schema"], dict) and left_column["column_name"] not in config["schema"]:
                    raise SyncanySqlCompileException('unknown having condition column, column must be in the query result, related sql "%s"'
                                                     % self.to_sql(expression))
                config["aggregate"]["having_columns"].add(left_column["column_name"])
                left_calculater = self.compile_column(left_expression, config, arguments, left_column)
            else:
                calculate_fields = []
                self.parse_calculate(left_expression, config, arguments, primary_table, calculate_fields)
                if isinstance(config["schema"], dict) and [calculate_field for calculate_field in calculate_fields
                                                           if calculate_field["column_name"] not in config["schema"]]:
                    raise SyncanySqlCompileException('unknown having condition column, column must be in the query result, related sql "%s"'
                                                     % self.to_sql(expression))
                for calculate_field in calculate_fields:
                    if calculate_field["column_name"] not in config["schema"]:
                        continue
                    config["aggregate"]["having_columns"].add(calculate_field["column_name"])
                left_calculater = self.compile_calculate(left_expression, config, arguments, primary_table, [])

            if isinstance(right_expression, list):
                value_items = []
                for value_expression_item in right_expression:
                    if not self.is_const(value_expression_item, config, arguments):
                        raise SyncanySqlCompileException('error having condition, array must be const value, related sql "%s"' % self.to_sql(expression))
                    value_items.append(self.parse_const(value_expression_item, config, arguments)["value"])
                return left_calculater, value_items
            if self.is_column(right_expression, config, arguments):
                right_column = self.parse_column(right_expression, config, arguments)
                if isinstance(config["schema"], dict) and right_column["column_name"] not in config["schema"]:
                    raise SyncanySqlCompileException('unknown having condition column, column must be in the query result, related sql "%s"'
                                                     % self.to_sql(expression))
                config["aggregate"]["having_columns"].add(right_column["column_name"])
                return left_calculater, self.compile_column(right_expression, config, arguments, right_column)
            if isinstance(right_expression, (sqlglot_expressions.Select, sqlglot_expressions.Subquery, sqlglot_expressions.Union)):
                value_column = self.compile_query_condition(right_expression, config, arguments, primary_table,
                                                            left_column["typing_filters"] if left_column else None)
                if isinstance(expression, sqlglot_expressions.In):
                    return left_calculater, ["@convert_array", value_column]
                return left_calculater, ["@convert_array", value_column, ":$.:0"]
            calculate_fields = []
            self.parse_calculate(right_expression, config, arguments, primary_table, calculate_fields)
            if isinstance(config["schema"], dict) and [calculate_field for calculate_field in calculate_fields
                                                       if calculate_field["column_name"] not in config["schema"]]:
                raise SyncanySqlCompileException('unknown having condition column, column must be in the query result, related sql "%s"'
                                                 % self.to_sql(expression))
            for calculate_field in calculate_fields:
                if calculate_field["column_name"] not in config["schema"]:
                    continue
                config["aggregate"]["having_columns"].add(calculate_field["column_name"])
            return left_calculater, self.compile_calculate(right_expression, config, arguments, primary_table, [])

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
            return ["@in", left_calculater, ["@convert_array", right_calculater]]
        elif isinstance(expression, sqlglot_expressions.Like):
            left_calculater, right_calculater = parse(expression)
            if not isinstance(right_calculater, list) or len(right_calculater) != 2 or right_calculater[0] != "#const":
                raise SyncanySqlCompileException('error having condition, like condition value must be const, related sql "%s"'
                                                 % self.to_sql(expression))
            return ["#if", ["@re::match", right_calculater[1].replace("%", ".*").replace(".*.*", "%"), left_calculater],
                    ["#const", True], ["#const", False]]
        elif isinstance(expression, (sqlglot_expressions.Not, sqlglot_expressions.Is)):
            return self.compile_calculate(expression, config, arguments, primary_table, [])
        elif isinstance(expression, sqlglot_expressions.Paren):
            return self.compile_having_condition(expression.args.get("this"), config, arguments, primary_table)
        else:
            raise SyncanySqlCompileException('unknown having condition, related sql "%s"' % self.to_sql(expression))

    def compile_order(self, expression, config, arguments, primary_table):
        primary_sort_keys, sort_keys = [], []
        for order_expression in expression:
            column = self.parse_column(order_expression.args["this"], config, arguments)
            if (column["table_name"] and primary_table["table_alias"] == column["table_name"]) or \
                    (not column["table_name"] and column["column_name"] in primary_table["columns"]):
                primary_sort_keys.append([column["column_name"], True if order_expression.args["desc"] else False])
            sort_keys.append((column["column_name"], True if order_expression.args["desc"] else False))
        if sort_keys and len(primary_sort_keys) < len(sort_keys):
            if isinstance(config["schema"], dict):
                for sort_key, _ in sort_keys:
                    if sort_key not in config["schema"]:
                        raise SyncanySqlCompileException('unknown order by column, related sql "%s"' % self.to_sql(expression))
            config["pipelines"].append([">>@sort", "$.*|array", False, sort_keys])
            if "@limit" in arguments and arguments["@limit"] > 0:
                config["pipelines"].append([">>@array::slice", "$.*|array", 0, arguments["@limit"]])
                arguments["@limit"] = 0
        if primary_sort_keys:
            config["orders"].extend(primary_sort_keys)
        
    def compile_column(self, expression, config, arguments, column, scope_depth=1):
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
    
    def compile_const(self, expression, config, arguments, literal):
        if "value_getter" in literal and literal["value_getter"]:
            if "imports" not in config:
                config["imports"] = {}
            config["imports"]["getter_" + str(id(literal["value_getter"]))] = literal["value_getter"]
            return ["@getter_" + str(id(literal["value_getter"])) + "::get", ["#const", literal["value_getter"].key]]
        return ["#const", literal["value"]]

    def parse_joins(self, expression, config, arguments, primary_table, join_expressions):
        join_tables = {}
        for join_expression in join_expressions:
            if "alias" not in join_expression.args["this"].args:
                raise SyncanySqlCompileException('error join, table must be alias name, related sql "%s"' % self.to_sql(join_expression))
            name = join_expression.args["this"].args["alias"].args["this"].name
            if isinstance(join_expression.args["this"], sqlglot_expressions.Table):
                table_info = self.parse_table(join_expression.args["this"], config, arguments)
                db, table, subquery_config = table_info["db"], table_info["name"], None
            elif isinstance(join_expression.args["this"], sqlglot_expressions.Subquery):
                subquery_name, subquery_config = self.compile_subquery(join_expression.args["this"], arguments)
                db, table = "--", subquery_name
                config["dependencys"].append(subquery_config)
            else:
                raise SyncanySqlCompileException('unknown join expression, related sql "%s"' % self.to_sql(join_expression))
            if "on" not in join_expression.args:
                raise SyncanySqlCompileException('error join, on condition is unknown, related sql "%s"'
                                                 % self.to_sql(join_expression))
            join_table = {
                "db": db, "table": table, "name": name, "primary_keys": [],
                "join_columns": [], "calculate_expressions": [], "querys": {}, "ref_count": 0,
                "subquery": subquery_config
            }
            self.parse_on_condition(join_expression.args["on"], config, arguments, primary_table, join_table)
            self.parse_condition_typing_filter(expression, join_table, arguments)
            if not join_table["primary_keys"] or not join_table["join_columns"]:
                raise SyncanySqlCompileException('error join, columns unknown, related sql "%s"' % self.to_sql(join_expression))
            join_tables[join_table["name"]] = join_table

        for name, join_table in join_tables.items():
            for join_column in join_table["join_columns"]:
                if not join_column["table_name"] or join_column["table_name"] == primary_table["table_name"]:
                    continue
                if join_column["table_name"] not in join_tables:
                    raise SyncanySqlCompileException("unknown join table: " + join_column["table_name"])
                join_tables[join_column["table_name"]]["ref_count"] += 1
        return join_tables

    def parse_on_condition(self, expression, config, arguments, primary_table, join_table):
        if not expression:
            return
        if isinstance(expression, sqlglot_expressions.And):
            self.parse_on_condition(expression.args.get("this"), config, arguments, primary_table, join_table)
            self.parse_on_condition(expression.args.get("expression"), config, arguments, primary_table, join_table)
            return

        def parse(expression):
            table_expression, value_expression = None, None
            if isinstance(expression, sqlglot_expressions.EQ) and expression.args.get("expression"):
                for arg_expression in (expression.args["this"], expression.args["expression"]):
                    if not isinstance(arg_expression, sqlglot_expressions.Column):
                        if not self.is_column(arg_expression, config, arguments):
                            continue
                        arg_column = self.parse_column(arg_expression, config, arguments)
                        condition_table = arg_column["table_name"]
                    else:
                        condition_table = arg_expression.args["table"].name if "table" in arg_expression.args else None
                    if condition_table and condition_table == join_table["name"]:
                        table_expression = arg_expression
                        break
                if table_expression is None:
                    raise SyncanySqlCompileException('unkonw join on condition, related sql "%s"' % self.to_sql(expression))
                value_expression = expression.args["expression"] if table_expression == expression.args["this"] else expression.args["this"]
            elif expression.args.get("expressions"):
                table_expression, value_expression = expression.args["this"], expression.args["expressions"]
            elif expression.args.get("query"):
                table_expression, value_expression = expression.args["this"], expression.args["query"]
            elif isinstance(expression.args["expression"], sqlglot_expressions.Subquery):
                table_expression, value_expression = expression.args["this"], expression.args["expression"].args["this"]
            else:
                table_expression, value_expression = expression.args["this"], expression.args["expression"]

            condition_column = self.parse_column(table_expression, config, arguments)
            if not isinstance(expression, sqlglot_expressions.EQ) or self.is_const(value_expression, config, arguments) \
                    or isinstance(value_expression, list) or isinstance(value_expression, sqlglot_expressions.Select):
                if not self.is_column(table_expression, config, arguments) or not condition_column["table_name"]:
                    raise SyncanySqlCompileException('unkonw join on condition, related sql "%s"' % self.to_sql(expression))
                if isinstance(value_expression, (sqlglot_expressions.Select, sqlglot_expressions.Subquery, sqlglot_expressions.Union)):
                    value_column = self.compile_query_condition(value_expression, config, arguments, primary_table,
                                                                condition_column["typing_filters"])
                    if isinstance(expression, sqlglot_expressions.In):
                        value_column = ["@convert_array", value_column]
                    else:
                        value_column = ["@convert_array", value_column, ":$.:0"]
                    return False, condition_column, value_column
                if isinstance(value_expression, list):
                    value_items = []
                    for value_expression_item in value_expression:
                        if not self.is_const(value_expression_item, config, arguments):
                            raise SyncanySqlCompileException('error join on condition, array must be const value, related sql "%s"'
                                                             % self.to_sql(expression))
                        value_items.append(self.parse_const(value_expression_item, config, arguments)["value"])
                    return False, condition_column, value_items
                calculate_fields = []
                self.parse_calculate(value_expression, config, arguments, primary_table, calculate_fields)
                if calculate_fields:
                    raise SyncanySqlCompileException('error join on condition, conditions except = conditions must be constants, related sql "%s"'
                                                     % self.to_sql(expression))
                return False, condition_column, self.compile_calculate(value_expression, config, arguments, primary_table, [])

            if self.is_column(value_expression, config, arguments):
                if condition_column["column_name"] in join_table["primary_keys"]:
                    raise SyncanySqlCompileException('error join on condition, primary_key duplicate, related sql "%s"' % self.to_sql(expression))
                join_table["join_columns"].append(self.parse_column(value_expression, config, arguments))
                join_table["primary_keys"].append(condition_column["column_name"])
                join_table["calculate_expressions"].append(value_expression)
                return True, condition_column, None
            calculate_fields = []
            self.parse_calculate(value_expression, config, arguments, primary_table, calculate_fields)
            if not calculate_fields and condition_column["table_name"] == join_table["name"]:
                return False, condition_column, self.compile_calculate(value_expression, config, arguments, primary_table, [])
            if condition_column["column_name"] in join_table["primary_keys"]:
                raise SyncanySqlCompileException('error join on condition, primary_key duplicate, related sql "%s"' % self.to_sql(expression))
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
                raise SyncanySqlCompileException('error join on condition, conditions except = conditions must be constants, related sql "%s"'
                                                 % self.to_sql(expression))
            if condition_column["typing_name"] not in join_table["querys"]:
                join_table["querys"][condition_column["typing_name"]] = {}
            join_table["querys"][condition_column["typing_name"]]["!="] = value_column
        elif isinstance(expression, sqlglot_expressions.GT):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException('error join on condition, conditions except = conditions must be constants, related sql "%s"'
                                                 % self.to_sql(expression))
            if condition_column["typing_name"] not in join_table["querys"]:
                join_table["querys"][condition_column["typing_name"]] = {}
            join_table["querys"][condition_column["typing_name"]][">"] = value_column
        elif isinstance(expression, sqlglot_expressions.GTE):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException('error join on condition, conditions except = conditions must be constants, related sql "%s"'
                                                 % self.to_sql(expression))
            if condition_column["typing_name"] not in join_table["querys"]:
                join_table["querys"][condition_column["typing_name"]] = {}
            join_table["querys"][condition_column["typing_name"]][">="] = value_column
        elif isinstance(expression, sqlglot_expressions.LT):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException('error join on condition, conditions except = conditions must be constants, related sql "%s"'
                                                 % self.to_sql(expression))
            if condition_column["typing_name"] not in join_table["querys"]:
                join_table["querys"][condition_column["typing_name"]] = {}
            join_table["querys"][condition_column["typing_name"]]["<"] = value_column
        elif isinstance(expression, sqlglot_expressions.LTE):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException('error join on condition, conditions except = conditions must be constants, related sql "%s"'
                                                 % self.to_sql(expression))
            if condition_column["typing_name"] not in join_table["querys"]:
                join_table["querys"][condition_column["typing_name"]] = {}
            join_table["querys"][condition_column["typing_name"]]["<="] = value_column
        elif isinstance(expression, sqlglot_expressions.In):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException('error join on condition, conditions except = conditions must be constants, related sql "%s"'
                                                 % self.to_sql(expression))
            if condition_column["typing_name"] not in join_table["querys"]:
                join_table["querys"][condition_column["typing_name"]] = {}
            join_table["querys"][condition_column["typing_name"]]["in"] = value_column
        else:
            raise SyncanySqlCompileException('error join on condition, only "=,!=,>,>=,<,<=,in" operations are supported, related sql "%s"'
                                             % self.to_sql(expression))

    def parse_calculate(self, expression, config, arguments, primary_table, calculate_fields):
        if isinstance(expression, sqlglot_expressions.Anonymous):
            for arg_expression in expression.args.get("expressions", []):
                self.parse_calculate(arg_expression, config, arguments, primary_table, calculate_fields)
        elif isinstance(expression, sqlglot_expressions.Cast):
            self.parse_calculate(expression.args["this"], config, arguments, primary_table, calculate_fields)
        elif isinstance(expression, sqlglot_expressions.If):
            self.parse_calculate(expression.args["this"], config, arguments, primary_table, calculate_fields)
            if expression.args.get("true"):
                self.parse_calculate(expression.args["true"], config, arguments, primary_table, calculate_fields)
            if expression.args.get("false"):
                self.parse_calculate(expression.args["false"], config, arguments, primary_table, calculate_fields)
        elif isinstance(expression, sqlglot_expressions.Case):
            if expression.args.get("this"):
                self.parse_calculate(expression.args["this"], config, arguments, primary_table, calculate_fields)
            if expression.args.get("default"):
                self.parse_calculate(expression.args["default"], config, arguments, primary_table, calculate_fields)
            if "ifs" in expression.args:
                for case_expression in expression.args["ifs"]:
                    self.parse_calculate(case_expression, config, arguments, primary_table, calculate_fields)
        elif isinstance(expression, sqlglot_expressions.Paren):
            self.parse_calculate(expression.args["this"], config, arguments, primary_table, calculate_fields)
        elif self.is_column(expression, config, arguments):
            column = self.parse_column(expression, config, arguments)
            if not column["table_name"] or primary_table["table_name"] == column["table_name"]:
                primary_table["columns"][column["column_name"]] = column
            calculate_fields.append(column)
        elif isinstance(expression, (sqlglot_expressions.Select, sqlglot_expressions.Star, sqlglot_expressions.Interval,
                                     sqlglot_expressions.DataType)):
            pass
        elif self.is_const(expression, config, arguments):
            pass
        else:
            if "this" in expression.args and expression.args["this"]:
                self.parse_calculate(expression.args["this"], config, arguments, primary_table, calculate_fields)
            if "expression" in expression.args and expression.args["expression"]:
                self.parse_calculate(expression.args["expression"], config, arguments, primary_table, calculate_fields)
            if "expressions" in expression.args and expression.args["expressions"] and isinstance(expression.args["expressions"], list):
                for arg_expression in expression.args.get("expressions", []):
                    self.parse_calculate(arg_expression, config, arguments, primary_table, calculate_fields)
            for arg_type in expression.arg_types:
                if arg_type in ("this", "expression", "expressions"):
                    continue
                if arg_type not in expression.args or not expression.args[arg_type]:
                    continue
                if isinstance(expression.args[arg_type], list):
                    for arg_expression in expression.args.get(arg_type, []):
                        self.parse_calculate(arg_expression, config, arguments, primary_table, calculate_fields)
                else:
                    self.parse_calculate(expression.args[arg_type], config, arguments, primary_table, calculate_fields)

    def parse_table(self, expression, config, arguments):
        db_name = expression.args["db"].name if "db" in expression.args and expression.args["db"] else None
        if isinstance(expression.args["this"], sqlglot_expressions.Dot):
            def parse(expression):
                return (parse(expression.args["this"]) if isinstance(expression.args["this"],
                                                                     sqlglot_expressions.Dot) else expression.args["this"].name) \
                       + "." + (parse(expression.args["expression"]) if isinstance(expression.args["expression"],
                                                                                   sqlglot_expressions.Dot) else expression.args["expression"].name)
            table_name = parse(expression.args["this"])
        elif isinstance(expression.args["this"], sqlglot_expressions.Parameter):
            try:
                table_name = self.env_variables.get_value("@" + expression.args["this"].name)
            except KeyError:
                raise SyncanySqlCompileException('error table, undefine table name variable, related sql "%s"' % self.to_sql(expression))
            if table_name is None:
                table_name = "#{env_variable__@" + expression.args["this"].name + "}"
            elif not isinstance(table_name, str):
                raise SyncanySqlCompileException('error table, table name variable value must be str, related sql "%s"' % self.to_sql(expression))
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

        if table_name in ("_", "."):
            db_name, table_name = "-", "&1"
        elif table_name in (".txt", ".csv", ".json"):
            format_type = table_name[1:]
            database = None
            if "databases" not in config:
                config["databases"] = []
            for d in config["databases"]:
                if d.get("driver") == "textline" and d.get("format") == format_type:
                    database = d
                    break
            if not database:
                database = {"name": "stdio-" + format_type, "driver": "textline", "format": format_type}
                config["databases"].append(database)
            db_name, table_name = database["name"], "&1"
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
                path_db_name = "dir__" + "".join([c if c.isalpha() or c.isdigit() else '_' for c in path])
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
                    if d.get("driver") == db_driver and d.get("path") == path and not db_params:
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
        
    def parse_column(self, expression, config, arguments):
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
    
    def parse_const(self, expression, config, arguments):
        is_neg, typing_filter, value_getter = False, None, None
        if isinstance(expression, sqlglot_expressions.Neg):
            is_neg, expression = True, expression.args["this"]
        if isinstance(expression, (sqlglot_expressions.Literal, sqlglot_expressions.ByteString)):
            value = expression.args["this"]
            if expression.is_number:
                value = int(value) if expression.is_int else float(value)
                if is_neg:
                    value = -value
                typing_filter = "int" if expression.is_int else "float"
            elif isinstance(value, str):
                try:
                    escape_values, escape_chars, encoding = [], [], os.environ.get("SYNCANYENCODING", "utf-8")
                    for c in value:
                        if ord(c) <= 256:
                            escape_chars.append(c)
                            continue
                        if escape_chars:
                            escape_values.append("".join(escape_chars).encode(encoding).decode("unicode_escape"))
                            escape_chars = []
                        escape_values.append(c)
                    if escape_chars:
                        escape_values.append("".join(escape_chars).encode(encoding).decode("unicode_escape"))
                    value = "".join(escape_values)
                except:
                    pass
        elif isinstance(expression, sqlglot_expressions.Boolean):
            value = expression.args["this"]
            typing_filter = "bool"
        elif isinstance(expression, (sqlglot_expressions.HexString, sqlglot_expressions.HexString)):
            value, typing_filter = int(expression.args["this"]), "int"
        elif isinstance(expression, sqlglot_expressions.Parameter):
            name = expression.args["this"].args["this"]
            if name in self.mapping:
                name = self.mapping[name]
            try:
                value = self.env_variables.get_value("@" + name)
                if isinstance(value, (int, float, bool)):
                    typing_filter = str(type(value).__name__)
                value_getter = EnvVariableGetter(self.env_variables, "@" + name)
            except KeyError:
                raise SyncanySqlCompileException('unkonw parameter variable, related sql "%s"' % self.to_sql(expression))
        else:
            value = None
        return {
            "value": value,
            "value_getter": value_getter,
            "typing_filter": typing_filter,
            "expression": expression,
        }

    def parse_condition_typing_filter(self, expression, config, arguments):
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

    def is_column(self, expression, config, arguments):
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

    def is_const(self, expression, config, arguments):
        if isinstance(expression, sqlglot_expressions.Neg):
            return not isinstance(expression.args["this"], sqlglot_expressions.Neg) and self.is_const(expression.args["this"], config, arguments)
        return isinstance(expression, (sqlglot_expressions.Literal, sqlglot_expressions.Boolean,
                                       sqlglot_expressions.Null, sqlglot_expressions.HexString, sqlglot_expressions.BitString,
                                       sqlglot_expressions.ByteString, sqlglot_expressions.Parameter))

    def is_calculate(self, expression, config, arguments):
        return isinstance(expression, (sqlglot_expressions.Neg, sqlglot_expressions.Anonymous, sqlglot_expressions.Binary, sqlglot_expressions.Func,
                                       sqlglot_expressions.Select, sqlglot_expressions.Subquery, sqlglot_expressions.Union, sqlglot_expressions.Not,
                                       sqlglot_expressions.BitwiseNot, sqlglot_expressions.Tuple))

    def is_aggregate(self, expression, config, arguments):
        if isinstance(expression, (sqlglot_expressions.Count, sqlglot_expressions.Sum, sqlglot_expressions.Max,
                                       sqlglot_expressions.Min, sqlglot_expressions.Avg)):
            return True
        if isinstance(expression, sqlglot_expressions.Anonymous):
            calculater_name = expression.args["this"].lower()
            try:
                find_aggregate_calculater(calculater_name)
            except CalculaterUnknownException:
                return False
            return True
        return False

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
