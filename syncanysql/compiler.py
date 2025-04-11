# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os
import copy
import uuid

import sqlglot.expressions
from sqlglot import maybe_parse, ParseError
from sqlglot import expressions as sqlglot_expressions
from sqlglot import dialects as sqlglot_dialects
from sqlglot import parser as sqlglot_parser
from sqlglot import generator as sqlglot_generator
from sqlglot import tokens
from syncany.filters import find_filter
from .utils import NumberTypes
from .errors import SyncanySqlCompileException
from .calculaters import is_mysql_func, find_generate_calculater, find_aggregate_calculater, find_window_aggregate_calculater, CalculaterUnknownException
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


class AssignParameter(sqlglot_expressions.Parameter):
    arg_types = {"this": True, "expression": False, "wrapped": False}


class CompilerDialect(sqlglot_dialects.Dialect):
    class Tokenizer(tokens.Tokenizer):
        QUOTES = ["'", '"']
        COMMENTS = ["--", "#", ("/*", "*/")]
        IDENTIFIERS = ["`"]
        ESCAPES = ["'", "\\"]
        BIT_STRINGS = [("b'", "'"), ("B'", "'"), ("0b", "")]
        HEX_STRINGS = [("x'", "'"), ("X'", "'"), ("0x", "")]

    class Parser(sqlglot_parser.Parser):
        def _parse_parameter(self):
            wrapped = self._match(sqlglot_parser.TokenType.L_BRACE)
            this = self._parse_var() or self._parse_primary()
            self._match(sqlglot_parser.TokenType.R_BRACE)
            if not self._match_pair(sqlglot_parser.TokenType.COLON, sqlglot_parser.TokenType.EQ, False):
                return self.expression(sqlglot_expressions.Parameter, this=this, wrapped=wrapped)
            self._advance(2)
            expression = self._parse_conjunction() or self._parse_function() or self._parse_id_var()
            return self.expression(AssignParameter, this=this, expression=expression, wrapped=wrapped)

        def _parse_limit(self, this=None, top=False):
            if top or not self._match(sqlglot_parser.TokenType.LIMIT, False):
                return sqlglot_parser.Parser._parse_limit(self, this, top)
            offset_token = sqlglot_parser.seq_get(self._tokens, self._index + 1)
            if offset_token is None or offset_token.token_type != sqlglot_parser.TokenType.NUMBER:
                return sqlglot_parser.Parser._parse_limit(self, this, top)
            comma_token = sqlglot_parser.seq_get(self._tokens, self._index + 2)
            if comma_token is None or comma_token.token_type != sqlglot_parser.TokenType.COMMA:
                return sqlglot_parser.Parser._parse_limit(self, this, top)
            limit_token = sqlglot_parser.seq_get(self._tokens, self._index + 3)
            if limit_token is None or limit_token.token_type != sqlglot_parser.TokenType.NUMBER:
                return sqlglot_parser.Parser._parse_limit(self, this, top)
            return self.expression(sqlglot_parser.exp.Limit, this=this,
                                   expression=self.PRIMARY_PARSERS[sqlglot_parser.TokenType.NUMBER](self, limit_token))

        def _parse_offset(self, this=None):
            if not self._match(sqlglot_parser.TokenType.LIMIT, False):
                return sqlglot_parser.Parser._parse_offset(self, this)
            offset_token = sqlglot_parser.seq_get(self._tokens, self._index + 1)
            if offset_token is None or offset_token.token_type != sqlglot_parser.TokenType.NUMBER:
                return sqlglot_parser.Parser._parse_offset(self, this)
            comma_token = sqlglot_parser.seq_get(self._tokens, self._index + 2)
            if comma_token is None or comma_token.token_type != sqlglot_parser.TokenType.COMMA:
                return sqlglot_parser.Parser._parse_offset(self, this)
            limit_token = sqlglot_parser.seq_get(self._tokens, self._index + 3)
            if limit_token is None or limit_token.token_type != sqlglot_parser.TokenType.NUMBER:
                return sqlglot_parser.Parser._parse_offset(self, this)
            self._advance(4)
            return self.expression(sqlglot_parser.exp.Offset, this=this,
                                   expression=self.PRIMARY_PARSERS[sqlglot_parser.TokenType.NUMBER](self, offset_token))

    class Generator(sqlglot_generator.Generator):
        TRANSFORMS = {
            **sqlglot_generator.Generator.TRANSFORMS,
            AssignParameter: lambda self, e: self.assign_parameter_sql(e),
        }

        def assign_parameter_sql(self, expression):
            this = self.sql(expression, "this")
            this = f"{{{this}}}" if expression.args.get("wrapped") else f"{this}"
            return f"""{self.PARAMETER_TOKEN}{this} := {self.sql(expression, "expression")}"""


class Compiler(object):
    ESCAPE_CHARS = ['\\\\a', '\\\\b', '\\\\f', '\\\\n', '\\\\r', '\\\\t', '\\\\v', '\\\\0']
    TYPE_FILTERS = {"int": "int", "float": "float", "str": "str", "string": "str", "bytes": "bytes", 'bool': 'bool', 'array': 'array', 'set': 'set',
                   'map': 'map', "objectid": "objectid", "uuid": "uuid", "datetime": "datetime", "date": "date", "time": "time",
                   "char": "str", "varchar": "str", "nchar": "str", "text": "str", "mediumtext": "str", "tinytext": "str",
                   "bigint": "int", "mediumint": "int", "smallint": "int", "tinyint": "int", "decimal": "decimal", "double": "float",
                   "boolean": "bool", "binary": "bytes", "varbinary": "bytes", "blob": "bytes", "timestamp": "datetime"}

    def __init__(self, config, env_variables, name):
        self.config = config
        self.env_variables = env_variables
        self.name = name
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
            query_tasker = QueryTasker(self.compile_query(expression, arguments), expression)
            if not expression.args.get("into"):
                return query_tasker
            if not isinstance(expression, sqlglot_expressions.Select):
                raise SyncanySqlCompileException('unknown into, related sql "%s"' % self.to_sql(expression))
            return IntoTasker(query_tasker, self.compile_select_into(expression.args.get("into"), arguments))
        elif isinstance(expression, sqlglot_expressions.Command):
            if expression.args["this"].lower() == "explain" and self.is_const(expression.args["expression"], {}, arguments):
                return ExplainTasker(self.compile(self.parse_const(expression.args["expression"], {}, arguments)["value"],
                                                  arguments), self.to_sql(expression.args["expression"]))
            if expression.args["this"].lower() == "set" and (isinstance(expression.args["expression"], str)
                                                             or self.is_const(expression.args["expression"], {}, arguments)):
                if isinstance(expression.args["expression"], str):
                    value = expression.args["expression"].split("=")
                else:
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
        elif isinstance(expression, sqlglot.expressions.Set) and expression.args.get("expressions") and len(expression.args.get("expressions")) == 1:
            set_item_expression = expression.args.get("expressions")[0]
            if isinstance(set_item_expression, sqlglot_expressions.SetItem) and isinstance(set_item_expression.args["this"], sqlglot_expressions.EQ):
                value = self.generate_sql(set_item_expression).split("=")
                config = {"key": value[0].strip(), "value": "=".join(value[1:]).strip()}
                return SetCommandTasker(config)
        raise SyncanySqlCompileException('unknown sql "%s"' % self.to_sql(expression))

    def compile_delete(self, expression, arguments):
        config = self.config.create_tasker_config()
        config.update({
            "extends": ["context://session/config"],
            "name": self.name or "",
            "input": "&.--.--::id",
            "loader": "const_loader",
            "loader_arguments":  {"datas": []},
            "output": "&.-.&1::id",
            "querys": {},
            "databases": [],
            "predicate": None,
            "schema": "$.*",
            "intercept": None,
            "orders": [],
            "dependencys": [],
            "pipelines": []
        })

        table_info = self.parse_table(expression.args["this"], config, arguments)
        where_expression = expression.args.get("where")
        if where_expression and isinstance(where_expression, sqlglot_expressions.Where):
            where_calculate_expression = self.compile_where_condition(where_expression.args["this"], config, arguments, table_info, [])
            if where_calculate_expression is not None:
                raise SyncanySqlCompileException('unknown where expression, related sql "%s"' % self.to_sql(expression))
            self.parse_condition_typing_filter(expression, config, arguments)
        config["output"] = "".join(["&.", table_info["db"], ".", table_info["name"], "::id use DI"])
        return config

    def compile_query(self, expression, arguments):
        config = self.config.create_tasker_config()
        config.update({
            "extends": ["context://session/config"],
            "name": self.name or "",
            "input": "&.-.&1::id",
            "output": "&.-.&1::id",
            "querys": {},
            "databases": [],
            "predicate": None,
            "schema": "$.*",
            "intercept": None,
            "orders": [],
            "dependencys": [],
            "pipelines": []
        })

        if expression.args.get("with"):
            self.compile_with(expression.args.get("with"), config, arguments)
        if isinstance(expression, sqlglot_expressions.Union):
            self.compile_union(expression, config, arguments)
        elif isinstance(expression, sqlglot_expressions.Insert):
            self.compile_insert_into(expression, config, arguments)
        elif isinstance(expression, sqlglot_expressions.Select):
            self.compile_select(self.optimize_rewrite(expression, config, arguments), config, arguments)
        else:
            raise SyncanySqlCompileException('unknown sql "%s"' % self.to_sql(expression))
        return config

    def compile_with(self, expression, config, arguments):
        for sub_expression in expression.args["expressions"]:
            table_name = sub_expression.args["alias"].name
            subquery_arguments = {key: arguments[key] for key in CONST_CONFIG_KEYS if key in arguments}
            subquery_config = self.compile_query(sub_expression.args["this"], subquery_arguments)
            subquery_config["output"] = "&.--." + table_name + "::" + subquery_config["output"].split("::")[-1].split(" use ")[0] + " use I"
            subquery_config["name"] = subquery_config["name"] + "#" + table_name + "#select"
            arguments.update({subquery_config["name"] + "@" + key: value for key, value in subquery_arguments.items()})
            output_db_table_name = subquery_config["output"].split("::")[0]
            for dependency_config in (subquery_config.get("dependencys") or []):
                if isinstance(dependency_config.get("input"), str) and dependency_config["input"].startswith(output_db_table_name):
                    dependency_config["input"] = dependency_config["output"].split(" ")[0]
                    arguments[subquery_config["name"] + "@" + dependency_config["name"] + "@@batch"] = 1
                    arguments[subquery_config["name"] + "@" + dependency_config["name"] + "@@loop"] = True
            config["dependencys"].append(subquery_config)

    def compile_subquery(self, expression, arguments):
        if not isinstance(expression, sqlglot_expressions.Subquery):
            table_name = "anonymous"
        else:
            table_name = expression.args["alias"].args["this"].name if expression.args.get("alias") else "anonymous"
        if table_name.startswith("__subquery_"):
            subquery_name = table_name
        else:
            subquery_name = "__subquery_" + str(uuid.uuid1().int) + "_" + table_name
        subquery_arguments = {key: arguments[key] for key in CONST_CONFIG_KEYS if key in arguments}
        subquery_config = self.compile_query(expression if not isinstance(expression, sqlglot_expressions.Subquery)
                                             else expression.args["this"], subquery_arguments)
        subquery_config["output"] = "&.--." + subquery_name + "::" + subquery_config["output"].split("::")[-1].split(" use ")[0] + " use I"
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
            raise SyncanySqlCompileException('unknown insert into, related sql "%s"' % self.to_sql(expression))
        if not expression.args.get("expression"):
            raise SyncanySqlCompileException('unknown insert into, related sql "%s"' % self.to_sql(expression))

        if table_info["db"] == "-" and table_info["name"] == "_":
            config["output"] = "&.-.&1::id use I"
        else:
            update_type = "I"
            if table_info["typing_options"]:
                for typing_option in table_info["typing_options"]:
                    if typing_option.upper() not in ("I", "U", "UI", "UDI", "DI"):
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
                self.compile_select(self.optimize_rewrite(select_expression, config, arguments), config, arguments)
        elif isinstance(expression.args["expression"], sqlglot_expressions.Values):
            values_expression = expression.args["expression"]
            if not values_expression.args.get("expressions"):
                raise SyncanySqlCompileException('unknown insert into, related sql "%s"' % self.to_sql(expression))
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
            config["input"] = "&.--.--::" + columns[0][0]
            config["loader"] = "const_loader"
            config["loader_arguments"] = {"datas": datas}
        else:
            raise SyncanySqlCompileException('unknown insert into, related sql "%s"' % self.to_sql(expression))

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
        union_column_names, output_primary_key, output_type = None, None, "I"
        for select_expression in select_expressions:
            subquery_name = "__unionquery_" + str(uuid.uuid1().int)
            subquery_arguments = {key: arguments[key] for key in CONST_CONFIG_KEYS if key in arguments}
            subquery_config = self.compile_query(select_expression, subquery_arguments)
            output_primary_key_type_info = subquery_config["output"].split("::")[-1].split(" use ")
            subquery_config["output"] = "&.--." + query_name + "::" + output_primary_key_type_info[0] + " use I"
            subquery_config["name"] = subquery_config["name"] + "#" + subquery_name[2:]
            arguments.update({subquery_config["name"] + "@" + key: value for key, value in subquery_arguments.items()})
            if isinstance(subquery_config["schema"], dict):
                if not isinstance(config["schema"], dict):
                    config["schema"] = {}
                for name in subquery_config["schema"]:
                    config["schema"][name] = "$." + name
            if isinstance(subquery_config["schema"], dict):
                if union_column_names is None:
                    union_column_names = sorted(list(subquery_config["schema"].keys()))
                else:
                    subquery_column_names = sorted(list(subquery_config["schema"].keys()))
                    for i in range(min(len(union_column_names), len(subquery_column_names))):
                        if union_column_names[i] == subquery_column_names[i]:
                            continue
                        subquery_config["schema"][union_column_names[i]] = subquery_config["schema"][subquery_column_names[i]]
            if output_primary_key is None:
                output_primary_key = output_primary_key_type_info[0]
            if output_type == "I" and len(output_primary_key_type_info) >= 2:
                output_type = output_primary_key_type_info[1].strip()
            config["dependencys"].append(subquery_config)
        config["input"] = "&.--." + query_name + "::" + output_primary_key
        config["output"] = config["output"].split("::")[0] + "::" + output_primary_key + " use " + output_type
        arguments["@primary_order"] = False
        arguments["@limit"] = 0

    def compile_select(self, expression, config, arguments):
        primary_table = {"db": None, "name": None, "table_name": None, "table_alias": None, "seted_primary_keys": False,
                         "loader_primary_keys": [], "outputer_primary_keys": [], "columns": {}, "subquery": None, "select_columns": {}}

        from_expression = expression.args.get("from")
        if not from_expression:
            primary_table["db"] = "--"
            primary_table["name"] = "--"
            primary_table["table_alias"] = "--"
            primary_table["table_name"] = "--"
            config["loader"] = "const_loader"
            config["loader_arguments"] = {"datas": [{}]}
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
                    if from_expression.args.get("alias") else None
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
            if expression.args.get("joins") else {}
        windows = self.parse_windows(expression, config, arguments, primary_table, expression.args["windows"]) \
            if expression.args.get("windows") else {}
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

            column_expression, aggregate_expression, window_aggregate_expression, calculate_expression, column_alias = None, None, None, None, None
            if self.is_column(select_expression, config, arguments):
                column_expression = select_expression
            elif isinstance(select_expression, sqlglot_expressions.Alias):
                column_alias = select_expression.args["alias"].name if select_expression.args.get("alias") else None
                if column_alias and column_alias in self.mapping:
                    column_alias = self.mapping[column_alias]
                if self.is_const(select_expression.args["this"], config, arguments):
                    const_info = self.parse_const(select_expression.args["this"], config, arguments)
                    config["schema"][column_alias] = self.compile_const(select_expression.args["this"], config, arguments, const_info)
                    continue
                elif self.is_column(select_expression.args["this"], config, arguments):
                    column_expression = select_expression.args["this"]
                elif self.is_window_aggregate(select_expression.args["this"], config, arguments):
                    window_aggregate_expression = select_expression.args["this"]
                elif self.is_aggregate(select_expression.args["this"], config, arguments):
                    aggregate_expression = select_expression.args["this"]
                else:
                    calculate_expression = select_expression.args["this"]
            elif self.is_const(select_expression, config, arguments):
                const_info = self.parse_const(select_expression, config, arguments)
                column_alias = self.generate_sql(select_expression)
                config["schema"][column_alias] = self.compile_const(select_expression, config, arguments, const_info)
                continue
            elif self.is_window_aggregate(select_expression, config, arguments):
                column_alias = self.generate_sql(select_expression)
                window_aggregate_expression = select_expression
            elif self.is_aggregate(select_expression, config, arguments):
                column_alias = self.generate_sql(select_expression)
                aggregate_expression = select_expression
            elif self.is_calculate(select_expression, config, arguments):
                column_alias = self.generate_sql(select_expression)
                calculate_expression = select_expression
            else:
                raise SyncanySqlCompileException('error select column, must have an alias name, related sql "%s"'
                                                 % self.to_sql(expression))
            if column_expression:
                self.compile_select_column(column_expression, config, arguments, primary_table, column_alias,
                                           self.parse_column(column_expression, config, arguments), join_tables)
                continue
            if window_aggregate_expression:
                self.compile_window_column(window_aggregate_expression, config, arguments, primary_table, column_alias,
                                           [window_aggregate_expression], join_tables, windows)
                continue
            if aggregate_expression:
                self.compile_aggregate_column(aggregate_expression, config, arguments, primary_table, column_alias,
                                              group_expression, [aggregate_expression], join_tables)
                continue

            window_aggregate_expressions = []
            self.parse_window_aggregate(calculate_expression, config, arguments, window_aggregate_expressions)
            if window_aggregate_expressions:
                self.compile_window_column(calculate_expression, config, arguments, primary_table, column_alias,
                                           window_aggregate_expressions, join_tables, windows)
                continue

            aggregate_expressions = []
            self.parse_aggregate(calculate_expression, config, arguments, aggregate_expressions)
            if aggregate_expressions:
                self.compile_aggregate_column(calculate_expression, config, arguments, primary_table, column_alias,
                                              group_expression, aggregate_expressions, join_tables)
                continue
            self.compile_select_calculate_column(calculate_expression, config, arguments, primary_table, column_alias,
                                                 join_tables)

        where_expression = expression.args.get("where")
        if where_expression and isinstance(where_expression, sqlglot_expressions.Where):
            where_calculate_expression = self.compile_where_condition(where_expression.args["this"], config, arguments, primary_table, join_tables)
            if where_calculate_expression is not None:
                config["predicate"] = self.compile_where_condition_column(where_calculate_expression, config, arguments, primary_table, join_tables)
            self.parse_condition_typing_filter(expression, config, arguments)

        having_expression = expression.args.get("having")
        if having_expression:
            having_condition = self.compile_having_condition(having_expression.args["this"], config, arguments, primary_table)
            if having_condition:
                config["intercept"] = having_condition

        order_expression = expression.args.get("order")
        if order_expression:
            self.compile_order(order_expression.args["expressions"], config, arguments, primary_table)

        offset_value = max(int(expression.args.get("offset").args["expression"].args["this"]), 0) if expression.args.get("offset") else 0
        limit_value = int(expression.args.get("limit").args["expression"].args["this"]) if expression.args.get("limit") else 0
        if limit_value != 0 or offset_value > 0:
            if config["predicate"] or config["intercept"] or (config.get("aggregate") and
                                                              (config["aggregate"]["schema"] or config["aggregate"]["window_schema"])) \
                    or config["pipelines"] or limit_value <= 0:
                if limit_value != 0:
                    config["pipelines"].append([">>@array::slice", "$.*|array", offset_value,
                                                (offset_value + limit_value) if limit_value > 0 else limit_value])
                else:
                    config["pipelines"].append([">>@array::slice", "$.*|array", offset_value])
            else:
                if offset_value > 0:
                    config["pipelines"].append([">>@array::slice", "$.*|array", offset_value])
                arguments["@limit"] = offset_value + limit_value

        if group_expression and ("aggregate" not in config or not config["aggregate"] or not config["aggregate"]["schema"]):
            self.compile_group_column(group_expression, config, arguments, primary_table, join_tables)
        distinct_expression = expression.args.get("distinct")
        if distinct_expression and ("aggregate" not in config or not config["aggregate"] or not config["aggregate"]["schema"]):
            if not group_expression:
                self.compile_distinct_column(expression, config, arguments, primary_table, join_tables)
            else:
                if not isinstance(config["schema"], dict):
                    raise SyncanySqlCompileException(
                        'error distinct, select columns is unknown, related sql "%s"' % self.to_sql(expression))
                if "aggregate" not in config:
                    config["aggregate"] = copy.deepcopy(DEAULT_AGGREGATE)
                for name, column in config["schema"].items():
                    if name in config["aggregate"]["schema"]:
                        continue
                    config["aggregate"]["distinct_keys"].append(copy.deepcopy(column))
        if not from_expression and not primary_table["outputer_primary_keys"] and isinstance(config["schema"], dict):
            for column_alias in config["schema"]:
                if not column_alias.isidentifier():
                    continue
                primary_table["outputer_primary_keys"] = [column_alias]
                break
        config["input"] = "".join(["&.", primary_table["db"], ".", primary_table["name"], "::",
                                   "+".join(primary_table["loader_primary_keys"]) if primary_table["loader_primary_keys"] else "id",
                                   (" use " + arguments["@use_input"]) if arguments.get("@use_input") else ""])
        config["output"] = "".join([config["output"].split("::")[0], "::",
                                    "+".join(primary_table["outputer_primary_keys"]) if primary_table["outputer_primary_keys"] else "id",
                                    (" use " + config["output"].split(" use ")[-1]) if " use " in config["output"] else (
                                            " use " + (arguments.get("@use_output") or "I"))])

    def compile_pipleline_select(self, expression, config, arguments, primary_table):
        select_expressions = expression.args.get("expressions")
        if not select_expressions or len(select_expressions) != 1 or not isinstance(select_expressions[0], sqlglot_expressions.Anonymous):
            return None
        if expression.args.get("group") or expression.args.get("where") or expression.args.get("having") \
                or expression.args.get("order") or expression.args.get("limit") or expression.args.get("offset"):
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
                                   "+".join(primary_table["loader_primary_keys"]) if primary_table["loader_primary_keys"] else "id",
                                   (" use " + arguments["@use_input"]) if arguments.get("@use_input") else ""])
        config["output"] = "".join([config["output"].split("::")[0], "::",
                                    "+".join(primary_table["outputer_primary_keys"]) if primary_table["outputer_primary_keys"] else "id",
                                    (" use " + config["output"].split(" use ")[-1]) if " use " in config["output"] else (
                                            " use " + (arguments.get("@use_output") or "I"))])
        arguments["@primary_order"] = False
        arguments["@limit"] = 0
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
        primary_table["select_columns"][column_alias] = expression

        if not column_info["column_name"].isidentifier() or not column_alias.isidentifier():
            return None
        if "pk" in column_info["typing_options"]:
            if not primary_table["seted_primary_keys"]:
                primary_table["loader_primary_keys"], primary_table["outputer_primary_keys"], primary_table["seted_primary_keys"] = [], [], True
            primary_table["loader_primary_keys"].append(column_info["column_name"])
            primary_table["outputer_primary_keys"].append(column_alias)
        elif not primary_table["seted_primary_keys"] and not primary_table["loader_primary_keys"]:
            if not column_info["table_name"] or column_info["table_name"] == primary_table["table_name"]:
                primary_table["loader_primary_keys"] = [column_info["column_name"]]
                if not primary_table["outputer_primary_keys"]:
                    primary_table["outputer_primary_keys"] = [column_alias]

    def compile_select_calculate_column(self, expression, config, arguments, primary_table, column_alias, join_tables, parse_primary_key=True):
        calculate_fields = []
        self.parse_calculate(expression, config, arguments, primary_table, calculate_fields)
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
            self.compile_join_column_tables(expression, config, arguments, primary_table,
                                            [join_tables[calculate_table_name] for calculate_table_name in calculate_table_names],
                                            join_tables, column_join_tables)
            calculate_column = self.compile_calculate(expression, config, arguments, primary_table, column_join_tables)
            config["schema"][column_alias] = self.compile_join_column(expression, config, arguments, primary_table,
                                                                      calculate_column, column_join_tables)
        else:
            config["schema"][column_alias] = self.compile_calculate(expression, config, arguments, primary_table, [])
        primary_table["select_columns"][column_alias] = expression
        if parse_primary_key and not primary_table["seted_primary_keys"] and not primary_table["outputer_primary_keys"] and column_alias.isidentifier():
            loader_primary_keys = [calculate_field["column_name"] for calculate_field in calculate_fields
                                   if calculate_field["column_name"].isidentifier() and
                                   (not calculate_field["table_name"] or calculate_field["table_name"] == primary_table["table_name"])]
            if loader_primary_keys:
                primary_table["loader_primary_keys"], primary_table["outputer_primary_keys"] = loader_primary_keys, [column_alias]
            else:
                primary_table["outputer_primary_keys"] = [column_alias]

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
        jit_define_name = None
        if isinstance(column, list) and len(column) == 3 and column[0] == "#call" and column[1].startswith("jit_define_") and column[2] == "$.*":
            jit_define_name, column = column[1], (config["defines"][column[1]] if column[1] in config["defines"] else self.config.get()[column[1]])

        if isinstance(column, str):
            column = ":" + column
        elif isinstance(column, list):
            if column and isinstance(column[0], str):
                column[0] = ":" + column[0]
            else:
                column = [":"] + column
        else:
            column = [":", column]

        for i in range(len(column_join_tables)):
            join_table = column_join_tables[i]
            if len(join_table["calculate_expressions"]) == 1:
                join_columns = self.compile_calculate(join_table["calculate_expressions"][0], config, arguments,
                                                      primary_table, column_join_tables, i)
            else:
                join_columns = [self.compile_calculate(calculate_expression, config, arguments, primary_table,
                                                       column_join_tables, i)
                                for calculate_expression in join_table["calculate_expressions"]]

            if join_table["primary_keys"]:
                primary_keys = []
                for primary_key in join_table["primary_keys"]:
                    foreign_column_info = join_table["columns"].get(primary_key)
                    if foreign_column_info and isinstance(foreign_column_info, dict):
                        primary_keys.append(self.compile_foreign_column(expression, config, arguments, foreign_column_info))
                    else:
                        primary_keys.append(primary_key)
            else:
                primary_keys = ["id"]
            join_db_table = "&&." + join_table["db"] + "." + join_table["table"] + "::" + "+".join(primary_keys)
            if join_table["querys"]:
                join_db_table = [join_db_table, join_table["querys"]]

            if join_table["having_expressions"]:
                if len(join_table["having_expressions"]) == 1:
                    having_columns = self.compile_calculate(join_table["having_expressions"][0], config, arguments,
                                                            primary_table, column_join_tables, i - 1)
                else:
                    def compile_having_column(left_having_expression, right_having_expression):
                        left_having_column = self.compile_calculate(left_having_expression, config, arguments,
                                                                    primary_table, column_join_tables, i - 1)
                        if isinstance(right_having_expression, list):
                            if len(right_having_expression) > 1:
                                right_having_column = compile_having_column(right_having_expression[0],
                                                                            right_having_expression[1:])
                                return ["#if", left_having_column, right_having_column, ["#const", 0]]
                            right_having_expression = right_having_expression[0]
                        right_having_column = self.compile_calculate(right_having_expression, config, arguments,
                                                                     primary_table, column_join_tables, i - 1)
                        return ["#if", left_having_column, right_having_column, ["#const", 0]]
                    having_columns = compile_having_column(join_table["having_expressions"][0],
                                                           join_table["having_expressions"][1:])
                column = [join_columns, join_db_table, having_columns, column] \
                    if join_columns else [join_db_table, having_columns, column]
            else:
                column = [join_columns, join_db_table, column] if join_columns else [join_db_table, column]

        if jit_define_name:
            config["defines"][jit_define_name] = column
            return ["#call", jit_define_name, "$.*"]
        return column

    def compile_join_column_field(self, expression, config, arguments, primary_table, ci, join_column, column_join_tables):
        if not join_column["table_name"] or join_column["table_name"] == primary_table["table_name"]:
            return self.compile_column(expression, config, arguments, join_column, len(column_join_tables) - ci)
        ji = [j for j in range(len(column_join_tables)) if join_column["table_name"] == column_join_tables[j]["name"]][0]
        return self.compile_column(expression, config, arguments, join_column, ji - ci)

    def compile_where_condition(self, expression, config, arguments, primary_table, join_tables):
        if isinstance(expression, sqlglot_expressions.And):
            left_calculate_expression = self.compile_where_condition(expression.args.get("this"), config, arguments, primary_table, join_tables)
            right_calculate_expression = self.compile_where_condition(expression.args.get("expression"), config, arguments, primary_table, join_tables)
            if left_calculate_expression is None:
                if right_calculate_expression is None:
                    return None
                return right_calculate_expression
            if right_calculate_expression is None:
                return left_calculate_expression
            return expression
        if isinstance(expression, sqlglot_expressions.Or):
            return expression

        def has_column_expression(condition_expression):
            if not isinstance(condition_expression, sqlglot_expressions.Expression):
                return False
            if self.is_column(condition_expression, config, arguments):
                return True
            if self.is_subquery(condition_expression, config, arguments):
                return True
            for _, child_expression in condition_expression.args.items():
                if isinstance(child_expression, list):
                    for child_expression_item in child_expression:
                        if (not isinstance(child_expression_item, sqlglot_expressions.Expression) or
                                self.is_const(child_expression_item, config, arguments)):
                            continue
                        if has_column_expression(child_expression_item):
                            return True
                else:
                    if (not isinstance(child_expression, sqlglot_expressions.Expression) or
                            self.is_const(child_expression, config, arguments)):
                        continue
                    if has_column_expression(child_expression):
                        return True
            return False

        def parse_condition(expression):
            if expression.args.get("expressions"):
                left_expression, right_expression = expression.args["this"], expression.args["expressions"]
            elif expression.args.get("query"):
                left_expression, right_expression = expression.args["this"], expression.args["query"]
            elif expression.args.get("field"):
                left_expression, right_expression = expression.args["this"], expression.args["field"]
            else:
                left_expression, right_expression = expression.args["this"], expression.args["expression"]

            left_column = None
            if self.is_column(left_expression, config, arguments):
                left_column = self.parse_column(left_expression, config, arguments)
                if left_column["table_name"] and primary_table["table_name"] != left_column["table_name"]:
                    return None, None
            if left_column is None and self.is_column(right_expression, config, arguments):
                right_column = self.parse_column(right_expression, config, arguments)
                if right_column["table_name"] and primary_table["table_name"] != right_column["table_name"]:
                    return None, None
                left_expression, right_expression, left_column = right_expression, left_expression, right_column
            if left_column is None:
                return None, None

            if isinstance(right_expression, list):
                if all([self.is_const(value_expression_item, config, arguments) for value_expression_item in right_expression]):
                    value_items = [self.parse_const(value_expression_item, config, arguments)["value"]
                                   for value_expression_item in right_expression]
                    return left_column, ["#const", value_items]
                value_items = [self.compile_where_condition_column(value_expression_item, config, arguments, primary_table, join_tables)
                               for value_expression_item in right_expression]
                return left_column, ["#make", value_items]
            if self.is_subquery(right_expression, config, arguments):
                value_column = self.compile_query_condition(right_expression, config, arguments, primary_table, join_tables,
                                                            left_column["typing_filters"] if left_column else None)
                if isinstance(value_column, list) and value_column and isinstance(value_column[0], str) and value_column[0][:2] == "&.":
                    if isinstance(expression, sqlglot_expressions.In):
                        return left_column, ["@convert_array", value_column]
                    return left_column, ["@convert_array", value_column, ":$.:0"]
                return None, None
            if not has_column_expression(right_expression):
                right_calculater = self.compile_where_condition_column(right_expression, config, arguments, primary_table, join_tables)
                return left_column, right_calculater
            return None, None

        def build_query_condition(condition_column, condition_exp, condition_calculater):
            if condition_column["typing_name"] not in config["querys"]:
                config["querys"][condition_column["typing_name"]] = {}
            condition_querys = config["querys"][condition_column["typing_name"]]
            if isinstance(condition_querys, dict):
                if condition_exp not in condition_querys:
                    condition_querys[condition_exp] = condition_calculater
                    return None
                condition_querys = [condition_querys]
                config["querys"][condition_column["typing_name"]] = condition_querys
            for condition_query in condition_querys:
                if condition_exp not in condition_query:
                    condition_query[condition_exp] = condition_calculater
                    return None
            condition_querys.append({condition_exp: condition_calculater})
            return None

        if isinstance(expression, sqlglot_expressions.EQ):
            condition_column, right_calculater = parse_condition(expression)
            if condition_column is None or right_calculater is None:
                return expression
            build_query_condition(condition_column, "==", right_calculater)
            setattr(expression, "syncany_valuer", ["#const", 1])
            return None
        elif isinstance(expression, sqlglot_expressions.NEQ):
            condition_column, right_calculater = parse_condition(expression)
            if condition_column is None or right_calculater is None:
                return expression
            build_query_condition(condition_column, "!=", right_calculater)
            setattr(expression, "syncany_valuer", ["#const", 1])
            return None
        elif isinstance(expression, sqlglot_expressions.GT):
            condition_column, right_calculater = parse_condition(expression)
            if condition_column is None or right_calculater is None:
                return expression
            build_query_condition(condition_column, ">", right_calculater)
            setattr(expression, "syncany_valuer", ["#const", 1])
            return None
        elif isinstance(expression, sqlglot_expressions.GTE):
            condition_column, right_calculater = parse_condition(expression)
            if condition_column is None or right_calculater is None:
                return expression
            build_query_condition(condition_column, ">=", right_calculater)
            setattr(expression, "syncany_valuer", ["#const", 1])
            return None
        elif isinstance(expression, sqlglot_expressions.LT):
            condition_column, right_calculater = parse_condition(expression)
            if condition_column is None or right_calculater is None:
                return expression
            build_query_condition(condition_column, "<", right_calculater)
            setattr(expression, "syncany_valuer", ["#const", 1])
            return None
        elif isinstance(expression, sqlglot_expressions.LTE):
            condition_column, right_calculater = parse_condition(expression)
            if condition_column is None or right_calculater is None:
                return expression
            build_query_condition(condition_column, "<=", right_calculater)
            setattr(expression, "syncany_valuer", ["#const", 1])
            return None
        elif isinstance(expression, sqlglot_expressions.In):
            condition_column, right_calculater = parse_condition(expression)
            if condition_column is None or right_calculater is None:
                return expression
            build_query_condition(condition_column, "in", ["@convert_array", right_calculater])
            setattr(expression, "syncany_valuer", ["#const", 1])
            return None
        elif isinstance(expression, sqlglot_expressions.Paren):
            return self.compile_where_condition(expression.args.get("this"), config, arguments, primary_table, join_tables)
        else:
            return expression

    def compile_where_condition_column(self, expression, config, arguments, primary_table, join_tables):
        if self.is_column(expression, config, arguments):
            expression_column = self.parse_column(expression, config, arguments)
            if isinstance(config["schema"], dict) and not expression_column["table_name"] \
                    and expression_column["column_name"] in config["schema"]:
                return config["schema"][expression_column["column_name"]]
            if not expression_column["table_name"] or expression_column["table_name"] == primary_table["table_name"]:
                return self.compile_column(expression, config, arguments, expression_column)

        def parse_calculate_expression(condition_expression):
            if hasattr(condition_expression, "syncany_valuer"):
                return
            if not isinstance(condition_expression, sqlglot_expressions.Expression):
                return
            if self.is_subquery(condition_expression, config, arguments):
                condition_query_condition = self.compile_query_condition(condition_expression, config, arguments, primary_table, join_tables, None)
                setattr(condition_expression, "syncany_valuer", condition_query_condition)
                return
            if self.is_column(condition_expression, config, arguments) or self.is_const(condition_expression, config, arguments):
                return
            for _, child_expression in condition_expression.args.items():
                if isinstance(child_expression, list):
                    for child_expression_item in child_expression:
                        if not isinstance(child_expression_item, sqlglot_expressions.Expression) or self.is_const(child_expression_item, config, arguments):
                            continue
                        parse_calculate_expression(child_expression_item)
                else:
                    if not isinstance(child_expression, sqlglot_expressions.Expression) or self.is_const(child_expression, config, arguments):
                        continue
                    parse_calculate_expression(child_expression)
        parse_calculate_expression(expression)

        calculate_fields = []
        self.parse_calculate(expression, config, arguments, primary_table, calculate_fields)
        calculate_fields = [calculate_field for calculate_field in calculate_fields if calculate_field["table_name"]
                            and calculate_field["table_name"] != primary_table["table_name"]]
        if not calculate_fields:
            return self.compile_calculate(expression, config, arguments, primary_table, [])

        column_join_tables = []
        calculate_table_names = set([])
        for calculate_field in calculate_fields:
            if calculate_field["table_name"] not in join_tables:
                raise SyncanySqlCompileException('error join column, join table %s unknown, related sql "%s"' %
                                                 (calculate_field["table_name"], self.to_sql(expression)))
            calculate_table_names.add(calculate_field["table_name"])
        self.compile_join_column_tables(expression, config, arguments, primary_table,
                                        [join_tables[calculate_table_name] for calculate_table_name in
                                         calculate_table_names], join_tables, column_join_tables)
        calculate_column = self.compile_calculate(expression, config, arguments, primary_table, column_join_tables)
        return self.compile_join_column(expression, config, arguments, primary_table, calculate_column, column_join_tables)

    def compile_query_condition(self, expression, config, arguments, primary_table, join_tables, typing_filters):
        def parse_subquery_select_column_name(subquery_config, exclude_keys=None):
            if isinstance(subquery_config["schema"], dict):
                subquery_schema = subquery_config["schema"]
            else:
                subquery_schema = {}
                for dependency in subquery_config.get("dependencys", []):
                    if not isinstance(dependency, dict) or not isinstance(dependency["schema"], dict):
                        continue
                    subquery_schema.update(dependency["schema"])
            for key in subquery_schema:
                if exclude_keys and key in exclude_keys:
                    continue
                return key
            return "*"

        subquery_join_parent_expression, subquery_join_patent_columns, subquery_interceptor_expressions = self.parse_subquery(
            expression, config, arguments, primary_table, join_tables)
        func_expression = expression.parent
        is_not_extracting_value = (func_expression and isinstance(func_expression, (sqlglot_expressions.Anonymous,
                                                                               sqlglot_expressions.Binary, sqlglot_expressions.Condition))
                                   and func_expression.key.lower() in ("exists", "yield_array"))
        subquery_interceptor_column = None
        for interceptor_expression in subquery_interceptor_expressions[::-1]:
            interceptor_column = self.compile_query_condition_column(interceptor_expression, config, arguments, primary_table,
                                                                     join_tables, None, -2)
            if subquery_interceptor_column is None:
                subquery_interceptor_column = ["#if", interceptor_column, ["#const", 1], ["#const", 0]]
            else:
                subquery_interceptor_column = ["#if", interceptor_column, subquery_interceptor_column, ["#const", 0]]

        if not subquery_join_patent_columns or not subquery_join_parent_expression:
            subquery_name, subquery_config = self.compile_subquery(subquery_join_parent_expression or expression, arguments)
            if not isinstance(subquery_config, dict):
                raise SyncanySqlCompileException('error subquery, unknown select columns, related sql "%s"' % self.to_sql(expression))
            column_name = parse_subquery_select_column_name(subquery_config)
            db_table = "&.--." + subquery_name + "::" + column_name
            config["dependencys"].append(subquery_config)
            if is_not_extracting_value:
                column_name= "*"
            elif typing_filters:
                column_name = column_name + "|" + typing_filters
            if subquery_interceptor_column:
                return [db_table, subquery_interceptor_column, ":$." + column_name]
            return [db_table, ":$." + column_name]

        if join_tables is None:
            raise SyncanySqlCompileException('error subquery, Only supported in select and where, related sql "%s"' % self.to_sql(expression))
        subquery_name, subquery_config = self.compile_subquery(subquery_join_parent_expression, arguments)
        if not isinstance(subquery_config, dict):
            raise SyncanySqlCompileException('error subquery, unknown select columns, related sql "%s"' % self.to_sql(expression))
        join_keys, join_key_columns = [], []
        for _, join_parent_column in subquery_join_patent_columns.items():
            join_keys.append(join_parent_column["column_alias"])
            join_key_columns.append(self.compile_query_condition_column(join_parent_column["parent_column"]["expression"], config, arguments,
                                                                        primary_table, join_tables, join_parent_column["parent_column"]))
        if is_not_extracting_value:
            column_name = "*"
        else:
            column_name = parse_subquery_select_column_name(subquery_config, join_keys)
            if typing_filters:
                column_name = column_name + "|" + typing_filters
        if len(join_keys) == 1:
            return [join_key_columns[0], ["&.:=@execute_query_tasker::" + join_keys[0], subquery_interceptor_column or {}, {"task_config": subquery_config}],
                    subquery_interceptor_column, ":$." + column_name]
        return [join_key_columns, ["&.:=@execute_query_tasker::" + "+".join(join_keys), {}, {"task_config": subquery_config}],
                subquery_interceptor_column, ":$." + column_name]

    def compile_query_condition_column(self, expression, config, arguments, primary_table, join_tables, expression_column=None, join_index=-1):
        if not expression_column and self.is_column(expression, config, arguments):
            expression_column = self.parse_column(expression, config, arguments)
        if expression_column:
            if isinstance(config["schema"], dict) and not expression_column["table_name"] \
                    and expression_column["column_name"] in config["schema"]:
                return config["schema"][expression_column["column_name"]]
            if not expression_column["table_name"] or expression_column["table_name"] == primary_table["table_name"]:
                return self.compile_column(expression, config, arguments, expression_column)

        calculate_fields = []
        self.parse_calculate(expression, config, arguments, primary_table, calculate_fields)
        calculate_fields = [calculate_field for calculate_field in calculate_fields if calculate_field["table_name"]
                            and calculate_field["table_name"] != primary_table["table_name"]]
        if not calculate_fields:
            return self.compile_calculate(expression, config, arguments, primary_table, [], join_index=join_index)

        column_join_tables = []
        calculate_table_names = set([])
        for calculate_field in calculate_fields:
            if calculate_field["table_name"] not in join_tables:
                raise SyncanySqlCompileException('error join column, join table %s unknown, related sql "%s"' %
                                                 (calculate_field["table_name"], self.to_sql(expression)))
            calculate_table_names.add(calculate_field["table_name"])
        self.compile_join_column_tables(expression, config, arguments, primary_table,
                                        [join_tables[calculate_table_name] for calculate_table_name in
                                         calculate_table_names], join_tables, column_join_tables)
        calculate_column = self.compile_calculate(expression, config, arguments, primary_table, column_join_tables)
        return self.compile_join_column(expression, config, arguments, primary_table, calculate_column, column_join_tables)

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
            "key": group_column[0] if len(group_column) == 1 else ["#make", group_column, [":@aggregate_key", "$.*"]],
            "value": config["schema"][column_alias],
            "calculate": "$$.value",
            "aggregate": [":#aggregate", "$.key", "$$.value"],
            "reduce": "$$." + column_alias,
            "final_value": None
        }
        config["schema"][column_alias] = ["#make", {
            "key": copy.deepcopy(aggregate_column["key"]),
            "value": copy.deepcopy(aggregate_column["value"])
        }, [":#aggregate", "$.key", "$$.value"]]
        config["aggregate"]["key"] = copy.deepcopy(aggregate_column["key"])
        config["aggregate"]["schema"][column_alias] = aggregate_column

    def compile_distinct_column(self, expression, config, arguments, primary_table, join_tables):
        column_alias = None
        if primary_table["outputer_primary_keys"]:
            if isinstance(config["schema"], dict) and primary_table["outputer_primary_keys"][0] in config["schema"]:
                column_alias = primary_table["outputer_primary_keys"][0]
        if not column_alias:
            if isinstance(config["schema"], dict) and config["schema"]:
                column_alias = list(config["schema"].keys())[0]
            else:
                raise SyncanySqlCompileException('error group by, unknown column, related sql "%s"' % self.to_sql(expression))
        if "aggregate" not in config:
            config["aggregate"] = copy.deepcopy(DEAULT_AGGREGATE)
        group_column = []
        for select_name, select_column in config["schema"].items():
            if select_name in config["aggregate"]["schema"] or select_name in config["aggregate"]["window_schema"]:
                continue
            group_column.append(copy.deepcopy(select_column))
        aggregate_column = {
            "key": group_column[0] if len(group_column) == 1 else ["#make", group_column, [":@aggregate_key", "$.*"]],
            "value": config["schema"][column_alias],
            "calculate": "$$.value",
            "aggregate": [":#aggregate", "$.key", "$$.value"],
            "reduce": "$$." + column_alias,
            "final_value": None
        }
        config["schema"][column_alias] = ["#make", {
            "key": copy.deepcopy(aggregate_column["key"]),
            "value": copy.deepcopy(aggregate_column["value"])
        }, [":#aggregate", "$.key", "$$.value"]]
        config["aggregate"]["key"] = copy.deepcopy(aggregate_column["key"])
        config["aggregate"]["schema"][column_alias] = aggregate_column

    def compile_aggregate_column(self, expression, config, arguments, primary_table, column_alias, group_expression,
                                 aggregate_expressions, join_tables):
        group_column = self.compile_aggregate_key(group_expression, config, arguments, primary_table, join_tables)
        aggregate_value_expressions = []
        for i in range(len(aggregate_expressions)):
            aggregate_expression = aggregate_expressions[i]
            if isinstance(aggregate_expression, sqlglot_expressions.Anonymous):
                if aggregate_expression.args.get("expressions") and isinstance(aggregate_expression.args["expressions"][0],
                                                                               sqlglot_expressions.Distinct):
                    raise SyncanySqlCompileException('error aggregate distinct, only be used for the count function, related sql "%s"' %
                                                     self.to_sql(expression))
                else:
                    value_expressions = aggregate_expression.args.get("expressions")
            else:
                if isinstance(aggregate_expression.args["this"], sqlglot_expressions.Distinct):
                    if not isinstance(aggregate_expression, sqlglot_expressions.Count):
                        raise SyncanySqlCompileException('error aggregate distinct, only be used for the count function, related sql "%s"' %
                                                         self.to_sql(expression))
                    value_expressions = aggregate_expression.args.get("this").args.get("expressions")
                else:
                    value_expressions = [aggregate_expression.args.get("this")]
            aggregate_value_expressions.append(value_expressions)

        if "aggregate" not in config:
            config["aggregate"] = copy.deepcopy(DEAULT_AGGREGATE)
        if len(aggregate_expressions) == 1 and aggregate_expressions[0] is expression:
            value_column = self.compile_aggregate_value(aggregate_expressions[0], config, arguments, primary_table, join_tables,
                                                        aggregate_value_expressions[0])
            aggregate_calculate, reduce_calculate, final_calculate = self.compile_aggregate_calculate(aggregate_expressions[0])
            aggregate_column = {
                "key": group_column[0] if len(group_column) == 1 else ["#make", group_column, [":@aggregate_key", "$.*"]],
                "value": value_column[0] if len(value_column) == 1 else ["#make", value_column],
                "calculate": [aggregate_calculate, "$." + column_alias, "$$.value"],
                "aggregate": [":#aggregate", "$.key", [aggregate_calculate, "$." + column_alias, "$$.value"]],
                "reduce": [reduce_calculate, "$." + column_alias, "$$." + column_alias],
                "final_value": [final_calculate, "$." + column_alias] if final_calculate else None
            }
        else:
            value_column, calculate_column, reduce_column = ["#make", {}], ["#make", {}], ["#make", {}]
            for i in range(len(aggregate_expressions)):
                aggregate_value_key = "value_%d" % id(aggregate_expressions[i])
                aggregate_value_column = self.compile_aggregate_value(aggregate_expressions[i], config, arguments, primary_table,
                                                                      join_tables, aggregate_value_expressions[i])
                aggregate_calculate, reduce_calculate, final_calculate = self.compile_aggregate_calculate(aggregate_expressions[i])
                value_column[1][aggregate_value_key] = aggregate_value_column[0] if len(aggregate_value_column) == 1 else ["#make", aggregate_value_column]
                calculate_column[1][aggregate_value_key] = [aggregate_calculate, "$." + column_alias + "." + aggregate_value_key,
                                                            "$$.value." + aggregate_value_key]
                reduce_column[1][aggregate_value_key] = [reduce_calculate, "$." + column_alias + "." + aggregate_value_key,
                                                         "$$." + column_alias + "." + aggregate_value_key]
                setattr(aggregate_expressions[i], "syncany_valuer",
                        [final_calculate, "$." + column_alias + "." + aggregate_value_key]
                        if final_calculate else ("$." + column_alias + "." + aggregate_value_key))
            self.compile_select_calculate_column(expression, config, arguments, primary_table, column_alias,
                                                 join_tables, False)
            aggregate_column = {
                "key": group_column[0] if len(group_column) == 1 else ["#make", group_column, [":@aggregate_key", "$.*"]],
                "value": value_column,
                "calculate": calculate_column,
                "aggregate": [":#aggregate", "$.key", copy.deepcopy(calculate_column)],
                "reduce": reduce_column,
                "final_value": config["schema"][column_alias]
            }

        config["schema"][column_alias] = ["#make", {
            "key": copy.deepcopy(aggregate_column["key"]),
            "value": copy.deepcopy(aggregate_column["value"]),
        }, copy.deepcopy(aggregate_column["aggregate"])]
        config["aggregate"]["key"] = copy.deepcopy(aggregate_column["key"])
        config["aggregate"]["schema"][column_alias] = aggregate_column
        primary_table["select_columns"][column_alias] = expression

    def compile_aggregate_key(self, expression, config, arguments, primary_table, join_tables):
        if not expression:
            return [["#const", "__k_g__"]]

        group_column = []
        for group_expression in expression.args["expressions"]:
            if self.is_column(group_expression, config, arguments):
                group_expression_column = self.parse_column(group_expression, config, arguments)
                if isinstance(config["schema"], dict) and not group_expression_column["table_name"] \
                        and group_expression_column["column_name"] in config["schema"]:
                    group_column.append(config["schema"][group_expression_column["column_name"]])
                    continue
                if not group_expression_column["table_name"] or group_expression_column["table_name"] == primary_table["table_name"]:
                    group_column.append(self.compile_column(group_expression, config, arguments, group_expression_column))
                    continue

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
            return [["#const", 0]]
        if not value_expressions:
            return [["#const", None]]

        value_column = []
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

    def compile_aggregate_calculate(self, expression):
        if isinstance(expression, sqlglot_expressions.Count):
            if isinstance(expression.args.get("this"), sqlglot_expressions.Distinct):
                return "@aggregate_distinct_count::aggregate", "@aggregate_distinct_count::reduce", "@aggregate_distinct_count::final_value"
            return "@aggregate_count::aggregate", "@aggregate_count::reduce", None
        elif isinstance(expression, sqlglot_expressions.Sum):
            return "@aggregate_sum::aggregate", "@aggregate_sum::reduce", None
        elif isinstance(expression, sqlglot_expressions.Min):
            return "@aggregate_min::aggregate", "@aggregate_min::reduce", None
        elif isinstance(expression, sqlglot_expressions.Max):
            return "@aggregate_max::aggregate", "@aggregate_max::reduce", None
        elif isinstance(expression, sqlglot_expressions.Avg):
            return "@aggregate_avg::aggregate", "@aggregate_avg::reduce", "@aggregate_avg::final_value"
        elif isinstance(expression, sqlglot_expressions.GroupConcat):
            return "@group_concat::aggregate", "@group_concat::reduce", "@group_concat::final_value"
        elif isinstance(expression, sqlglot_expressions.GroupUniqArray):
            return "@group_uniq_array::aggregate", "@group_uniq_array::reduce", "@group_uniq_array::final_value"
        elif isinstance(expression, sqlglot_expressions.Anonymous):
            aggregate_funcs = {"grouparray": "group_array", "groupuniqarray": "group_uniq_array", "groupbitand": "group_bit_and",
                               "groupbitor": "group_bit_or", "groupbitxor": "group_bit_xor"}
            calculater_name = expression.args["this"].lower()
            if calculater_name in aggregate_funcs:
                calculater_name = aggregate_funcs[calculater_name]
            try:
                aggregate_calculater = find_aggregate_calculater(calculater_name)
                return ("@" + calculater_name + "::aggregate",
                        "@" + calculater_name + "::reduce",
                        ("@" + calculater_name + "::final_value" if hasattr(aggregate_calculater, "final_value") else None))
            except CalculaterUnknownException:
                raise SyncanySqlCompileException('unknown aggregate calculate, related sql "%s"' % self.to_sql(expression))
        else:
            raise SyncanySqlCompileException('unknown aggregate calculate, related sql "%s"' % self.to_sql(expression))

    def compile_window_column(self, expression, config, arguments, primary_table, column_alias, window_expressions,
                              join_tables, windows):
        if "aggregate" not in config:
            config["aggregate"] = copy.deepcopy(DEAULT_AGGREGATE)
        config["aggregate"]["window_schema"][column_alias] = {"aggregate": None, "final_value": None}
        for i in range(len(window_expressions)):
            window_expression = window_expressions[i]
            aggregate_expression = window_expression.args["this"]
            if isinstance(aggregate_expression, sqlglot_expressions.Anonymous):
                if aggregate_expression.args.get("expressions") and isinstance(aggregate_expression.args["expressions"][0], sqlglot_expressions.Distinct):
                    raise SyncanySqlCompileException('error window aggregate, only be used for the count function, related sql "%s"' %
                                                     self.to_sql(expression))
                else:
                    value_expressions = aggregate_expression.args.get("expressions")
            else:
                if isinstance(aggregate_expression.args.get("this"), sqlglot_expressions.Distinct):
                    if not isinstance(aggregate_expression, sqlglot_expressions.Count):
                        raise SyncanySqlCompileException('error window aggregate, only be used for the count function, related sql "%s"' %
                                                         self.to_sql(expression))
                    value_expressions = aggregate_expression.args.get("this").args.get("expressions")
                else:
                    if aggregate_expression.args.get("this"):
                        value_expressions = [aggregate_expression.args.get("this")]
                    else:
                        value_expressions = []

            if window_expression.args.get("alias"):
                window_alias_name = window_expression.args["alias"].args["this"]
                if window_alias_name not in windows:
                    raise SyncanySqlCompileException('error window aggregate, unknown window alias name, related sql "%s"'
                                                     % self.to_sql(expression))
                window = windows[window_alias_name]
                partition_expression, order_expression = window.args.get("partition_by"), window.args.get("order")
            else:
                partition_expression = window_expression.args.get("partition_by")
                order_expression = window_expression.args.get("order")
            partition_column = self.compile_window_key(partition_expression, config, arguments, primary_table,
                                                       join_tables) if partition_expression else None
            order_column = self.compile_window_order(order_expression, config, arguments, primary_table,
                                                     join_tables) if order_expression else None
            value_column = self.compile_window_value(window_expression.args["this"], config, arguments, primary_table,
                                                     join_tables, value_expressions) if value_expressions else None
            is_window_aggregate_calculate, aggregate_calculate, final_calculate = self.compile_window_calculate(window_expression)
            partition_calculate = "$.partition_key" if partition_column is not None else None
            if order_column is not None:
                order_calculate = {"valuer": "$.order_key", "orders": order_column["orders"]}
            elif is_window_aggregate_calculate:
                order_calculate = {"valuer": ["#const", 0], "orders": []}
            else:
                order_calculate = None
            value_calculate = "$.value" if value_column is not None else None
            if is_window_aggregate_calculate:
                aggregate_calculate = [":#partition", partition_calculate, order_calculate, value_calculate,
                                       [aggregate_calculate, "$.state", "$.value", "$.context"]]
            else:
                aggregate_calculate = [":#partition", partition_calculate, order_calculate, value_calculate,
                                       [aggregate_calculate, "$.state", "$.value"]]
            if final_calculate:
                aggregate_calculate.append([":" + final_calculate, "$.*"])
            window_aggregate_column = ["#make", {}, aggregate_calculate]
            if partition_column is not None:
                window_aggregate_column[1]["partition_key"] = partition_column[0] \
                    if len(partition_column) == 1 else ["#make", partition_column, [":@aggregate_key", "$.*"]]
            if order_column is not None:
                window_aggregate_column[1]["order_key"] = order_column["valuer"][0] \
                    if len(order_column["valuer"]) == 1 else ["#make", order_column["valuer"], [":@aggregate_key", "$.*"]]
            if value_column is not None:
                window_aggregate_column[1]["value"] = value_column[0] if len(value_column) == 1 else ["#make", value_column]

            if len(window_expressions) == 1 and window_expressions[0] is expression:
                config["schema"][column_alias] = window_aggregate_column
                config["aggregate"]["window_schema"][column_alias]["aggregate"] = copy.deepcopy(window_aggregate_column)
            else:
                aggregate_column_alias = "__window_aggregate_value_%d__" % id(window_expression)
                config["schema"][aggregate_column_alias] = window_aggregate_column
                setattr(window_expression, "syncany_valuer", "$." + aggregate_column_alias)
                if not isinstance(config["aggregate"]["window_schema"][column_alias]["aggregate"], dict):
                    config["aggregate"]["window_schema"][column_alias]["aggregate"] = {}
                config["aggregate"]["window_schema"][column_alias]["aggregate"][aggregate_column_alias] = copy.deepcopy(window_aggregate_column)

        if not (len(window_expressions) == 1 and window_expressions[0] is expression):
            self.compile_select_calculate_column(expression, config, arguments, primary_table, column_alias, join_tables, False)
            config["aggregate"]["window_schema"][column_alias]["final_value"] = config["schema"].pop(column_alias, None)
        primary_table["select_columns"][column_alias] = expression

    def compile_window_key(self, expressions, config, arguments, primary_table, join_tables):
        if not expressions:
            return [["#const", "__k_g__"]]

        key_column = []
        for expression in expressions:
            if self.is_column(expression, config, arguments):
                key_expression_column = self.parse_column(expression, config, arguments)
                if isinstance(config["schema"], dict) and not key_expression_column["table_name"] \
                        and key_expression_column["column_name"] in config["schema"]:
                    key_column.append(config["schema"][key_expression_column["column_name"]])
                    continue
                if not key_expression_column["table_name"] or key_expression_column["table_name"] == primary_table["table_name"]:
                    key_column.append(self.compile_column(expression, config, arguments, key_expression_column))
                    continue

            calculate_fields = []
            self.parse_calculate(expression, config, arguments, primary_table, calculate_fields)
            calculate_fields = [calculate_field for calculate_field in calculate_fields if calculate_field["table_name"]
                                and calculate_field["table_name"] != primary_table["table_name"]]
            if not calculate_fields:
                key_column.append(self.compile_calculate(expression, config, arguments, primary_table, []))
                continue

            column_join_tables = []
            calculate_table_names = set([])
            for calculate_field in calculate_fields:
                if calculate_field["table_name"] not in join_tables:
                    raise SyncanySqlCompileException('error join column, join table %s unknown, related sql "%s"' %
                                                     (calculate_field["table_name"], self.to_sql(expression)))
                calculate_table_names.add(calculate_field["table_name"])
            self.compile_join_column_tables(expression, config, arguments, primary_table,
                                            [join_tables[calculate_table_name] for calculate_table_name in calculate_table_names],
                                            join_tables, column_join_tables)
            calculate_column = self.compile_calculate(expression, config, arguments, primary_table, column_join_tables)
            key_column.append(self.compile_join_column(expression, config, arguments, primary_table,
                                                         calculate_column, column_join_tables))
        return key_column

    def compile_window_order(self, expression, config, arguments, primary_table, join_tables):
        value_order_expressions, orders = [], []
        for order_expression in expression.args["expressions"]:
            order_key = self.generate_sql(order_expression.args["this"])
            orders.append([order_key, order_expression.args.get("desc")])
            value_order_expressions.append(order_expression.args["this"])
        return {"valuer": self.compile_window_key(value_order_expressions, config, arguments,
                                                  primary_table, join_tables), "orders": orders}

    def compile_window_value(self, expression, config, arguments, primary_table, join_tables, value_expressions):
        if isinstance(expression, sqlglot_expressions.Count) and isinstance(expression.args["this"],
                                                                            sqlglot_expressions.Star):
            return [["#const", 0]]
        if not value_expressions:
            return [["#const", None]]

        value_column = []
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

    def compile_window_calculate(self, expression):
        if isinstance(expression.args["this"], sqlglot_expressions.RowNumber):
            return True, "@row_number::order_aggregate", None
        elif isinstance(expression.args["this"], sqlglot_expressions.Anonymous):
            calculater_name = expression.args["this"].args["this"].lower()
            try:
                aggregate_calculater = find_window_aggregate_calculater(calculater_name)
                return (True, "@" + calculater_name + "::order_aggregate",
                        ("@" + calculater_name + "::final_value" if hasattr(aggregate_calculater, "final_value") else None))
            except CalculaterUnknownException:
                aggregate_calculate, reduce_calculate, final_calculate = self.compile_aggregate_calculate(expression.args["this"])
                return False, aggregate_calculate, final_calculate
        else:
            aggregate_calculate, reduce_calculate, final_calculate = self.compile_aggregate_calculate(expression.args["this"])
            return False, aggregate_calculate, final_calculate
        
    def compile_calculate(self, expression, config, arguments, primary_table, column_join_tables, join_index=-1):
        if hasattr(expression, "syncany_valuer"):
            return expression.syncany_valuer
        if hasattr(expression, 'syncany_column'):
            return self.compile_join_column_field(expression, config, arguments, primary_table, join_index,
                                                  expression.syncany_column, column_join_tables)
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
                        column.append((":$.:" + str(int(get_value_key))) if isinstance(get_value_key, NumberTypes) else (":$." + str(get_value_key)))
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

            if calculater_name == "jit":
                if len(expression.args["expressions"]) != 1:
                    raise SyncanySqlCompileException(
                        'unknown calculate expression, jit run must by one expression related sql "%s"' % self.to_sql(expression))
                define_name = "jit_define_" + str(self.generate_sql(expression).__hash__()).replace("-", "_")
                if "defines" not in config:
                    config["defines"] = {}
                if self.is_const(expression.args["expressions"][0], config, arguments):
                    config["defines"][define_name] = self.compile_const(expression.args["expressions"][0], config, arguments,
                                                                        self.parse_const(expression.args["expressions"][0], config, arguments))
                else:
                    config["defines"][define_name] = self.compile_calculate(expression.args["expressions"][0], config, arguments, primary_table, column_join_tables, join_index)
                return ["#call", define_name, "$.*"]

            if calculater_name not in ("add", "sub", "mul", "div", "mod") and is_mysql_func(calculater_name):
                column = ["@mysql::" + calculater_name]
            else:
                if calculater_name[:8] == "convert_":
                    if calculater_name[8:] in self.TYPE_FILTERS:
                        calculater_name = "convert_" + self.TYPE_FILTERS[calculater_name[8:]]
                    if calculater_name.startswith("convert_str"):
                        column = ["@convert_string|str"]
                    else:
                        column = ["@" + calculater_name + "|" + calculater_name[8:]]
                else:
                    calculater_modules = calculater_name.split("$")
                    column = ["@" + calculater_modules[0] + (("::" + ".".join(calculater_modules[1:])) if len(calculater_modules) >= 2 else "")]
            for arg_expression in expression.args.get("expressions", []):
                if self.is_const(arg_expression, config, arguments):
                    column.append(self.compile_const(arg_expression, config, arguments,
                                                     self.parse_const(arg_expression, config, arguments)))
                else:
                    column.append(self.compile_calculate(arg_expression, config, arguments, primary_table, column_join_tables, join_index))

            try:
                find_generate_calculater(calculater_name)
                return ["#yield", column]
            except CalculaterUnknownException:
                pass
            return column
        elif isinstance(expression, sqlglot_expressions.JSONExtract):
            return [
                "@mysql::json_extract" if is_mysql_func("json_extract") else "@json_extract",
                self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index),
                self.compile_calculate(expression.args["expression"], config, arguments, primary_table, column_join_tables, join_index),
            ]
        elif isinstance(expression, sqlglot_expressions.Cast):
            typing_filter = self.parse_cast_typing_filter(expression, config, arguments)
            value_column = self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index)
            if typing_filter:
                return ["#if", ["#const", 1], value_column, None, ":$.*|" + typing_filter]
            return value_column
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
                                       join_index) if expression.args.get("true") else ["#const", 1],
                self.compile_calculate(expression.args["false"], config, arguments, primary_table, column_join_tables, 
                                       join_index) if expression.args.get("false") else ["#const", 0],
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
            if expression.args.get("this") and all([self.is_const(case_expression.args["this"], config, arguments)
                                                    for case_expression in expression.args.get("ifs", [])]):
                cases = {}
                cases["#case"] = self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index)
                if expression.args.get("ifs"):
                    for case_expression in expression.args["ifs"]:
                        case_value = self.parse_const(case_expression.args["this"], config, arguments)["value"]
                        cases[(":" + str(case_value)) if isinstance(case_value, NumberTypes) and not isinstance(case_value, bool) else case_value] = \
                            self.compile_calculate(case_expression.args["true"], config, arguments, primary_table, column_join_tables, join_index)
                cases["#end"] = self.compile_calculate(expression.args["default"], config, arguments, primary_table, column_join_tables, join_index) \
                    if expression.args.get("default") else None
                return cases

            value_column = self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index) \
                if expression.args.get("this") else None
            defaul_column = self.compile_calculate(expression.args["default"], config, arguments, primary_table, column_join_tables, join_index) \
                if expression.args.get("default") else ["#const", 0]
            if not expression.args.get("ifs"):
                return defaul_column
            def build_if(if_expression, case_expressions):
                if_value_column = self.compile_calculate(if_expression.args["this"], config, arguments, primary_table, column_join_tables, join_index)
                return [
                    "#if",
                    if_value_column if value_column is None else ["@mysql::eq", value_column, if_value_column],
                    self.compile_calculate(if_expression.args["true"], config, arguments, primary_table, column_join_tables,
                                           join_index) if if_expression.args.get("true") else ["#const", 1],
                    build_if(case_expressions[0], case_expressions[1:]) if case_expressions else defaul_column,
                ]
            return build_if(expression.args["ifs"][0], expression.args["ifs"][1:])
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
        elif self.is_subquery(expression, config, arguments):
            return self.compile_query_condition(expression, config, arguments, primary_table,
                                                {column_join_table["name"]: column_join_table for column_join_table in column_join_tables},
                                                None)
        elif isinstance(expression, sqlglot_expressions.Paren):
            return self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index)
        elif isinstance(expression, sqlglot_expressions.Between):
            return ["#make", {
                "key_value": self.compile_calculate(expression.args["this"], config, arguments, primary_table, []),
                "low_value": self.compile_calculate(expression.args["low"], config, arguments, primary_table, []),
                "high_value": self.compile_calculate(expression.args["high"], config, arguments, primary_table, [])
            }, [":#if", ["@mysql::gte", "$.key_value", "$.low_value"], ["@mysql::lte", "$.key_value", "$.high_value"], ["#const", 0]]]
        elif self.is_column(expression, config, arguments):
            join_column = self.parse_column(expression, config, arguments)
            return self.compile_join_column_field(expression, config, arguments, primary_table, join_index, 
                                                  join_column, column_join_tables)
        elif isinstance(expression, sqlglot_expressions.Star):
            return "$.*"
        elif isinstance(expression, sqlglot_expressions.Tuple):
            if all([self.is_const(tuple_expression, config, arguments) for tuple_expression in expression.args["expressions"]]):
                value_columns = [self.parse_const(tuple_expression, config, arguments)["value"]
                                 for tuple_expression in expression.args["expressions"]]
                return ["#const", value_columns]
            value_columns = [self.compile_calculate(tuple_expression, config, arguments, primary_table, column_join_tables, join_index)
                             for tuple_expression in expression.args["expressions"]]
            return ["#make", value_columns]
        elif isinstance(expression, sqlglot_expressions.Interval):
            return ["#const", {"value": expression.args["this"].args["this"], "unit": expression.args["unit"].args["this"]}]
        elif self.is_const(expression, config, arguments):
            return self.compile_const(expression, config, arguments, self.parse_const(expression, config, arguments))
        elif isinstance(expression, sqlglot_expressions.Not):
            if isinstance(expression.args["this"], sqlglot_expressions.Is):
                return [
                    "@mysql::is_not",
                    self.compile_calculate(expression.args["this"].args["this"], config, arguments, primary_table,
                                           column_join_tables, join_index),
                    self.compile_calculate(expression.args["this"].args["expression"], config, arguments, primary_table,
                                           column_join_tables, join_index)
                ]
            return ["@mysql::not", self.compile_calculate(expression.args["this"], config, arguments, primary_table,
                                                          column_join_tables, join_index)]
        elif isinstance(expression, sqlglot_expressions.Parameter):
            name = expression.args["this"].args["this"]
            if name in self.mapping:
                name = self.mapping[name]
            if isinstance(expression, AssignParameter):
                return ["@current_env_variable::set_value", ["#const", "@" + name],
                        self.compile_calculate(expression.args.get("expression"), config, arguments, primary_table,
                                               column_join_tables, join_index)]
            return ["@current_env_variable::get_value", ["#const", "@" + name]]
        elif isinstance(expression, (sqlglot_expressions.Binary, sqlglot_expressions.Condition)):
            if isinstance(expression, sqlglot_expressions.And):
                return [
                    "#if",
                    self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index),
                    self.compile_calculate(expression.args["expression"], config, arguments, primary_table, column_join_tables, join_index),
                    ["#const", 0]
                ]
            if isinstance(expression, sqlglot_expressions.Or):
                return [
                    "#if",
                    self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index),
                    ["#const", 1],
                    self.compile_calculate(expression.args["expression"], config, arguments, primary_table, column_join_tables, join_index)
                ]

            func_name = expression.key.lower()
            if expression.args.get("expressions"):
                return [
                    ("@mysql::" + func_name) if is_mysql_func(func_name) else ("@" + func_name),
                    self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index),
                    ["#make", [self.compile_calculate(item_expression, config, arguments, primary_table, column_join_tables, join_index)
                               for item_expression in expression.args["expressions"]]]
                ]
            if expression.args.get("query"):
                query_value_calculate = self.compile_calculate(expression.args["query"], config, arguments, primary_table, column_join_tables, join_index)
                return [
                    ("@mysql::" + func_name) if is_mysql_func(func_name) else (":@" + func_name),
                    self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index),
                    ["@convert_array", query_value_calculate] if isinstance(expression, sqlglot_expressions.In) else query_value_calculate
                ]
            if not expression.args.get("expression"):
                return [
                    ("@mysql::" + func_name) if is_mysql_func(func_name) else ("@" + func_name),
                    self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index)
                ]
            if isinstance(expression.args["expression"], sqlglot_expressions.Interval):
                func_name = "date" + func_name
            value_calculate = self.compile_calculate(expression.args["expression"], config, arguments, primary_table, column_join_tables, join_index)
            return [
                ("@mysql::" + func_name) if is_mysql_func(func_name) else ("@" + func_name),
                self.compile_calculate(expression.args["this"], config, arguments, primary_table, column_join_tables, join_index),
                ["@convert_array", value_calculate] if isinstance(expression, sqlglot_expressions.In) else value_calculate
            ]
        else:
            raise SyncanySqlCompileException('unknown calculate expression, related sql "%s"' % self.to_sql(expression))

    def compile_having_condition(self, expression, config, arguments, primary_table):
        if isinstance(expression, sqlglot_expressions.And):
            return [
                "#if",
                self.compile_having_condition(expression.args.get("this"), config, arguments, primary_table),
                self.compile_having_condition(expression.args.get("expression"), config, arguments, primary_table),
                ["#const", 0]
            ]
        if isinstance(expression, sqlglot_expressions.Or):
            return [
                "#if",
                self.compile_having_condition(expression.args.get("this"), config, arguments, primary_table),
                ["#const", 1],
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
                left_expression, right_expression = expression.args["this"], expression.args.get("expression")
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
            if not right_expression:
                return left_calculater, None

            if isinstance(right_expression, list):
                if all([self.is_const(value_expression_item, config, arguments) for value_expression_item in right_expression]):
                    value_items = [self.parse_const(value_expression_item, config, arguments)["value"]
                                   for value_expression_item in right_expression]
                    return left_calculater, ["#const", value_items]
                value_items = [self.compile_calculate(value_expression_item, config, arguments, primary_table, [])
                               for value_expression_item in right_expression]
                return left_calculater, ["#make", value_items]
            if self.is_column(right_expression, config, arguments):
                right_column = self.parse_column(right_expression, config, arguments)
                if isinstance(config["schema"], dict) and right_column["column_name"] not in config["schema"]:
                    raise SyncanySqlCompileException('unknown having condition column, column must be in the query result, related sql "%s"'
                                                     % self.to_sql(expression))
                config["aggregate"]["having_columns"].add(right_column["column_name"])
                return left_calculater, self.compile_column(right_expression, config, arguments, right_column)
            if self.is_subquery(right_expression, config, arguments):
                value_column = self.compile_query_condition(right_expression, config, arguments, primary_table, None,
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
            return ["@mysql::eq", left_calculater, right_calculater]
        elif isinstance(expression, sqlglot_expressions.NEQ):
            left_calculater, right_calculater = parse(expression)
            return ["@mysql::neq", left_calculater, right_calculater]
        elif isinstance(expression, sqlglot_expressions.GT):
            left_calculater, right_calculater = parse(expression)
            return ["@mysql::gt", left_calculater, right_calculater]
        elif isinstance(expression, sqlglot_expressions.GTE):
            left_calculater, right_calculater = parse(expression)
            return ["@mysql::gte", left_calculater, right_calculater]
        elif isinstance(expression, sqlglot_expressions.LT):
            left_calculater, right_calculater = parse(expression)
            return ["@mysql::lt", left_calculater, right_calculater]
        elif isinstance(expression, sqlglot_expressions.LTE):
            left_calculater, right_calculater = parse(expression)
            return ["@mysql::lte", left_calculater, right_calculater]
        elif isinstance(expression, sqlglot_expressions.In):
            left_calculater, right_calculater = parse(expression)
            return ["@mysql::in", left_calculater, ["@convert_array", right_calculater]]
        elif isinstance(expression, sqlglot_expressions.Like):
            left_calculater, right_calculater = parse(expression)
            if not isinstance(right_calculater, list) or len(right_calculater) != 2 or right_calculater[0] != "#const":
                raise SyncanySqlCompileException('error having condition, like condition value must be const, related sql "%s"'
                                                 % self.to_sql(expression))
            return ["#if", ["@re::match", ".*" if right_calculater[1] == "%%" else right_calculater[1].replace("%", ".*")
                .replace(".*.*", "%"), left_calculater], ["#const", 1], ["#const", 0]]
        elif isinstance(expression, sqlglot_expressions.Paren):
            return self.compile_having_condition(expression.args.get("this"), config, arguments, primary_table)
        else:
            left_calculater, right_calculater = parse(expression)
            func_name = expression.key.lower()
            if expression.args.get("expression") and isinstance(expression.args["expression"], sqlglot_expressions.Interval):
                func_name = "date" + func_name
            if right_calculater is None:
                return [("@mysql::" + func_name) if is_mysql_func(func_name) else ("@" + func_name), left_calculater]
            return [("@mysql::" + func_name) if is_mysql_func(func_name) else ("@" + func_name), left_calculater, right_calculater]

    def compile_order(self, expression, config, arguments, primary_table):
        primary_sort_keys, sort_keys = [], []
        for order_expression in expression:
            if not self.is_column(order_expression.args["this"], config, arguments):
                if isinstance(config["schema"], dict):
                    if self.generate_sql(order_expression.args["this"]) in config["schema"]:
                        sort_keys.append((self.generate_sql(order_expression.args["this"]), True if order_expression.args["desc"] else False))
                        continue
                    has_column = False
                    for column_alias, column_expression in primary_table["select_columns"].items():
                        if self.generate_sql(order_expression.args["this"]).lower() == self.generate_sql(column_expression).lower():
                            sort_keys.append((column_alias, True if order_expression.args["desc"] else False))
                            has_column = True
                            break
                    if has_column:
                        continue
                raise SyncanySqlCompileException('unknown order by column, the order by field must appear in the select field, related sql "%s"'
                                                 % self.to_sql(expression))
            column = self.parse_column(order_expression.args["this"], config, arguments)
            if not isinstance(config["schema"], dict) or (column["table_name"] and primary_table["table_name"] == column["table_name"]):
                primary_sort_keys.append([column["column_name"], True if order_expression.args["desc"] else False])
            elif not column["table_name"] and not primary_table["table_alias"]:
                if not config.get("aggregate"):
                    if column["column_name"] in primary_table["columns"] or column["column_name"] not in config["schema"]:
                        primary_sort_keys.append([column["column_name"], True if order_expression.args["desc"] else False])
                elif column["column_name"] not in config["aggregate"]["schema"] and column["column_name"] not in config["aggregate"]["window_schema"]:
                    if column["column_name"] in primary_table["columns"] or column["column_name"] not in config["schema"]:
                        primary_sort_keys.append([column["column_name"], True if order_expression.args["desc"] else False])
            elif not column["table_name"] and primary_table["table_alias"]:
                if not config.get("aggregate"):
                    if column["column_name"] in primary_table["columns"] and column["column_name"] not in config["schema"]:
                        primary_sort_keys.append([column["column_name"], True if order_expression.args["desc"] else False])
                elif column["column_name"] not in config["aggregate"]["schema"] and column["column_name"] not in config["aggregate"]["window_schema"]:
                    if column["column_name"] in primary_table["columns"] and column["column_name"] not in config["schema"]:
                        primary_sort_keys.append([column["column_name"], True if order_expression.args["desc"] else False])
            sort_keys.append((column["column_name"], True if order_expression.args["desc"] else False))
        if sort_keys and len(primary_sort_keys) < len(sort_keys):
            if isinstance(config["schema"], dict):
                for sort_key, _ in sort_keys:
                    if sort_key in config["schema"]:
                        continue
                    raise SyncanySqlCompileException('unknown order by column, related sql "%s"' % self.to_sql(expression))
            config["pipelines"].append([">>@sort", "$.*|array", False, sort_keys])
        if primary_sort_keys:
            config["orders"].extend(primary_sort_keys)

    def compile_foreign_column(self, expression, config, arguments, column):
        if column.get("typing_filters"):
            return column["column_name"] + "|" + column["typing_filters"][0]
        return column["column_name"]
        
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
            return literal["value_getter"]
        return ["#const", literal["value"]]

    def parse_joins(self, expression, config, arguments, primary_table, join_expressions):
        join_tables = {}
        for join_expression in join_expressions:
            if (join_expression.kind and join_expression.kind.lower() != "cross") \
                    or (join_expression.side and join_expression.side.lower() != "left"):
                raise SyncanySqlCompileException('unknown join expression, unsupported join type, related sql "%s"' % self.to_sql(join_expression))
            if isinstance(join_expression.args["this"], sqlglot_expressions.Table):
                table_info = self.parse_table(join_expression.args["this"], config, arguments)
                db, table, subquery_config = table_info["db"], table_info["name"], None
            elif isinstance(join_expression.args["this"], sqlglot_expressions.Subquery):
                subquery_name, subquery_config = self.compile_subquery(join_expression.args["this"], arguments)
                db, table = "--", subquery_name
                config["dependencys"].append(subquery_config)
            else:
                raise SyncanySqlCompileException('unknown join expression, related sql "%s"' % self.to_sql(join_expression))
            if not join_expression.args["this"].args.get("alias"):
                name = db + "." + table
            else:
                name = join_expression.args["this"].args["alias"].args["this"].name
            join_table = {
                "db": db, "table": table, "name": name, "primary_keys": [], "columns": {},
                "join_columns": [], "calculate_expressions": [], "querys": {}, "ref_count": 0,
                "having_expressions": [], "subquery": subquery_config
            }
            if join_expression.args.get("on"):
                self.parse_on_condition(join_expression.args["on"], config, arguments, primary_table, join_table)
                self.parse_condition_typing_filter(expression, join_table, arguments)
            if not join_table["primary_keys"]:
                if join_table["columns"]:
                    join_table["primary_keys"] = list(join_table["columns"].keys())
                else:
                    def find_primary_keys(item_expression, primary_keys):
                        if not isinstance(item_expression, sqlglot_expressions.Expression):
                            return
                        if self.is_column(item_expression, config, arguments):
                            item_column = self.parse_column(item_expression, config, arguments)
                            if item_column["table_name"] and item_column["table_name"] == join_table["name"]:
                                if item_column["column_name"] not in primary_keys:
                                    primary_keys.append(item_column["column_name"])
                            return
                        for child_item_expressions in item_expression.args.values():
                            if isinstance(child_item_expressions, list):
                                for child_item_expression in child_item_expressions:
                                    if not isinstance(child_item_expression, sqlglot_expressions.Expression) or self.is_const(child_item_expression, config, arguments):
                                        continue
                                    if self.is_subquery(child_item_expression, config, arguments):
                                        continue
                                    find_primary_keys(child_item_expression, primary_keys)
                            else:
                                if not isinstance(child_item_expressions, sqlglot_expressions.Expression) or self.is_const(child_item_expressions, config, arguments):
                                    continue
                                if self.is_subquery(child_item_expressions, config, arguments):
                                    continue
                                find_primary_keys(child_item_expressions, primary_keys)

                    primary_keys = []
                    find_primary_keys(join_expression.args.get("on"), primary_keys)
                    if not primary_keys:
                        find_primary_keys(expression, primary_keys)
                        if not primary_keys:
                            raise SyncanySqlCompileException('unknown join expression, related sql "%s"' % self.to_sql(join_expression))
                        primary_keys = primary_keys[:1]
                    join_table["primary_keys"] = primary_keys
            join_tables[join_table["name"]] = join_table

        for name, join_table in join_tables.items():
            if not join_table["join_columns"]:
                continue
            for join_column in join_table["join_columns"]:
                if not join_column["table_name"] or join_column["table_name"] == primary_table["table_name"]:
                    continue
                if join_column["table_name"] not in join_tables:
                    raise SyncanySqlCompileException("unknown join table: " + join_column["table_name"])
                join_tables[join_column["table_name"]]["ref_count"] += 1
        return join_tables

    def parse_windows(self, expression, config, arguments, primary_table, window_expressions):
        windows = {}
        for window_expression in window_expressions:
            windows[window_expression.name] = window_expression
        return windows

    def parse_on_condition(self, expression, config, arguments, primary_table, join_table):
        if isinstance(expression, sqlglot_expressions.And):
            self.parse_on_condition(expression.args.get("this"), config, arguments, primary_table, join_table)
            self.parse_on_condition(expression.args.get("expression"), config, arguments, primary_table, join_table)
            return
        if isinstance(expression, sqlglot_expressions.Or):
            calculate_fields = []
            self.parse_calculate(expression.args.get("this"), config, arguments, primary_table, calculate_fields)
            self.parse_calculate(expression.args.get("expression"), config, arguments, primary_table, calculate_fields)
            for calculate_field in calculate_fields:
                if calculate_field["table_name"] != join_table["name"]:
                    join_table["join_columns"].append(calculate_field)
                elif calculate_field["column_name"] not in join_table["columns"]:
                    join_table["columns"][calculate_field["column_name"]] = calculate_field
            join_table["having_expressions"].append(expression)
            return

        def parse(expression):
            if expression.args.get("expressions"):
                left_expression, right_expression = expression.args["this"], expression.args["expressions"]
            elif expression.args.get("query"):
                left_expression, right_expression = expression.args["this"], expression.args["query"]
            else:
                left_expression, right_expression = expression.args["this"], expression.args["expression"]

            left_column, right_column = None, None
            if self.is_column(left_expression, config, arguments):
                left_column = self.parse_column(left_expression, config, arguments)
            if self.is_column(right_expression, config, arguments):
                right_column = self.parse_column(right_expression, config, arguments)

            condition_column, value_column, join_on_expression, value_expression = left_column, None, None, right_expression
            if isinstance(expression, sqlglot_expressions.EQ):
                if left_column and left_column["table_name"] == join_table["name"] and not left_column["convert_typing_filter"]:
                    condition_column, value_column, value_expression = left_column, right_column, right_expression
                    if not value_column and not self.is_const(right_expression, config, arguments) and not isinstance(right_expression, list) \
                            and not self.is_subquery(right_expression, config, arguments):
                        join_on_expression = right_expression
                elif right_column and right_column["table_name"] == join_table["name"] and not right_column["convert_typing_filter"]:
                    condition_column, value_column, value_expression = right_column, left_column, left_expression
                    if not value_column and not self.is_const(left_expression, config, arguments) and not isinstance(left_expression, list) \
                            and not self.is_subquery(left_expression, config, arguments):
                        join_on_expression = left_expression
            if condition_column and not condition_column["table_name"]:
                raise SyncanySqlCompileException('unknown join on condition, related sql "%s"' % self.to_sql(expression))

            if condition_column and value_column:
                if condition_column["column_name"] in join_table["primary_keys"]:
                    raise SyncanySqlCompileException(
                        'error join on condition, primary_key duplicate, related sql "%s"' % self.to_sql(expression))
                join_table["join_columns"].append(value_column)
                join_table["primary_keys"].append(condition_column["column_name"])
                join_table["columns"][condition_column["column_name"]] = condition_column
                setattr(value_expression, "syncany_column", value_column)
                join_table["calculate_expressions"].append(value_expression)
                return True, condition_column, None

            if condition_column and join_on_expression:
                calculate_fields = []
                self.parse_calculate(join_on_expression, config, arguments, primary_table, calculate_fields)
                if not calculate_fields and condition_column["table_name"] == join_table["name"]:
                    return False, condition_column, self.compile_calculate(join_on_expression, config, arguments,
                                                                           primary_table, [])
                if condition_column["column_name"] in join_table["primary_keys"]:
                    raise SyncanySqlCompileException(
                        'error join on condition, primary_key duplicate, related sql "%s"' % self.to_sql(expression))
                join_table["join_columns"].extend(calculate_fields)
                join_table["primary_keys"].append(condition_column["column_name"])
                join_table["columns"][condition_column["column_name"]] = condition_column
                join_table["calculate_expressions"].append(join_on_expression)
                return True, condition_column, None

            calculate_fields = []
            if condition_column and not self.is_subquery(value_expression, config, arguments) and not isinstance(value_expression, list):
                self.parse_calculate(value_expression, config, arguments, primary_table, calculate_fields)
            if not condition_column or calculate_fields:
                self.parse_calculate(right_expression if value_expression is left_expression else left_expression,
                                     config, arguments, primary_table, calculate_fields)
                for calculate_field in calculate_fields:
                    if calculate_field["table_name"] != join_table["name"]:
                        join_table["join_columns"].append(calculate_field)
                    elif calculate_field["column_name"] not in join_table["columns"]:
                        join_table["columns"][calculate_field["column_name"]] = calculate_field["column_name"]
                join_table["having_expressions"].append(expression)
                return False, None, None

            if self.is_subquery(value_expression, config, arguments):
                value_column = self.compile_query_condition(value_expression, config, arguments, primary_table, None,
                                                            condition_column["typing_filters"])
                if isinstance(expression, sqlglot_expressions.In):
                    value_column = ["@convert_array", value_column]
                else:
                    value_column = ["@convert_array", value_column, ":$.:0"]
                return False, condition_column, value_column
            if isinstance(value_expression, list):
                if all([self.is_const(value_expression_item, config, arguments) for value_expression_item in value_expression]):
                    value_items = [self.parse_const(value_expression_item, config, arguments)["value"]
                                   for value_expression_item in value_expression]
                    return False, condition_column, ["#const", value_items]
                value_items = [self.compile_calculate(value_expression_item, config, arguments, primary_table, [])
                               for value_expression_item in value_expression]
                return False, condition_column, ["#make", value_items]
            return False, condition_column, self.compile_calculate(value_expression, config, arguments, primary_table, [])

        def build_query_condition(condition_column, condition_exp, condition_calculater):
            if condition_column["typing_name"] not in join_table["querys"]:
                join_table["querys"][condition_column["typing_name"]] = {}
            condition_querys = join_table["querys"][condition_column["typing_name"]]
            if isinstance(condition_querys, dict):
                if condition_exp not in condition_querys:
                    condition_querys[condition_exp] = condition_calculater
                    return None
                condition_querys = [condition_querys]
                join_table["querys"][condition_column["typing_name"]] = condition_querys
            for condition_query in condition_querys:
                if condition_exp not in condition_query:
                    condition_query[condition_exp] = condition_calculater
                    return None
            condition_querys.append({condition_exp: condition_calculater})
            return None

        if isinstance(expression, sqlglot_expressions.EQ):
            is_column, condition_column, value_column = parse(expression)
            if not is_column and condition_column:
                build_query_condition(condition_column, "==", value_column)
        elif isinstance(expression, sqlglot_expressions.NEQ):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException('error join on condition, conditions except != conditions must be constants, related sql "%s"'
                                                 % self.to_sql(expression))
            if condition_column:
                build_query_condition(condition_column, "!=", value_column)
        elif isinstance(expression, sqlglot_expressions.GT):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException('error join on condition, conditions except > conditions must be constants, related sql "%s"'
                                                 % self.to_sql(expression))
            if condition_column:
                build_query_condition(condition_column, ">", value_column)
        elif isinstance(expression, sqlglot_expressions.GTE):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException('error join on condition, conditions except >= conditions must be constants, related sql "%s"'
                                                 % self.to_sql(expression))
            if condition_column:
                build_query_condition(condition_column, ">=", value_column)
        elif isinstance(expression, sqlglot_expressions.LT):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException('error join on condition, conditions except < conditions must be constants, related sql "%s"'
                                                 % self.to_sql(expression))
            if condition_column:
                build_query_condition(condition_column, "<", value_column)
        elif isinstance(expression, sqlglot_expressions.LTE):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException('error join on condition, conditions except <= conditions must be constants, related sql "%s"'
                                                 % self.to_sql(expression))
            if condition_column:
                build_query_condition(condition_column, "<=", value_column)
        elif isinstance(expression, sqlglot_expressions.In):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException('error join on condition, conditions except in conditions must be constants, related sql "%s"'
                                                 % self.to_sql(expression))
            if condition_column:
                build_query_condition(condition_column, "in", value_column)
        else:
            calculate_fields = []
            self.parse_calculate(expression, config, arguments, primary_table, calculate_fields)
            for calculate_field in calculate_fields:
                if calculate_field["table_name"] != join_table["name"]:
                    join_table["join_columns"].append(calculate_field)
                elif calculate_field["column_name"] not in join_table["columns"]:
                    join_table["columns"][calculate_field["column_name"]] = calculate_field["column_name"]
            join_table["having_expressions"].append(expression)

    def parse_calculate(self, expression, config, arguments, primary_table, calculate_fields):
        if hasattr(expression, "syncany_valuer"):
            return
        if hasattr(expression, 'syncany_column'):
            return
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
            if expression.args.get("ifs"):
                for case_expression in expression.args["ifs"]:
                    self.parse_calculate(case_expression, config, arguments, primary_table, calculate_fields)
        elif isinstance(expression, sqlglot_expressions.Paren):
            self.parse_calculate(expression.args["this"], config, arguments, primary_table, calculate_fields)
        elif self.is_column(expression, config, arguments):
            column = self.parse_column(expression, config, arguments)
            if not column["table_name"] or primary_table["table_name"] == column["table_name"]:
                primary_table["columns"][column["column_name"]] = column
            calculate_fields.append(column)
        elif isinstance(expression, sqlglot_expressions.Parameter):
            if isinstance(expression, AssignParameter):
                self.parse_calculate(expression.args["expression"], config, arguments, primary_table, calculate_fields)
        elif self.is_subquery(expression, config, arguments) or isinstance(expression, (sqlglot_expressions.Star,
                                                                                        sqlglot_expressions.Interval, sqlglot_expressions.DataType)):
            pass
        elif self.is_const(expression, config, arguments):
            pass
        else:
            if not isinstance(expression, sqlglot_expressions.Expression):
                return
            for arg_expression in expression.args.values():
                if isinstance(arg_expression, list):
                    for item_arg_expression in arg_expression:
                        if not isinstance(item_arg_expression, sqlglot_expressions.Expression) or self.is_const(item_arg_expression, config, arguments):
                            continue
                        self.parse_calculate(item_arg_expression, config, arguments, primary_table, calculate_fields)
                else:
                    if not isinstance(arg_expression, sqlglot_expressions.Expression) or self.is_const(arg_expression, config, arguments):
                        continue
                    self.parse_calculate(arg_expression, config, arguments, primary_table, calculate_fields)

    def parse_aggregate(self, expression, config, arguments, aggregate_expressions):
        if self.is_aggregate(expression, config, arguments):
            aggregate_expressions.append(expression)
        if not self.is_calculate(expression, config, arguments) and not isinstance(expression, sqlglot_expressions.Paren):
            return
        if self.is_subquery(expression, config, arguments):
            return
        for _, child_expression in expression.args.items():
            if isinstance(child_expression, list):
                for child_expression_item in child_expression:
                    if not isinstance(child_expression_item, sqlglot_expressions.Expression):
                        continue
                    self.parse_aggregate(child_expression_item, config, arguments, aggregate_expressions)
            else:
                if not isinstance(child_expression, sqlglot_expressions.Expression):
                    continue
                self.parse_aggregate(child_expression, config, arguments, aggregate_expressions)

    def parse_window_aggregate(self, expression, config, arguments, window_aggregate_expressions):
        if self.is_window_aggregate(expression, config, arguments):
            window_aggregate_expressions.append(expression)
        if not self.is_calculate(expression, config, arguments) and not isinstance(expression, sqlglot_expressions.Paren):
            return
        if self.is_subquery(expression, config, arguments):
            return
        for _, child_expression in expression.args.items():
            if isinstance(child_expression, list):
                for child_expression_item in child_expression:
                    if not isinstance(child_expression_item, sqlglot_expressions.Expression):
                        continue
                    self.parse_window_aggregate(child_expression_item, config, arguments, window_aggregate_expressions)
            else:
                if not isinstance(child_expression, sqlglot_expressions.Expression):
                    continue
                self.parse_window_aggregate(child_expression, config, arguments, window_aggregate_expressions)

    def parse_subquery(self, expression, config, arguments, primary_table, join_tables):
        if isinstance(expression, sqlglot_expressions.Select):
            sub_expression = expression
        else:
            sub_expression = expression.args["this"]
        where_expression = sub_expression.args.get("where")
        if not where_expression or not where_expression.args.get("this"):
            return None, {}, []

        parent_tables = {primary_table["table_name"]: primary_table}
        if join_tables:
            for join_table_name, join_table in join_tables.items():
                parent_tables[join_table_name] = join_table
        current_tables = {}
        from_expression = sub_expression.args.get("from")
        if from_expression and from_expression.args.get("expressions"):
            for expression in from_expression.args["expressions"]:
                table_info = self.parse_table(expression, config, arguments)
                current_tables[table_info["table_name"]] = table_info
        if sub_expression.args.get("joins"):
            for join_expression in sub_expression.args["joins"]:
                table_info = self.parse_table(join_expression.args["this"], config, arguments)
                current_tables[table_info["table_name"]] = table_info

        def parse_parent_calculate_condition(condition_expression, join_parent_columns):
            if self.is_column(condition_expression, config, arguments):
                condition_column = self.parse_column(condition_expression, config, arguments)
                if (not condition_column["table_name"] or condition_column["table_name"] in current_tables
                        or condition_column["table_name"] not in parent_tables):
                    return
                join_parent_columns.append(condition_expression)
                return
            if self.is_subquery(condition_expression, config, arguments):
                return
            for _, child_expression in condition_expression.args.items():
                if isinstance(child_expression, list):
                    for child_expression_item in child_expression:
                        if not isinstance(child_expression_item, sqlglot_expressions.Expression) or self.is_const(child_expression_item, config, arguments):
                            continue
                        parse_parent_calculate_condition(child_expression_item, join_parent_columns)
                else:
                    if not isinstance(child_expression, sqlglot_expressions.Expression) or self.is_const(child_expression, config, arguments):
                        continue
                    parse_parent_calculate_condition(child_expression, join_parent_columns)

        def parse_calculate_condition(condition_expression, join_select_columns):
            if self.is_column(condition_expression, config, arguments):
                condition_column = self.parse_column(condition_expression, config, arguments)
                if condition_column["table_name"] and condition_column["table_name"] not in current_tables:
                    return
                column_key = self.generate_sql(condition_expression)
                if column_key in join_select_columns:
                    column_alias = join_select_columns[column_key]["column_alias"]
                else:
                    column_alias = "__subquery_select_column_" + str(id(condition_expression))
                    join_select_columns[column_key] = {
                        "column_alias": column_alias,
                        "column": condition_column,
                        "column_expression": condition_expression,
                    }
                setattr(condition_expression, "syncany_valuer", ["$." + column_alias])
                return
            if self.is_subquery(condition_expression, config, arguments):
                return
            for _, child_expression in condition_expression.args.items():
                if isinstance(child_expression, list):
                    for child_expression_item in child_expression:
                        if not isinstance(child_expression_item, sqlglot_expressions.Expression) or self.is_const(child_expression_item, config, arguments):
                            continue
                        parse_calculate_condition(child_expression_item, join_select_columns)
                else:
                    if not isinstance(child_expression, sqlglot_expressions.Expression) or self.is_const(child_expression, config, arguments):
                        continue
                    parse_calculate_condition(child_expression, join_select_columns)

        def parse_where_condition(condition_expression, join_parent_columns, join_select_columns, interceptor_expressions):
            if isinstance(condition_expression, sqlglot_expressions.And):
                left_sql = parse_where_condition(condition_expression.args["this"], join_parent_columns,
                                                 join_select_columns, interceptor_expressions)
                right_sql = parse_where_condition(condition_expression.args["expression"], join_parent_columns,
                                                  join_select_columns, interceptor_expressions)
                if left_sql:
                    return (left_sql + " AND " + right_sql) if right_sql else left_sql
                return right_sql

            if (isinstance(condition_expression, sqlglot_expressions.EQ) and
                    self.is_column(condition_expression.args["this"], config, arguments) and
                    self.is_column(condition_expression.args["expression"], config, arguments)):
                left_condition_column = self.parse_column(condition_expression.args["this"], config, arguments)
                right_condition_column = self.parse_column(condition_expression.args["expression"], config, arguments)
                if ((not left_condition_column["table_name"] or left_condition_column["table_name"] in current_tables) and
                        right_condition_column["table_name"] and right_condition_column["table_name"] in parent_tables and
                        not left_condition_column["convert_typing_filter"]):
                    left_condition_column, right_condition_column = left_condition_column, right_condition_column
                elif ((not right_condition_column["table_name"] or right_condition_column["table_name"] in current_tables) and
                      left_condition_column["table_name"] and left_condition_column["table_name"] in parent_tables and
                      not right_condition_column["convert_typing_filter"]):
                    left_condition_column, right_condition_column = right_condition_column, left_condition_column
                else:
                    left_condition_column, right_condition_column = None, None
                if left_condition_column and right_condition_column:
                    current_column = self.generate_sql(left_condition_column["expression"])
                    parent_column = self.generate_sql(right_condition_column["expression"])
                    column_key = (current_column, parent_column)
                    if column_key in join_parent_columns:
                        column_alias = join_parent_columns[column_key][1]
                    else:
                        column_alias = "_subquery_join_parent_" + str(id(condition_expression))
                        join_parent_columns[column_key] = {
                            "current_expression": left_condition_column["expression"],
                            "current_column": left_condition_column,
                            "parent_expression": right_condition_column["expression"],
                            "parent_column": right_condition_column,
                            "column_alias": column_alias,
                        }
                    return current_column + " IN @" + column_alias

            current_join_parent_columns = []
            parse_parent_calculate_condition(condition_expression, current_join_parent_columns)
            if not current_join_parent_columns:
                return self.generate_sql(condition_expression)
            parse_calculate_condition(condition_expression, join_select_columns)
            interceptor_expressions.append(condition_expression)
            return ""

        join_parent_columns, join_select_columns, interceptor_expressions = {}, {}, []
        where_sql = parse_where_condition(where_expression.args["this"], join_parent_columns, join_select_columns,
                                          interceptor_expressions)
        if not join_parent_columns and not join_select_columns and not interceptor_expressions:
            return None, {}, []

        sql = ["SELECT"]
        if sub_expression.args.get("distinct"):
            sql.append(self.generate_sql(sub_expression.args["distinct"]))

        select_sql, has_aggregate_column = [], False
        for select_expression in sub_expression.args["expressions"]:
            select_sql.append(self.generate_sql(select_expression))
            if not has_aggregate_column:
                has_aggregate_column = self.is_aggregate(select_expression.args["this"] if isinstance(select_expression,
                                                                                         sqlglot_expressions.Alias) else select_expression,
                                                         config, arguments)
        for _, join_parent_column in join_parent_columns.items():
            select_sql.append(self.generate_sql(join_parent_column["current_expression"]) + " AS " + join_parent_column["column_alias"])
        for _, join_select_column in join_select_columns.items():
            select_sql.append(self.generate_sql(join_select_column["column_expression"]) + " AS " + join_select_column["column_alias"])
        sql.append(", ".join(select_sql))
        sql.append(self.generate_sql(from_expression))
        if sub_expression.args.get("joins"):
            for join_expression in sub_expression.args["joins"]:
                sql.append(self.generate_sql(join_expression))
        if where_sql:
            sql.append("WHERE " + where_sql)
        if sub_expression.args.get("group") or has_aggregate_column:
            group_sql = []
            for _, join_parent_column in join_parent_columns.items():
                group_sql.append(self.generate_sql(join_parent_column["current_expression"]))
            for _, join_select_column in join_select_columns.items():
                group_sql.append(self.generate_sql(join_select_column["column_expression"]))
            if sub_expression.args.get("group"):
                for group_expression in sub_expression.args["group"].args["expressions"]:
                    group_sql.append(self.generate_sql(group_expression))
            sql.append("GROUP BY")
            sql.append(", ".join(group_sql))
        if sub_expression.args.get("having"):
            sql.append(self.generate_sql(sub_expression.args["having"]))
        if sub_expression.args.get("order"):
            sql.append(self.generate_sql(sub_expression.args["order"]))
        if sub_expression.args.get("limit"):
            sql.append(self.generate_sql(sub_expression.args.get("limit")))
        if sub_expression.args.get("offset"):
            sql.append(self.generate_sql(sub_expression.args.get("offset")))
        return maybe_parse(" ".join(sql), dialect=CompilerDialect), join_parent_columns, interceptor_expressions


    def parse_table(self, expression, config, arguments):
        db_name = expression.args["db"].name if expression.args.get("db") else None
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
        if expression.args.get("catalog"):
            db_name, table_name = expression.args["catalog"].name, ((db_name + ".") if db_name else "") + table_name
        table_alias = expression.args["alias"].args["this"].name if expression.args.get("alias") else None
        use_output_type = arguments.get("@use_output_type")
        if use_output_type and isinstance(use_output_type, str):
            use_output_type = use_output_type.upper()
            output_type_names = {"INSERT": "I", "UPDATE": "U", "UPDATE_INSERT": "UI", "UPDATE_DELETE_INSERT": "UDI", "DELETE_INSERT": "DI"}
            if use_output_type in output_type_names:
                use_output_type = output_type_names[use_output_type]
            if use_output_type not in typing_options:
                typing_options.append(use_output_type)

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
            "table_name": table_alias if table_alias else ((db_name + "." + table_name if db_name else table_name)),
            "table_alias": table_alias,
            "origin_name": origin_name,
            "typing_options": typing_options,
        }
        
    def parse_column(self, expression, config, arguments):
        dot_keys, convert_typing_filter = [], None
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
        elif isinstance(expression, sqlglot_expressions.Cast):
            convert_typing_filter = self.parse_cast_typing_filter(expression, config, arguments)
            expression = expression.args["this"]
        elif isinstance(expression, sqlglot_expressions.Anonymous):
            calculater_name = expression.args["this"].lower()
            if calculater_name[:8] == "convert_" and calculater_name[8:] in self.TYPE_FILTERS:
                convert_typing_filter = self.TYPE_FILTERS[calculater_name[8:]]
                expression = expression.args["expressions"][0]

        typing_filters = [convert_typing_filter] if convert_typing_filter else []
        db_name = expression.args["db"].name if expression.args.get("db") else None
        table_name = expression.args["table"].name if expression.args.get("table") else None
        column_name = expression.args["this"].name
        origin_name = self.mapping[column_name] if column_name in self.mapping else column_name
        try:
            start_index, end_index = origin_name.index("["), origin_name.rindex("]")
            for typing_filter in origin_name[start_index+1: end_index].split(";"):
                typing_filter = typing_filter.split(" ")
                if not typing_filter:
                    continue
                typing_filter_name = typing_filter[0].lower()
                if typing_filter_name in self.TYPE_FILTERS:
                    typing_filters.append(" ".join([self.TYPE_FILTERS[typing_filter_name]] + typing_filter[1:]))
                else:
                    try:
                        if find_filter(typing_filter_name):
                            typing_filters.append(" ".join([typing_filter_name] + typing_filter[1:]))
                    except:
                        pass
            column_name = origin_name[:start_index]
            typing_name = (column_name + "|" + typing_filters[0]) if typing_filters else column_name
        except ValueError:
            column_name = origin_name
            typing_name = column_name
        try:
            start_index, end_index = origin_name.index("<"), origin_name.rindex(">")
            typing_options = origin_name[start_index+1: end_index].split(",")
        except ValueError:
            typing_options = []
        return {
            "table_name": (db_name + "." + table_name) if db_name else table_name,
            "column_name": (column_name + "." + ".".join(dot_keys)) if dot_keys else column_name,
            "origin_name": origin_name,
            "typing_name": typing_name,
            "dot_keys": dot_keys,
            "typing_filters": typing_filters,
            "typing_options": typing_options,
            "expression": expression,
            "convert_typing_filter": convert_typing_filter,
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
            except KeyError:
                value = None
            value_getter = ["@current_env_variable::get_value", ["#const", "@" + name]]
        else:
            value = None
        return {
            "value": value,
            "value_getter": value_getter,
            "typing_filter": typing_filter,
            "expression": expression,
        }

    def parse_cast_typing_filter(self, expression, config, arguments):
        to_type = expression.args["to"].args["this"]
        if to_type in sqlglot_expressions.DataType.FLOAT_TYPES:
            typing_filter = "float"
        elif to_type in sqlglot_expressions.DataType.INTEGER_TYPES:
            typing_filter = "int"
        elif to_type == sqlglot_expressions.DataType.Type.DATE:
            typing_filter = "date"
        elif to_type in sqlglot_expressions.DataType.TEMPORAL_TYPES:
            typing_filter = "datetime"
        elif to_type == sqlglot_expressions.DataType.Type.TIME:
            typing_filter = "time"
        elif to_type in sqlglot_expressions.DataType.TEXT_TYPES:
            typing_filter = "str"
        elif to_type == sqlglot_expressions.DataType.Type.BOOLEAN:
            typing_filter = "bool"
        elif to_type in (sqlglot_expressions.DataType.Type.DECIMAL, sqlglot_expressions.DataType.Type.BIGDECIMAL):
            typing_filter = "decimal"
        elif to_type == sqlglot_expressions.DataType.Type.UUID:
            typing_filter = "uuid"
        elif to_type in (sqlglot_expressions.DataType.Type.BINARY, sqlglot_expressions.DataType.Type.VARBINARY,
                         sqlglot_expressions.DataType.Type.MEDIUMBLOB, sqlglot_expressions.DataType.Type.LONGBLOB):
            typing_filter = "bytes"
        else:
            typing_filter = None
        return typing_filter

    def parse_condition_typing_filter(self, expression, config, arguments):
        for key in list(config["querys"].keys()):
            if "|" in key:
                continue
            typing_filter = None
            column_query = config["querys"][key]
            if isinstance(column_query, dict):
                column_query = [column_query]
            for query_exps in column_query:
                for exp_key, exp_value in query_exps.items():
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
                            typing_filter = False
                            break
                        typing_filter = "datetime"
                        continue
                    if isinstance(exp_value, list) and len(exp_value) == 2 and exp_value[0] == "#const":
                        exp_value = exp_value[1]
                    type_name = str(type(exp_value).__name__)
                    if type_name in ("int", "float", "bool"):
                        if typing_filter and typing_filter != type_name:
                            typing_filter = False
                            break
                        typing_filter = type_name
                if typing_filter is False:
                    break
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

    def is_column(self, expression, config, arguments, check_convert_type=True):
        if isinstance(expression, sqlglot_expressions.Column):
            return True
        if check_convert_type:
            if isinstance(expression, sqlglot_expressions.Cast) and self.is_column(expression.args["this"],
                                                                                   config, arguments, check_convert_type=False):
                return True
            if isinstance(expression, sqlglot_expressions.Anonymous):
                calculater_name = expression.args["this"].lower()
                if (calculater_name[:8] == "convert_" and calculater_name[8:] in self.TYPE_FILTERS and
                        expression.args.get("expressions") and len(expression.args["expressions"]) == 1 and
                        self.is_column(expression.args["expressions"][0], config, arguments, check_convert_type=False)):
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
        if isinstance(expression, sqlglot_expressions.Parameter):
            return not isinstance(expression, AssignParameter)
        return isinstance(expression, (sqlglot_expressions.Literal, sqlglot_expressions.Boolean,
                                       sqlglot_expressions.Null, sqlglot_expressions.HexString, sqlglot_expressions.BitString,
                                       sqlglot_expressions.ByteString))

    def is_calculate(self, expression, config, arguments):
        if isinstance(expression, sqlglot_expressions.Condition):
            return isinstance(expression, (sqlglot_expressions.EQ, sqlglot_expressions.NEQ, sqlglot_expressions.GT, sqlglot_expressions.GTE,
                                           sqlglot_expressions.LT, sqlglot_expressions.LTE, sqlglot_expressions.In, sqlglot_expressions.Not,
                                           sqlglot_expressions.Is, sqlglot_expressions.Between, sqlglot_expressions.Like, sqlglot_expressions.Func,
                                           sqlglot_expressions.Binary, sqlglot_expressions.SubqueryPredicate))
        return isinstance(expression, (sqlglot_expressions.Neg, sqlglot_expressions.Binary, sqlglot_expressions.Select,
                                       sqlglot_expressions.Subquery, sqlglot_expressions.Union,
                                       sqlglot_expressions.BitwiseNot, sqlglot_expressions.Tuple, sqlglot_expressions.Parameter))

    def is_aggregate(self, expression, config, arguments):
        if isinstance(expression, (sqlglot_expressions.Column, sqlglot_expressions.Literal)):
            return False
        if isinstance(expression, (sqlglot_expressions.Count, sqlglot_expressions.Sum, sqlglot_expressions.Max,
                                   sqlglot_expressions.Min, sqlglot_expressions.Avg, sqlglot_expressions.GroupConcat,
                                   sqlglot_expressions.GroupUniqArray)):
            return True
        if isinstance(expression, sqlglot_expressions.Anonymous):
            aggregate_funcs = {"group_array", "grouparray", "group_uniq_array", "groupuniqarray", "group_bit_and", "groupbitand",
                               "group_bit_or", "groupbitor", "group_bit_xor", "groupbitxor"}
            calculater_name = expression.args["this"].lower()
            if calculater_name in aggregate_funcs:
                return True
            try:
                find_aggregate_calculater(calculater_name)
            except CalculaterUnknownException:
                return False
            return True
        return False

    def is_window_aggregate(self, expression, config, arguments):
        return isinstance(expression, sqlglot_expressions.Window)

    def is_subquery(self, expression, config, arguments):
        return isinstance(expression, (sqlglot_expressions.Subquery, sqlglot_expressions.Select, sqlglot_expressions.Union))

    def optimize_rewrite(self, expression, config, arguments):
        from_expression = expression.args.get("from")
        if not from_expression or not from_expression.expressions:
            return expression
        if len(from_expression.expressions) > 1:
            expression = self.optimize_rewrite_multi_select(expression, config, arguments, from_expression)

        if not expression.args.get("joins") or not expression.args.get("expressions"):
            return expression
        for join_expression in expression.args["joins"]:
            if join_expression.side and join_expression.side.lower() == "right":
                expression = self.optimize_rewrite_right_join(expression, config, arguments)
                break
        for join_expression in expression.args["joins"]:
            if join_expression.kind and join_expression.kind.lower() == "inner":
                expression = self.optimize_rewrite_inner_join(expression, config, arguments)
                break
        return expression

    def optimize_rewrite_multi_select(self, expression, config, arguments, from_expression):
        primary_table = self.optimize_rewrite_parse_table(expression, config, arguments,
                                                          expression.args["from"].expressions[0])
        if not primary_table["table_name"]:
            return expression

        join_tables, can_on_expressions_tables = [], [primary_table["table_name"]]
        for from_table_expression in from_expression.expressions[1:]:
            join_table = {"table_name": self.optimize_rewrite_parse_table(expression, config, arguments,
                                                                          from_table_expression)["table_name"],
                          "related_tables": set([]), "on_expressions": [], "const_expressions": [],
                          "calculate_expressions": [], "join_expression": from_table_expression, "join_type": "left",
                          "table_expression": from_table_expression}
            if expression.args.get("where"):
                self.optimize_rewrite_parse_condition(expression, config, arguments, primary_table, expression.args["where"].args["this"],
                                                      join_table["table_name"], join_table["related_tables"],
                                                      join_table["on_expressions"], join_table["const_expressions"],
                                                      join_table["calculate_expressions"], can_on_expressions_tables)
            join_tables.append(join_table)
            can_on_expressions_tables.append(join_table["table_name"])

        sql = ["SELECT"]
        if expression.args.get("distinct"):
            sql.append(self.generate_sql(expression.args["distinct"]))
        sql.append(", ".join([self.generate_sql(select_expression) for select_expression in expression.args["expressions"]]))
        sql.append("FROM " + self.generate_sql(expression.args["from"].expressions[0]))
        for join_table in join_tables:
            sql.append("LEFT JOIN " + self.generate_sql(join_table["table_expression"]))
            on_expressions  = join_table["on_expressions"] + join_table["const_expressions"]
            if on_expressions:
                sql.append("ON " + " AND ".join([self.generate_sql(on_expression) for on_expression in on_expressions]))

        if expression.args.get("where"):
            sql.append(self.generate_sql(expression.args["where"]))
        if expression.args.get("group"):
            sql.append(self.generate_sql(expression.args["group"]))
        if expression.args.get("having"):
            sql.append(self.generate_sql(expression.args["having"]))
        if expression.args.get("order"):
            sql.append(self.generate_sql(expression.args["order"]))
        if expression.args.get("limit"):
            sql.append(self.generate_sql(expression.args.get("limit")))
        if expression.args.get("offset"):
            sql.append(self.generate_sql(expression.args.get("offset")))
        return maybe_parse(" ".join(sql), dialect=CompilerDialect)

    def optimize_rewrite_right_join(self, expression, config, arguments):
        primary_table = self.optimize_rewrite_parse_table(expression, config, arguments,
                                                          expression.args["from"].expressions[0])
        if not primary_table["table_name"]:
            return expression
        primary_table.update({"related_tables": set([]), "on_expressions": [], "const_expressions": [],
                              "calculate_expressions": [], "join_type": "primary",
                              "table_expression": expression.args["from"].args["expressions"][0]})
        if expression.args.get("where"):
            self.optimize_rewrite_parse_condition(expression, config, arguments, primary_table,
                                                  expression.args["where"].args["this"], primary_table["table_name"],
                                                  primary_table["related_tables"], primary_table["calculate_expressions"],
                                                  primary_table["calculate_expressions"], primary_table["calculate_expressions"])
        join_tables = []
        for join_expression in expression.args["joins"]:
            join_table = {"table_name": self.optimize_rewrite_parse_table(expression, config, arguments,
                                                                          join_expression.args["this"])["table_name"],
                          "related_tables": set([]), "on_expressions": [], "const_expressions": [],
                          "calculate_expressions": [], "join_expression": join_expression, "join_type": join_expression.side.lower(),
                          "table_expression": join_expression.args["this"]}
            if join_expression.args.get("on"):
                self.optimize_rewrite_parse_condition(expression, config, arguments, primary_table, join_expression.args["on"],
                                                      join_table["table_name"], join_table["related_tables"],
                                                      join_table["on_expressions"], join_table["const_expressions"],
                                                      join_table["calculate_expressions"])
            join_tables.append(join_table)

        selected_table = primary_table
        while True:
            new_primary_table_table = None
            for join_table in join_tables:
                if join_table["join_type"] == "right" and selected_table["table_name"] in join_table["related_tables"]:
                    new_primary_table_table = join_table
            if not new_primary_table_table:
                break
            selected_table = new_primary_table_table
        if selected_table["join_type"] == "primary":
            return expression
        join_tables.remove(selected_table)
        join_tables.append(primary_table)

        def resort_join_tables(current_table, on_expressions, join_tables):
            related_tables = []
            for table_name in current_table["related_tables"]:
                if table_name not in join_tables:
                    continue
                related_table = join_tables.pop(table_name)
                related_tables.append(related_table)
                if related_table["join_type"] == "right":
                    related_tables.extend(resort_join_tables(related_table, related_table["on_expressions"], join_tables))
                    related_table["on_expressions"] = on_expressions
                    related_table["join_type"] = "left"
                else:
                    related_table["on_expressions"].extend(on_expressions)
            return related_tables
        join_table_names = {join_table["table_name"]: join_table for join_table in join_tables}
        join_tables = resort_join_tables(selected_table, selected_table["on_expressions"],
                                         join_table_names) + list(join_table_names.values())

        sql = ["SELECT"]
        if expression.args.get("distinct"):
            sql.append(self.generate_sql(expression.args["distinct"]))
        sql.append(", ".join([self.generate_sql(select_expression) for select_expression in expression.args["expressions"]]))
        sql.append("FROM " + self.generate_sql(selected_table["table_expression"]))
        for join_table in join_tables:
            if join_table["join_type"] == "right":
                return expression
            sql.append("LEFT JOIN " + self.generate_sql(join_table["table_expression"]))
            on_expressions = join_table["on_expressions"] + join_table["const_expressions"] + join_table["calculate_expressions"]
            if on_expressions:
                sql.append("ON " + " AND ".join([self.generate_sql(on_expression) for on_expression in on_expressions]))

        where_expressions = selected_table["const_expressions"] + primary_table["calculate_expressions"] + selected_table["calculate_expressions"]
        if where_expressions:
            sql.append("WHERE " + " AND ".join([("(" + self.generate_sql(where_expression) + ")")
                                                if isinstance(where_expression, sqlglot_expressions.Or)
                                                else self.generate_sql(where_expression)
                                                for where_expression in where_expressions]))
        if expression.args.get("group"):
            sql.append(self.generate_sql(expression.args["group"]))
        if expression.args.get("having"):
            sql.append(self.generate_sql(expression.args["having"]))
        if expression.args.get("order"):
            sql.append(self.generate_sql(expression.args["order"]))
        if expression.args.get("limit"):
            sql.append(self.generate_sql(expression.args.get("limit")))
        if expression.args.get("offset"):
            sql.append(self.generate_sql(expression.args.get("offset")))
        return maybe_parse(" ".join(sql), dialect=CompilerDialect)

    def optimize_rewrite_inner_join(self, expression, config, arguments):
        primary_table = self.optimize_rewrite_parse_table(expression, config, arguments,
                                                          expression.args["from"].expressions[0])
        if not primary_table["table_name"]:
            return expression

        inner_calculate_fields = []
        for join_expression in expression.args["joins"]:
            join_table = {"table_name": self.optimize_rewrite_parse_table(expression, config, arguments,
                                                                          join_expression.args["this"])["table_name"],
                          "related_tables": set([]), "on_expressions": [], "const_expressions": [],
                          "calculate_expressions": [], "join_expression": join_expression, "join_kind": join_expression.kind.lower(),
                          "table_expression": join_expression.args["this"]}
            if join_expression.args.get("on"):
                self.optimize_rewrite_parse_condition(expression, config, arguments, primary_table,
                                                      join_expression.args["on"],
                                                      join_table["table_name"], join_table["related_tables"],
                                                      join_table["on_expressions"], join_table["const_expressions"],
                                                      join_table["calculate_expressions"])
            if primary_table["table_name"] in join_table["related_tables"]:
                for on_expression in join_table["on_expressions"]:
                    self.parse_calculate(on_expression, config, arguments, primary_table, inner_calculate_fields)
        inner_calculate_fields = [calculate_field for calculate_field in inner_calculate_fields
                                  if calculate_field["table_name"] and calculate_field["table_name"] != primary_table["table_name"]]
        if not inner_calculate_fields:
            return expression

        sql = ["SELECT"]
        if expression.args.get("distinct"):
            sql.append(self.generate_sql(expression.args["distinct"]))
        sql.append(", ".join([self.generate_sql(select_expression) for select_expression in expression.args["expressions"]]))
        sql.append(self.generate_sql(expression.args["from"]))
        for join_expression in expression.args["joins"]:
            sql.append("LEFT JOIN " + self.generate_sql(join_expression.args["this"]))
            if join_expression.args.get("on"):
                sql.append("ON " + self.generate_sql(join_expression.args["on"]))

        inner_condition_sql = " AND ".join(["%s.%s IS NOT NULL" % (calculate_field["table_name"], calculate_field["column_name"])
                                            for calculate_field in inner_calculate_fields])
        if expression.args.get("where"):
            if isinstance(expression.args["where"].args["this"], sqlglot_expressions.Or):
                sql.append("WHERE (" + self.generate_sql(expression.args["where"].args["this"]) + ") AND " + inner_condition_sql)
            else:
                sql.append("WHERE " + self.generate_sql(expression.args["where"].args["this"]) + " AND " + inner_condition_sql)
        else:
            sql.append("WHERE " + inner_condition_sql)
        if expression.args.get("group"):
            sql.append(self.generate_sql(expression.args["group"]))
        if expression.args.get("having"):
            sql.append(self.generate_sql(expression.args["having"]))
        if expression.args.get("order"):
            sql.append(self.generate_sql(expression.args["order"]))
        if expression.args.get("limit"):
            sql.append(self.generate_sql(expression.args.get("limit")))
        if expression.args.get("offset"):
            sql.append(self.generate_sql(expression.args.get("offset")))
        return maybe_parse(" ".join(sql), dialect=CompilerDialect)

    def optimize_rewrite_parse_table(self, expression, config, arguments, table_expression):
        table = {"table_name": None, "table_alias": None, "columns": {}}
        if isinstance(table_expression, sqlglot_expressions.Table):
            table.update(self.parse_table(table_expression, config, arguments))
        elif isinstance(table_expression, sqlglot_expressions.Subquery):
            if "alias" not in table_expression.args:
                return table
            table["table_alias"] = table_expression.args["alias"].args["this"].name \
                if table_expression.args.get("alias") else None
            table["table_name"] = table["table_alias"]
        return table

    def optimize_rewrite_parse_condition(self, expression, config, arguments, primary_table, condition_expression,
                                         table_name, related_tables, on_expressions, const_expressions, calculate_expressions,
                                         can_on_expressions_tables = None):
        if isinstance(condition_expression, sqlglot_expressions.And):
            self.optimize_rewrite_parse_condition(expression, config, arguments, primary_table, condition_expression.args.get("this"),
                                                  table_name, related_tables, on_expressions, const_expressions, calculate_expressions,
                                                  can_on_expressions_tables)
            self.optimize_rewrite_parse_condition(expression, config, arguments, primary_table, condition_expression.args.get("expression"),
                                                  table_name, related_tables, on_expressions, const_expressions, calculate_expressions,
                                                  can_on_expressions_tables)
        elif isinstance(condition_expression, (sqlglot_expressions.EQ, sqlglot_expressions.NEQ, sqlglot_expressions.GT,
                                               sqlglot_expressions.GTE, sqlglot_expressions.LT, sqlglot_expressions.LTE,
                                               sqlglot_expressions.In)):
            if condition_expression.args.get("expressions"):
                left_expression, right_expression = condition_expression.args["this"], condition_expression.args["expressions"]
            elif condition_expression.args.get("query"):
                left_expression, right_expression = condition_expression.args["this"], condition_expression.args["query"]
            else:
                left_expression, right_expression = condition_expression.args["this"], condition_expression.args["expression"]

            left_column, condition_column, value_expression = None, None, left_expression
            if self.is_column(left_expression, config, arguments):
                left_column = self.parse_column(left_expression, config, arguments)
                if left_column["table_name"] == table_name:
                    if can_on_expressions_tables and self.is_column(right_expression, config, arguments):
                        right_column = self.parse_column(right_expression, config, arguments)
                        if right_column["table_name"] in can_on_expressions_tables:
                            condition_column, value_expression = left_column, right_expression
                    else:
                        condition_column, value_expression = left_column, right_expression
            if not condition_column and self.is_column(right_expression, config, arguments):
                right_column = self.parse_column(right_expression, config, arguments)
                if right_column["table_name"] == table_name:
                    if can_on_expressions_tables and left_column:
                        if left_column["table_name"] in can_on_expressions_tables:
                            condition_column, value_expression = right_column, left_expression
                    else:
                        condition_column, value_expression = right_column, left_expression
            if not condition_column or isinstance(value_expression, (sqlglot_expressions.Select,
                                                                     sqlglot_expressions.Subquery,
                                                                     sqlglot_expressions.Union, list)):
                calculate_expressions.append(condition_expression)
                return

            calculate_fields = []
            self.parse_calculate(value_expression, config, arguments, primary_table, calculate_fields)
            if not calculate_fields:
                const_expressions.append(condition_expression)
                return
            on_expressions.append(condition_expression)
            for calculate_field in calculate_fields:
                related_tables.add(calculate_field["table_name"])
        else:
            calculate_expressions.append(condition_expression)

    def generate_sql(self, expression):
        if isinstance(expression, sqlglot_expressions.Expression):
            return expression.sql(dialect=CompilerDialect)
        return str(expression)

    def to_sql(self, expression_sql):
        if not isinstance(expression_sql, str):
            expression_sql = self.generate_sql(expression_sql)
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
