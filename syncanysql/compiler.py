# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

from sqlglot import maybe_parse
from sqlglot import expressions
from .errors import SyncanySqlCompileException

class Compiler(object):
    def __init__(self, sql):
        self.sql = sql
        self.expression = None
        self.config = {
            "input": "&.=.-::id",
            "output": "&.-.&2::id",
            "querys": {},
            "schema": "$.*"
        }
        self.arguments = {
            "@timeout": 0,
            "@limit": 100
        }

    def compile(self):
        self.expression = maybe_parse(self.sql)
        if isinstance(self.expression, expressions.Select):
            self.compile_select(self.expression)
        else:
            raise SyncanySqlCompileException("unkonw sql: " + self.sql)
        return self.config, self.arguments

    def compile_select(self, expression):
        primary_key, primary_alias = None, None
        select_expressions = expression.args.get("expressions")
        if select_expressions:
            self.config["schema"] = {}
            for select_expression in select_expressions:
                if isinstance(select_expression, expressions.Star):
                    self.config["schema"] = "$.*"
                    break
                if isinstance(select_expression, (expressions.Column, expressions.Alias)):
                    column_name, column_alias = select_expression.args["this"].name, None
                    if "alias" in select_expression.args and select_expression.args["alias"]:
                        column_alias = select_expression.args["alias"].name
                    column_info = column_name.split("__")
                    column_name, column_info = column_info[0], column_info[1:]
                    if "pk" in column_info:
                        primary_key, primary_alias = column_name, column_alias
                        column_info.remove("pk")
                    elif primary_key is None or (column_alias and column_alias == "id"):
                        primary_key, primary_alias = column_name, column_alias
                    self.config["schema"][column_alias if column_alias else column_name] = "$." + column_name \
                                                                                           + (("|" + " ".join(column_info)) if column_info else "")

        from_expression = expression.args.get("from")
        if isinstance(from_expression, expressions.From) and from_expression.expressions \
            and isinstance(from_expression.expressions[0], expressions.Table):
            self.config["input"] = "".join(["&.", from_expression.expressions[0].args["db"].name, ".",
                                            from_expression.expressions[0].args["this"].name, "::", primary_key if primary_key else "id"])
        if primary_alias and primary_alias != "id":
            self.config["output"] = "".join([self.config["output"].split("::")[0], "::", primary_alias if primary_alias else "id",
                                             " use I" if primary_key is None else ""])

        where_expression = expression.args.get("where")
        if where_expression and isinstance(where_expression, expressions.Where):
            self.compile_where_condition(where_expression.args["this"])

        limit_expression = expression.args.get("limit")
        if limit_expression:
            self.arguments["@limit"] = int(limit_expression.args["expression"].args["this"])

    def compile_where_condition(self, expression):
        if not expression:
            return
        if isinstance(expression, expressions.And):
            self.compile_where_condition(expression.args.get("expression"))
            self.compile_where_condition(expression.args.get("this"))
            return

        def parse(expression):
            condition_name = expression.args["this"].name
            condition_value = expression.args["expression"].args["this"]
            if expression.args["expression"].is_int:
                condition_name += "|int"
            if condition_name not in self.config["querys"]:
                self.config["querys"][condition_name] = {}
            return condition_name, condition_value

        if isinstance(expression, expressions.EQ):
            condition_name, condition_value = parse(expression)
            self.config["querys"][condition_name]["=="] = condition_value
        elif isinstance(expression, expressions.NEQ):
            condition_name, condition_value = parse(expression)
            self.config["querys"][condition_name]["!="] = condition_value
        elif isinstance(expression, expressions.GT):
            condition_name, condition_value = parse(expression)
            self.config["querys"][condition_name][">"] = condition_value
        elif isinstance(expression, expressions.GTE):
            condition_name, condition_value = parse(expression)
            self.config["querys"][condition_name][">="] = condition_value
        elif isinstance(expression, expressions.LT):
            condition_name, condition_value = parse(expression)
            self.config["querys"][condition_name]["<"] = condition_value
        elif isinstance(expression, expressions.LTE):
            condition_name, condition_value = parse(expression)
            self.config["querys"][condition_name]["<="] = condition_value
        elif isinstance(expression, expressions.In):
            condition_name, condition_value = parse(expression)
            self.config["querys"][condition_name]["in"] = condition_value