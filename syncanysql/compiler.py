# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

from sqlglot import maybe_parse
from sqlglot import expressions
from .errors import SyncanySqlCompileException

class Compiler(object):
    def __init__(self, config):
        self.config = config

    def compile(self, sql, arguments):
        expression = maybe_parse(sql)
        return self.compile_expression(expression, arguments)

    def compile_expression(self, expression, arguments):
        config = {}
        config.update(self.config)
        config.update({
            "input": "&.=.-::id",
            "output": "&.-.&2::id",
            "querys": {},
            "schema": "$.*",
            "dependencys": []
        })
        arguments.update({"@timeout": 0, "@limit": 100})
        if isinstance(expression, expressions.Insert):
            self.compile_insert_into(expression, config, arguments)
        elif isinstance(expression, expressions.Select):
            self.compile_select(expression, config, arguments)
        else:
            raise SyncanySqlCompileException("unkonw sql: " + str(expression))
        return config

    def compile_subquery(self, expression, arguments):
        table_name = expression.args["alias"].args["this"].name
        subquery_name = "__subquery_" + str(id(expression)) + "_" + table_name
        subquery_arguments = {}
        subquery_config = self.compile_expression(expression.args["this"], subquery_arguments)
        subquery_config["output"] = "&.--." + subquery_name + "::" + subquery_config["output"].split("::")[-1]
        subquery_config["name"] = subquery_config["name"] + "#" + subquery_name
        arguments.update({subquery_config["name"] + "@" + key: value for key, value in subquery_arguments.items()})
        return subquery_name, subquery_config

    def compile_insert_into(self, expression, config, arguments):
        if not isinstance(expression.args["this"], expressions.Table):
            raise SyncanySqlCompileException("unkonw insert info table: " + str(expression))
        if expression.args["this"].name == "_":
            config["output"] = "&.-.&2::id"
        else:
            config["output"] = "".join(["&.", expression.args["db"].name, ".", expression.args["this"].name, "::", "id"])
        select_expression = expression.args.get("expression")
        if not select_expression or not isinstance(select_expression, expressions.Select):
            raise SyncanySqlCompileException("unkonw insert info select: " + str(expression))
        self.compile_select(select_expression, config, arguments)

    def compile_select(self, expression, config, arguments):
        table_name, primary_key, primary_alias = None, None, None

        from_expression = expression.args.get("from")
        if not isinstance(from_expression, expressions.From) or not from_expression.expressions:
            raise SyncanySqlCompileException("unkonw table: " + str(expression))
        from_expression = from_expression.expressions[0]
        if isinstance(from_expression, expressions.Table):
            if "alias" in from_expression.args:
                table_name = from_expression.args["alias"].args["this"].name
            else:
                table_name = from_expression.args["this"].name
            config["input"] = "".join(["&.", from_expression.args["db"].name, ".", from_expression.args["this"].name, "::",
                                       primary_key if primary_key else "id"])
        elif isinstance(from_expression, expressions.Subquery):
            if "alias" not in from_expression.args:
                raise SyncanySqlCompileException("subquery must be alias name: " + str(expression))
            table_name = from_expression.args["alias"].args["this"].name
            subquery_name, subquery_config = self.compile_subquery(from_expression, arguments)
            config["input"] = "".join(["&.--.", subquery_name, "::", primary_key if primary_key else "id"])
            config["dependencys"].append(subquery_config)
        else:
            raise SyncanySqlCompileException("unkonw table: " + str(expression))
        if primary_alias and primary_alias != "id":
            config["output"] = "".join([config["output"].split("::")[0], "::", primary_alias if primary_alias else "id",
                                        " use I" if primary_key is None else ""])
        join_tables = self.parse_joins(config, table_name, expression.args["joins"], arguments) \
            if "joins" in expression.args and expression.args["joins"] else {}

        select_expressions = expression.args.get("expressions")
        if not select_expressions:
            raise SyncanySqlCompileException("unkonw table selects: " + str(expression))
        config["schema"] = {}
        for select_expression in select_expressions:
            if isinstance(select_expression, expressions.Star):
                config["schema"] = "$.*"
                break
            if isinstance(select_expression, (expressions.Column, expressions.Alias)):
                column_table, column_name, column_alias = select_expression.args["table"].name if "table" in select_expression.args else None, \
                                                          select_expression.args["this"].name, None
                if "alias" in select_expression.args and select_expression.args["alias"]:
                    column_alias = select_expression.args["alias"].name
                if primary_key is None or (column_alias and column_alias == "id"):
                    primary_key, primary_alias = column_name, column_alias
                field_name = column_alias if column_alias else column_name
                if column_table and column_table != table_name and column_table in join_tables:
                    config["schema"][field_name] = self.compile_join_column(table_name, "$." + column_name,
                                                                            join_tables[column_table], join_tables)
                else:
                    config["schema"][field_name] = "$." + column_name

        where_expression = expression.args.get("where")
        if where_expression and isinstance(where_expression, expressions.Where):
            self.compile_where_condition(where_expression.args["this"], config)

        limit_expression = expression.args.get("limit")
        if limit_expression:
            arguments["@limit"] = int(limit_expression.args["expression"].args["this"])

    def compile_join_column(self, table_name, column, join_table, join_tables):
        column_join_tables = []
        def gen_column_joins(join_table, join_tables):
            column_join_tables.append(join_table)
            column_join_names = sorted(list({join_field["table_name"] for join_field in join_table["join_fields"]}),
                                       key=lambda x: 0xffffff if x == table_name else join_tables[x]["ref_count"])
            for column_join_name in column_join_names:
                if column_join_name == table_name:
                    continue
                gen_column_joins(join_tables[column_join_name], join_tables)
        gen_column_joins(join_table, join_tables)

        def gen_column(ci, join_field):
            if join_field["table_name"] == table_name:
                return ("$" * (len(column_join_tables) - ci)) + "." + join_field["field_name"]
            ji = [j for j in range(len(column_join_tables)) if join_field["table_name"] == column_join_tables[j]["name"]][0]
            return ("$" * (ji - ci)) + "." + join_field["field_name"]

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
            if len(join_table["join_fields"]) == 1:
                join_fields = gen_column(i, join_table["join_fields"][0])
            else:
                join_fields = [gen_column(i, join_field) for join_field in join_table["join_fields"]]
            join_db_table = "&." + join_table["db"] + "." + join_table["table"] + "::" + "+".join(join_table["primary_keys"])
            if join_table["querys"]:
                join_db_table = [join_db_table, join_table["querys"]]
            column = [join_fields, join_db_table, column]
        return column

    def compile_where_condition(self, expression, config):
        if not expression:
            return
        if isinstance(expression, expressions.And):
            self.compile_where_condition(expression.args.get("expression"), config)
            self.compile_where_condition(expression.args.get("this"), config)
            return

        def parse(expression):
            condition_name = expression.args["this"].name
            if not isinstance(expression.args["expression"], expressions.Literal):
                raise SyncanySqlCompileException("unkonw where condition: " + str(expression))
            condition_value = expression.args["expression"].args["this"]
            if expression.args["expression"].is_int:
                condition_name += "|int"
            if condition_name not in config["querys"]:
                config["querys"][condition_name] = {}
            return condition_name, condition_value

        if isinstance(expression, expressions.EQ):
            condition_name, condition_value = parse(expression)
            config["querys"][condition_name]["=="] = condition_value
        elif isinstance(expression, expressions.NEQ):
            condition_name, condition_value = parse(expression)
            config["querys"][condition_name]["!="] = condition_value
        elif isinstance(expression, expressions.GT):
            condition_name, condition_value = parse(expression)
            config["querys"][condition_name][">"] = condition_value
        elif isinstance(expression, expressions.GTE):
            condition_name, condition_value = parse(expression)
            config["querys"][condition_name][">="] = condition_value
        elif isinstance(expression, expressions.LT):
            condition_name, condition_value = parse(expression)
            config["querys"][condition_name]["<"] = condition_value
        elif isinstance(expression, expressions.LTE):
            condition_name, condition_value = parse(expression)
            config["querys"][condition_name]["<="] = condition_value
        elif isinstance(expression, expressions.In):
            condition_name, condition_value = parse(expression)
            config["querys"][condition_name]["in"] = condition_value
        else:
            raise SyncanySqlCompileException("unkonw where condition: " + str(expression))

    def parse_joins(self, config, table_name, join_expressions, arguments):
        join_tables = {}
        for join_expression in join_expressions:
            if "alias" not in join_expression.args["this"].args:
                raise SyncanySqlCompileException("join table must be alias name: " + str(join_expression))
            name = join_expression.args["this"].args["alias"].args["this"].name
            if isinstance(join_expression.args["this"], expressions.Table):
                db, table = join_expression.args["this"].args["db"].name, join_expression.args["this"].args["this"].name
            elif isinstance(join_expression.args["this"], expressions.Subquery):
                subquery_name, subquery_config = self.compile_subquery(join_expression.args["this"], arguments)
                db, table = "--", subquery_name
                config["dependencys"].append(subquery_config)
            else:
                raise SyncanySqlCompileException("unkonw join table: " + str(join_expression))
            if "on" not in join_expression.args:
                raise SyncanySqlCompileException("unkonw join on: " + str(join_expression))
            join_table = {
                "db": db,
                "table": table,
                "name": name,
                "primary_keys": set([]),
                "join_fields": [],
                "querys": {},
                "ref_count": 0
            }
            self.parse_on_condition(join_expression.args["on"], join_table)
            if not join_table["primary_keys"] or not join_table["join_fields"]:
                raise SyncanySqlCompileException("empty join table: " + str(join_expression))
            join_tables[join_table["name"]] = join_table

        for name, join_table in join_tables.items():
            for join_field in join_table["join_fields"]:
                if join_field["table_name"] == table_name:
                    continue
                if join_field["table_name"] not in join_tables:
                    raise SyncanySqlCompileException("unknown join table: " + join_field["table_name"])
                join_tables[join_field["table_name"]]["ref_count"] += 1
        return join_tables

    def parse_on_condition(self, expression, join_table):
        if not expression:
            return
        if isinstance(expression, expressions.And):
            self.parse_on_condition(expression.args.get("expression"), join_table)
            self.parse_on_condition(expression.args.get("this"), join_table)
            return

        def parse(expression):
            condition_table = expression.args["this"].args["table"].name if "table" in expression.args["this"].args else None
            condition_name = expression.args["this"].name
            if isinstance(expression.args["expression"], expressions.Literal):
                condition_value = expression.args["expression"].args["this"]
                if expression.args["expression"].is_int:
                    condition_name += "|int"
                if condition_name not in join_table["querys"]:
                    join_table["querys"][condition_name] = {}
                return (False, condition_table, condition_name, condition_value)
            if isinstance(expression.args["expression"], expressions.Column):
                if condition_table is None or "table" not in expression.args["expression"].args:
                    raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
                return (True, condition_table, condition_name,
                        (expression.args["expression"].args["table"].name,
                         expression.args["expression"].args["this"].name))
            raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))

        if isinstance(expression, expressions.EQ):
            is_column, condition_table, condition_name, condition_value = parse(expression)
            if is_column:
                if condition_table == join_table["name"]:
                    join_table["primary_keys"].add(condition_name)
                    join_table["join_fields"].append({
                        "table_name": condition_value[0],
                        "field_name": condition_value[1]
                    })
                elif condition_value[0] == join_table["name"]:
                    join_table["primary_keys"].add(condition_value[1])
                    join_table["join_fields"].append({
                        "table_name": condition_table,
                        "field_name": condition_name
                    })
                else:
                    raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            else:
                join_table["querys"][condition_name]["=="] = condition_value
        elif isinstance(expression, expressions.NEQ):
            is_column, condition_table, condition_name, condition_value = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            join_table["querys"][condition_name]["!="] = condition_value
        elif isinstance(expression, expressions.GT):
            is_column, condition_table, condition_name, condition_value = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            join_table["querys"][condition_name][">"] = condition_value
        elif isinstance(expression, expressions.GTE):
            is_column, condition_table, condition_name, condition_value = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            join_table["querys"][condition_name][">="] = condition_value
        elif isinstance(expression, expressions.LT):
            is_column, condition_table, condition_name, condition_value = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            join_table["querys"][condition_name]["<"] = condition_value
        elif isinstance(expression, expressions.LTE):
            is_column, condition_table, condition_name, condition_value = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            join_table["querys"][condition_name]["<="] = condition_value
        elif isinstance(expression, expressions.In):
            is_column, condition_table, condition_name, condition_value = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            join_table["querys"][condition_name]["in"] = condition_value
        else:
            raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))