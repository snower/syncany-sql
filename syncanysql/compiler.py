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

            if isinstance(select_expression, expressions.Column):
                column_table, column_name, column_alias = self.compile_select_column(select_expression)
                if primary_key is None or (column_alias and column_alias == "id"):
                    primary_key, primary_alias = column_name, column_alias
                field_name = column_alias if column_alias else column_name
                if column_table and column_table != table_name and column_table in join_tables:
                    column_join_tables = []
                    self.compile_join_column_tables(table_name, [join_tables[column_table]], join_tables, column_join_tables)
                    config["schema"][field_name] = self.compile_join_column(table_name, "$." + column_name, column_join_tables)
                else:
                    config["schema"][field_name] = "$." + column_name
                continue

            if isinstance(select_expression, expressions.Alias):
                if isinstance(select_expression.args["this"], expressions.Literal):
                    const_value = select_expression.args["this"].args["this"]
                    if select_expression.args["this"].is_int:
                        const_value = int(const_value)
                    config["schema"][select_expression.args["alias"].name] = ["#const", const_value]
                    continue

                if isinstance(select_expression.args["this"], expressions.Column):
                    column_table, column_name, column_alias = self.compile_select_column(select_expression.args["this"])
                    if primary_key is None or (column_alias and column_alias == "id"):
                        primary_key, primary_alias = column_name, column_alias
                    field_name = column_alias if column_alias else column_name
                    if column_table and column_table != table_name and column_table in join_tables:
                        column_join_tables = []
                        self.compile_join_column_tables(table_name, [join_tables[column_table]], join_tables,column_join_tables)
                        config["schema"][field_name] = self.compile_join_column(table_name, "$." + column_name, column_join_tables)
                    else:
                        config["schema"][field_name] = "$." + column_name
                    continue

                field_name = select_expression.args["alias"].name
                calculate_fields = []
                self.parse_calculate(table_name, select_expression.args["this"], calculate_fields)
                if calculate_fields:
                    column_join_tables = []
                    calculate_table_names = {calculate_field["table_name"] for calculate_field in calculate_fields}
                    self.compile_join_column_tables(table_name, [join_tables[calculate_table_name] for calculate_table_name in calculate_table_names],
                                                                            join_tables, column_join_tables)
                    calculate_column = self.compile_calculate(table_name, select_expression.args["this"], column_join_tables)
                    config["schema"][field_name] = self.compile_join_column(table_name, calculate_column, column_join_tables)
                else:
                    config["schema"][field_name] = self.compile_calculate(table_name, select_expression.args["this"], [])
                continue
            raise SyncanySqlCompileException("unkonw table select field: " + str(expression))

        where_expression = expression.args.get("where")
        if where_expression and isinstance(where_expression, expressions.Where):
            self.compile_where_condition(where_expression.args["this"], config)

        limit_expression = expression.args.get("limit")
        if limit_expression:
            arguments["@limit"] = int(limit_expression.args["expression"].args["this"])

    def compile_select_column(self, select_expression):
        column_table, column_name, column_alias = select_expression.args["table"].name if "table" in select_expression.args else None, \
                                                  select_expression.args["this"].name, None
        if "alias" in select_expression.args and select_expression.args["alias"]:
            column_alias = select_expression.args["alias"].name
        return column_table, column_name, column_alias

    def compile_join_column_tables(self, table_name, current_join_tables, join_tables, column_join_tables):
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
        column_join_names = sorted(list({join_field["table_name"] for join_table in current_join_tables
                                         for join_field in join_table["join_fields"]}),
                                   key=lambda x: 0xffffff if x == table_name else join_tables[x]["ref_count"])
        self.compile_join_column_tables(table_name, [join_tables[column_join_name] for column_join_name in column_join_names
                          if column_join_name != table_name], join_tables, column_join_tables)

    def compile_join_column(self, table_name, column, column_join_tables):
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
                join_fields = self.compile_calculate(table_name, join_table["calculate_expressions"][0], column_join_tables, i)
            else:
                join_fields = [self.compile_calculate(table_name, calculate_expression, column_join_tables, i)
                               for calculate_expression in join_table["calculate_expressions"]]
            join_db_table = "&." + join_table["db"] + "." + join_table["table"] + "::" + "+".join(join_table["primary_keys"])
            if join_table["querys"]:
                join_db_table = [join_db_table, join_table["querys"]]
            column = [join_fields, join_db_table, column]
        return column

    def compile_join_column_field(self, table_name, ci, join_field, column_join_tables):
        if join_field["table_name"] == table_name:
            return ("$" * (len(column_join_tables) - ci)) + "." + join_field["field_name"]
        ji = [j for j in range(len(column_join_tables)) if join_field["table_name"] == column_join_tables[j]["name"]][0]
        return ("$" * (ji - ci)) + "." + join_field["field_name"]

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
                "db": db, "table": table, "name": name, "primary_keys": set([]),
                "join_fields": [], "calculate_expressions": [], "querys": {}, "ref_count": 0
            }
            self.parse_on_condition(table_name, join_expression.args["on"], join_table)
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

    def parse_on_condition(self, table_name, expression, join_table):
        if not expression:
            return
        if isinstance(expression, expressions.And):
            self.parse_on_condition(expression.args.get("expression"), join_table)
            self.parse_on_condition(expression.args.get("this"), join_table)
            return

        def parse(expression):
            table_expression, value_expression = None, None
            for arg_expression in expression.args["this"], expression.args["expression"]:
                if not isinstance(arg_expression, expressions.Column):
                    continue
                condition_table = arg_expression.args["table"].name if "table" in arg_expression.args else None
                if condition_table and condition_table == join_table["name"]:
                    table_expression = arg_expression
                    break
            if table_expression is None:
                raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            value_expression = expression.args["expression"] if table_expression == expression.args["this"] else expression.args["this"]

            condition_table = table_expression.args["table"].name if "table" in table_expression.args else None
            condition_name = table_expression.args["this"].name
            if isinstance(value_expression, expressions.Literal):
                condition_value = value_expression.args["this"]
                if value_expression.is_int:
                    condition_name += "|int"
                if condition_name not in join_table["querys"]:
                    join_table["querys"][condition_name] = {}
                return (False, condition_table, condition_name, condition_value)
            if isinstance(value_expression, expressions.Column):
                if condition_table is None or "table" not in value_expression.args:
                    raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
                join_table["join_fields"].append({
                    "table_name": value_expression.args["table"].name,
                    "field_name": value_expression.args["this"].name
                })
                join_table["calculate_expressions"].append(value_expression)
                return (True, condition_table, condition_name, None)

            calculate_fields = []
            self.parse_calculate(table_name, value_expression, calculate_fields)
            join_table["join_fields"].extend(calculate_fields)
            join_table["calculate_expressions"].append(value_expression)
            return (True, condition_table, condition_name, None)

        if isinstance(expression, expressions.EQ):
            is_column, condition_table, condition_name, condition_value = parse(expression)
            if is_column:
                join_table["primary_keys"].add(condition_name)
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

    def parse_calculate(self, table_name, expression, calculate_fields):
        if isinstance(expression, expressions.Anonymous):
            for arg_expression in expression.args.get("expressions", []):
                self.parse_calculate(table_name, arg_expression, calculate_fields)
        elif isinstance(expression, expressions.Add):
            self.parse_calculate(table_name, expression.args["this"], calculate_fields)
            self.parse_calculate(table_name, expression.args["expression"], calculate_fields)
        elif isinstance(expression, expressions.Sub):
            self.parse_calculate(table_name, expression.args["this"], calculate_fields)
            self.parse_calculate(table_name, expression.args["expression"], calculate_fields)
        elif isinstance(expression, expressions.Mul):
            self.parse_calculate(table_name, expression.args["this"], calculate_fields)
            self.parse_calculate(table_name, expression.args["expression"], calculate_fields)
        elif isinstance(expression, expressions.Div):
            self.parse_calculate(table_name, expression.args["this"], calculate_fields)
            self.parse_calculate(table_name, expression.args["expression"], calculate_fields)
        elif isinstance(expression, expressions.Paren):
            self.parse_calculate(table_name, expression.args["this"], calculate_fields)
        elif isinstance(expression, expressions.Column):
            if table_name != expression.args["table"].name:
                calculate_fields.append({
                    "table_name": expression.args["table"].name,
                    "field_name": expression.args["this"].name
                })
        elif isinstance(expression, expressions.Literal):
            pass
        else:
            raise SyncanySqlCompileException("unkonw calculate: " + str(expression))

    def compile_calculate(self, table_name, expression, column_join_tables, join_index=-1):
        if isinstance(expression, expressions.Anonymous):
            column = ["@" + expression.args["this"]]
            for arg_expression in expression.args.get("expressions", []):
                column.append(self.compile_calculate(table_name, arg_expression, column_join_tables, join_index))
            return column
        elif isinstance(expression, expressions.Add):
            return ["@add",
                    self.compile_calculate(table_name, expression.args["this"], column_join_tables, join_index),
                    self.compile_calculate(table_name, expression.args["expression"], column_join_tables, join_index),
                    ]
        elif isinstance(expression, expressions.Sub):
            return ["@sub",
                    self.compile_calculate(table_name, expression.args["this"], column_join_tables, join_index),
                    self.compile_calculate(table_name, expression.args["expression"], column_join_tables, join_index),
                    ]
        elif isinstance(expression, expressions.Mul):
            return ["@mul",
                    self.compile_calculate(table_name, expression.args["this"], column_join_tables, join_index),
                    self.compile_calculate(table_name, expression.args["expression"], column_join_tables, join_index),
                    ]
        elif isinstance(expression, expressions.Div):
            return ["@div",
                    self.compile_calculate(table_name, expression.args["this"], column_join_tables, join_index),
                    self.compile_calculate(table_name, expression.args["expression"], column_join_tables, join_index),
                    ]
        elif isinstance(expression, expressions.Paren):
            return self.compile_calculate(table_name, expression.args["this"], column_join_tables, join_index)
        elif isinstance(expression, expressions.Column):
            join_field = {
                "table_name": expression.args["table"].name,
                "field_name": expression.args["this"].name
            }
            return self.compile_join_column_field(table_name, join_index, join_field, column_join_tables)
        elif isinstance(expression, expressions.Literal):
            const_value = expression.args["this"]
            if expression.is_int:
                const_value = int(const_value)
            return ["#const", const_value]
        else:
            raise SyncanySqlCompileException("unkonw calculate: " + str(expression))
