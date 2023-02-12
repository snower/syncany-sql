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
            "output": "&.-.&1::id",
            "querys": {},
            "schema": "$.*",
            "intercepts": [],
            "dependencys": [],
            "pipelines": []
        })
        arguments.update({"@timeout": 0, "@limit": 100})
        if isinstance(expression, expressions.Union):
            self.compile_union(expression, config, arguments)
        elif isinstance(expression, expressions.Insert):
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
            config["output"] = "&.-.&1::id"
        else:
            config["output"] = "".join(["&.", expression.args["db"].name, ".", expression.args["this"].name, "::", "id"])
        select_expression = expression.args.get("expression")
        if not select_expression or not isinstance(select_expression, (expressions.Select, expressions.Union)):
            raise SyncanySqlCompileException("unkonw insert info select: " + str(expression))
        if isinstance(select_expression, expressions.Union):
            self.compile_union(select_expression, config, arguments)
        else:
            self.compile_select(select_expression, config, arguments)

    def compile_union(self, expression, config, arguments):
        select_expressions = []
        def parse(union_expression):
            if isinstance(union_expression.args["this"], expressions.Select):
                select_expressions.append(union_expression.args["this"])
            else:
                parse(union_expression.args["this"])
            if isinstance(union_expression.args["expression"], expressions.Select):
                select_expressions.append(union_expression.args["expression"])
            else:
                parse(union_expression.args["expression"])
        parse(expression)

        query_name = "__unionquery_" + str(id(expression))
        for select_expression in select_expressions:
            subquery_name = "__unionquery_" + str(id(expression)) + "_" + str(id(select_expression))
            subquery_arguments = {}
            subquery_config = self.compile_expression(expression.args["this"], subquery_arguments)
            subquery_config["output"] = "&.--." + query_name + "::" + subquery_config["output"].split("::")[-1].split(" ")[0] + " use I"
            subquery_config["name"] = subquery_config["name"] + "#" + subquery_name
            arguments.update({subquery_config["name"] + "@" + key: value for key, value in subquery_arguments.items()})
            config["dependencys"].append(subquery_config)
        config["input"] = "&.--." + query_name + "::" + config["dependencys"][0]["output"].split("::")[-1].split(" ")[0]
        config["output"] = config["output"].split("::")[0] + "::" + config["input"].split("::")[-1].split(" ")[0]

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
        group_fields = []
        group_expression = expression.args.get("group")
        if group_expression:
            self.parse_group(table_name, group_expression, group_fields)

        select_expressions = expression.args.get("expressions")
        if not select_expressions:
            raise SyncanySqlCompileException("unkonw table selects: " + str(expression))
        config["schema"] = {}
        for select_expression in select_expressions:
            if isinstance(select_expression, expressions.Star):
                config["schema"] = "$.*"
                break

            column_expression, aggregate_expression, column_alias = None, None, None
            if isinstance(select_expression, expressions.Column):
                column_expression = select_expression
            elif isinstance(select_expression, expressions.Alias):
                column_alias = select_expression.args["alias"].name if "alias" in select_expression.args else None
                if isinstance(select_expression.args["this"], expressions.Literal):
                    literal_info = self.parse_literal(select_expression.args["this"])
                    config["schema"][column_alias] = self.compile_literal(literal_info)
                    continue
                elif isinstance(select_expression.args["this"], expressions.Column):
                    column_expression = select_expression.args["this"]
                elif isinstance(select_expression.args["this"], (expressions.Count, expressions.Sum, expressions.Min, expressions.Max)):
                    aggregate_expression = select_expression.args["this"]
            else:
                raise SyncanySqlCompileException("unkonw table select field: " + str(expression))
            if column_expression:
                primary_key, primary_alias = self.compile_select_column(table_name, column_expression, column_alias, config,
                                                                        join_tables, primary_key, primary_alias)
                continue
            if aggregate_expression:
                self.compile_aggregate_column(table_name, column_alias, config, group_expression, aggregate_expression,
                                              group_fields, join_tables)
                continue

            column_alias = select_expression.args["alias"].name
            calculate_fields = []
            self.parse_calculate(table_name, select_expression.args["this"], calculate_fields)
            if calculate_fields:
                column_join_tables = []
                calculate_table_names = {calculate_field["table_name"] for calculate_field in calculate_fields}
                self.compile_join_column_tables(table_name, [join_tables[calculate_table_name] for calculate_table_name in calculate_table_names],
                                                                        join_tables, column_join_tables)
                calculate_column = self.compile_calculate(table_name, select_expression.args["this"], column_join_tables)
                config["schema"][column_alias] = self.compile_join_column(table_name, calculate_column, column_join_tables)
            else:
                config["schema"][column_alias] = self.compile_calculate(table_name, select_expression.args["this"], [])

        where_expression = expression.args.get("where")
        if where_expression and isinstance(where_expression, expressions.Where):
            self.compile_where_condition(table_name, where_expression.args["this"], config)

        having_expression = expression.args.get("having")
        if having_expression:
            config["intercepts"].append(self.compile_having_condition(table_name, having_expression.args["this"], config))

        order_expression = expression.args.get("order")
        if order_expression:
            self.compile_order(table_name, order_expression.args["expressions"], config)

        limit_expression = expression.args.get("limit")
        if limit_expression:
            arguments["@limit"] = int(limit_expression.args["expression"].args["this"])

    def compile_select_column(self, table_name, column_expression, column_alias, config, join_tables, primary_key, primary_alias):
        column_info = self.parse_column(column_expression)
        if not column_alias:
            column_alias = column_info["column_name"]
        if primary_key is None or (column_alias and column_alias == "id"):
            primary_key, primary_alias = column_info["column_name"], column_alias
        if column_info["table_name"] and column_info["table_name"] != table_name and column_info["table_name"] in join_tables:
            column_join_tables = []
            self.compile_join_column_tables(table_name, [join_tables[column_info["table_name"]]], join_tables,
                                            column_join_tables)
            config["schema"][column_alias] = self.compile_join_column(table_name, self.compile_column(column_info),
                                                                                   column_join_tables)
        else:
            config["schema"][column_alias] = self.compile_column(column_info)
        return primary_key, primary_alias

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
        column_join_names = sorted(list({join_column["table_name"] for join_table in current_join_tables
                                         for join_column in join_table["join_columns"]}),
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
                join_columns = self.compile_calculate(table_name, join_table["calculate_expressions"][0], column_join_tables, i)
            else:
                join_columns = [self.compile_calculate(table_name, calculate_expression, column_join_tables, i)
                               for calculate_expression in join_table["calculate_expressions"]]
            join_db_table = "&." + join_table["db"] + "." + join_table["table"] + "::" + "+".join(join_table["primary_keys"])
            if join_table["querys"]:
                join_db_table = [join_db_table, join_table["querys"]]
            column = [join_columns, join_db_table, column]
        return column

    def compile_join_column_field(self, table_name, ci, join_column, column_join_tables):
        if join_column["table_name"] == table_name:
            return self.compile_column(join_column, len(column_join_tables) - ci)
        ji = [j for j in range(len(column_join_tables)) if join_column["table_name"] == column_join_tables[j]["name"]][0]
        return self.compile_column(join_column, ji - ci)

    def compile_where_condition(self, table_name, expression, config):
        if not expression:
            return
        if isinstance(expression, expressions.And):
            self.compile_where_condition(table_name, expression.args.get("expression"), config)
            self.compile_where_condition(table_name, expression.args.get("this"), config)
            return

        def parse(expression):
            table_expression, value_expression = expression.args["this"], expression.args["expression"]
            if not isinstance(table_expression, expressions.Column):
                raise SyncanySqlCompileException("unkonw where condition: " + str(expression))
            condition_table = table_expression.args["table"].name if "table" in table_expression.args else None
            if condition_table and condition_table != table_name:
                raise SyncanySqlCompileException("unkonw where condition: " + str(expression))

            condition_column = self.parse_column(table_expression)
            calculate_fields = []
            self.parse_calculate("", value_expression, calculate_fields)
            if calculate_fields:
                raise SyncanySqlCompileException("unkonw where condition: " + str(expression))
            value_column = self.compile_calculate(table_name, value_expression, [])
            if condition_column["column_name"] not in config["querys"]:
                config["querys"][condition_column["column_name"]] = {}
            return (condition_column, value_column)

        if isinstance(expression, expressions.EQ):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["column_name"]]["=="] = value_column
        elif isinstance(expression, expressions.NEQ):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["column_name"]]["!="] = value_column
        elif isinstance(expression, expressions.GT):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["column_name"]][">"] = value_column
        elif isinstance(expression, expressions.GTE):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["column_name"]][">="] = value_column
        elif isinstance(expression, expressions.LT):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["column_name"]]["<"] = value_column
        elif isinstance(expression, expressions.LTE):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["column_name"]]["<="] = value_column
        elif isinstance(expression, expressions.In):
            condition_column, value_column = parse(expression)
            config["querys"][condition_column["column_name"]]["in"] = value_column
        else:
            raise SyncanySqlCompileException("unkonw where condition: " + str(expression))

    def compile_aggregate_column(self, table_name, column_alias, config, group_expression, aggregate_expression, group_fields, join_tables):
        calculate_fields = [group_field for group_field in group_fields]
        self.parse_aggregate(table_name, aggregate_expression, calculate_fields)
        column_join_tables = []
        if calculate_fields:
            calculate_table_names = {calculate_field["table_name"] for calculate_field in calculate_fields}
            self.compile_join_column_tables(table_name, [join_tables[calculate_table_name] for calculate_table_name in calculate_table_names],
                                            join_tables, column_join_tables)
        if len(group_expression.args["expressions"]) > 1:
            group_column = ["@add"]
            for expression in group_expression.args["expressions"]:
                group_column.append(self.compile_calculate(table_name, expression, column_join_tables, -1))
        else:
            group_column = self.compile_calculate(table_name, group_expression.args["expressions"][0], column_join_tables, -1)
        calculate_column = self.compile_aggtegate(table_name, column_alias, aggregate_expression, column_join_tables)
        if calculate_fields:
            config["schema"][column_alias] = self.compile_join_column(table_name, ["#aggregate", group_column, calculate_column],
                                                                      column_join_tables)
        else:
            config["schema"][column_alias] = ["#aggregate", group_column, calculate_column]

    def compile_aggtegate(self, table_name, column_alias, expression, column_join_tables, join_index=-1):
        if isinstance(expression, expressions.Count):
            return ["@add", "$." + column_alias + "|int", 1]
        elif isinstance(expression, expressions.Sum):
            return ["@add", "$." + column_alias + "|float",
                    self.compile_calculate(table_name, expression.args["this"], column_join_tables, join_index - 1),
                    ]
        elif isinstance(expression, expressions.Min):
            return ["@min", "$." + column_alias + "|float",
                    self.compile_calculate(table_name, expression.args["this"], column_join_tables, join_index - 1),
                    ]
        elif isinstance(expression, expressions.Max):
            return ["@max", "$." + column_alias + "|float",
                    self.compile_calculate(table_name, expression.args["this"], column_join_tables, join_index - 1),
                    ]
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
            join_column = self.parse_column(expression)
            return self.compile_join_column_field(table_name, join_index, join_column, column_join_tables)
        elif isinstance(expression, expressions.Star):
            return "$.*"
        elif isinstance(expression, expressions.Literal):
            return self.compile_literal(self.parse_literal(expression))
        else:
            raise SyncanySqlCompileException("unkonw calculate: " + str(expression))

    def compile_having_condition(self, table_name, expression, config):
        if isinstance(expression, expressions.And):
            return ["@and",
                self.compile_having_condition(table_name, expression.args.get("expression"), config),
                self.compile_having_condition(table_name, expression.args.get("this"), config)
            ]
        if isinstance(expression, expressions.Or):
            return ["@or",
                self.compile_having_condition(table_name, expression.args.get("expression"), config),
                self.compile_having_condition(table_name, expression.args.get("this"), config)
            ]

        def parse(expression):
            table_expression, value_expression = expression.args["this"], expression.args["expression"]
            if not isinstance(table_expression, expressions.Column) or "table" in table_expression.args:
                raise SyncanySqlCompileException("unkonw having condition: " + str(expression))
            condition_column = self.parse_column(table_expression)
            calculate_fields = []
            self.parse_calculate("", value_expression, calculate_fields)
            if calculate_fields:
                raise SyncanySqlCompileException("unkonw having condition: " + str(expression))
            return (self.compile_column(condition_column), self.compile_calculate(table_name, value_expression, []))

        if isinstance(expression, expressions.EQ):
            column, value = parse(expression)
            return ["@eq", column, value]
        elif isinstance(expression, expressions.NEQ):
            column, value = parse(expression)
            return ["@neq", column, value]
        elif isinstance(expression, expressions.GT):
            column, value = parse(expression)
            return ["@gt", column, value]
        elif isinstance(expression, expressions.GTE):
            column, value = parse(expression)
            return ["@gte", column, value]
        elif isinstance(expression, expressions.LT):
            column, value = parse(expression)
            return ["@lt", column, value]
        elif isinstance(expression, expressions.LTE):
            column, value = parse(expression)
            return ["@lte", column, value]
        elif isinstance(expression, expressions.In):
            column, value = parse(expression)
            return ["@in", column, value]
        elif isinstance(expression, expressions.Paren):
            return self.compile_having_condition(table_name, expression.args.get("this"), config),
        else:
            raise SyncanySqlCompileException("unkonw where condition: " + str(expression))

    def compile_order(self, table_name, expressions, config):
        for expression in expressions:
            column = self.parse_column(expression.args["this"])
            config["pipelines"].append([">>@sort", "$.*|array", expression.args["desc"], column["column_name"]])
            break
        
    def compile_column(self, column, scope_depth=1):
        return ("$" * scope_depth) + "." + column["column_name"]
    
    def compile_literal(self, literal):
        return ["#const", literal["value"]]

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
                "join_columns": [], "calculate_expressions": [], "querys": {}, "ref_count": 0
            }
            self.parse_on_condition(table_name, join_expression.args["on"], join_table)
            if not join_table["primary_keys"] or not join_table["join_columns"]:
                raise SyncanySqlCompileException("empty join table: " + str(join_expression))
            join_tables[join_table["name"]] = join_table

        for name, join_table in join_tables.items():
            for join_column in join_table["join_columns"]:
                if join_column["table_name"] == table_name:
                    continue
                if join_column["table_name"] not in join_tables:
                    raise SyncanySqlCompileException("unknown join table: " + join_column["table_name"])
                join_tables[join_column["table_name"]]["ref_count"] += 1
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
            if isinstance(expression, expressions.EQ):
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
            else:
                table_expression, value_expression = expression.args["this"], expression.args["expression"]

            condition_column = self.parse_column(table_expression)
            if not isinstance(expression, expressions.EQ) or isinstance(value_expression, expressions.Literal):
                if not isinstance(table_expression, expressions.Column) or "table" not in table_expression.args \
                        or not table_expression.args["table"].name:
                    raise SyncanySqlCompileException("unkonw where condition: " + str(expression))
                calculate_fields = []
                self.parse_calculate("", value_expression, calculate_fields)
                if calculate_fields:
                    raise SyncanySqlCompileException("unkonw where condition: " + str(expression))
                return (False, condition_column, self.compile_calculate(table_name, value_expression, []))

            if isinstance(value_expression, expressions.Column):
                join_table["join_columns"].append(self.parse_column(value_expression))
                join_table["calculate_expressions"].append(value_expression)
                return (True, condition_column, None)
            calculate_fields = []
            self.parse_calculate("", value_expression, calculate_fields)
            if not calculate_fields and condition_column["table_name"] == join_table["name"]:
                return (False, condition_column, self.compile_calculate(table_name, value_expression, []))
            join_table["join_columns"].extend([calculate_field for calculate_field in calculate_fields
                                               if calculate_field["table_name"] != table_name])
            join_table["calculate_expressions"].append(value_expression)
            return (True, condition_column, None)

        if isinstance(expression, expressions.EQ):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                join_table["primary_keys"].add(condition_column["column_name"])
            else:
                join_table["querys"][condition_column["column_name"]]["=="] = value_column
        elif isinstance(expression, expressions.NEQ):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            join_table["querys"][condition_column["column_name"]]["!="] = value_column
        elif isinstance(expression, expressions.GT):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            join_table["querys"][condition_column["column_name"]][">"] = value_column
        elif isinstance(expression, expressions.GTE):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            join_table["querys"][condition_column["column_name"]][">="] = value_column
        elif isinstance(expression, expressions.LT):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            join_table["querys"][condition_column["column_name"]]["<"] = value_column
        elif isinstance(expression, expressions.LTE):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            join_table["querys"][condition_column["column_name"]]["<="] = value_column
        elif isinstance(expression, expressions.In):
            is_column, condition_column, value_column = parse(expression)
            if is_column:
                raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))
            join_table["querys"][condition_column["column_name"]]["in"] = value_column
        else:
            raise SyncanySqlCompileException("unkonw join on condition: " + str(expression))

    def parse_group(self, table_name, expression, group_fields):
        for group_expression in expression.args["expressions"]:
            self.parse_calculate(table_name, group_expression, group_fields)

    def parse_aggregate(self, table_name, expression, calculate_fields):
        if isinstance(expression, expressions.Count):
            self.parse_calculate(table_name, expression.args["this"], calculate_fields)
        elif isinstance(expression, expressions.Sum):
            self.parse_calculate(table_name, expression.args["this"], calculate_fields)
        elif isinstance(expression, expressions.Min):
            self.parse_calculate(table_name, expression.args["this"], calculate_fields)
        elif isinstance(expression, expressions.Max):
            self.parse_calculate(table_name, expression.args["this"], calculate_fields)
        else:
            raise SyncanySqlCompileException("unkonw aggregate calculate: " + str(expression))

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
                calculate_fields.append(self.parse_column(expression))
        elif isinstance(expression, expressions.Star):
            pass
        elif isinstance(expression, expressions.Literal):
            pass
        else:
            raise SyncanySqlCompileException("unkonw calculate: " + str(expression))
        
    def parse_column(self, expression):
        table_name = expression.args["table"].name if "table" in expression.args else None
        column_name = expression.args["this"].name
        return {
            "table_name": table_name,
            "column_name": column_name,
            "expression": expression
        }
    
    def parse_literal(self, expression):
        value = expression.args["this"]
        if expression.is_int:
            value = int(value)
        return {
            "value": value,
            "expression": expression
        }
