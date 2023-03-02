# -*- coding: utf-8 -*-
# 2023/2/8
# create by: snower

import os
import sys
from pygments.lexers import SqlLexer
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.styles import Style
from prompt_toolkit.filters import Condition
from prompt_toolkit.cursor_shapes import CursorShape
from .version import version
from .parser import SqlParser, SqlSegment

sql_completer = WordCompleter(
    [
        "abort",
        "action",
        "add",
        "after",
        "all",
        "alter",
        "analyze",
        "and",
        "as",
        "asc",
        "attach",
        "autoincrement",
        "before",
        "begin",
        "between",
        "by",
        "cascade",
        "case",
        "cast",
        "check",
        "collate",
        "column",
        "commit",
        "conflict",
        "constraint",
        "create",
        "cross",
        "current_date",
        "current_time",
        "current_timestamp",
        "database",
        "default",
        "deferrable",
        "deferred",
        "delete",
        "desc",
        "detach",
        "distinct",
        "drop",
        "each",
        "else",
        "end",
        "escape",
        "except",
        "exclusive",
        "exists",
        "explain",
        "fail",
        "for",
        "foreign",
        "from",
        "full",
        "glob",
        "group",
        "having",
        "if",
        "ignore",
        "immediate",
        "in",
        "index",
        "indexed",
        "initially",
        "inner",
        "insert",
        "instead",
        "intersect",
        "into",
        "is",
        "isnull",
        "join",
        "key",
        "left",
        "like",
        "limit",
        "match",
        "natural",
        "no",
        "not",
        "notnull",
        "null",
        "of",
        "offset",
        "on",
        "or",
        "order",
        "outer",
        "plan",
        "pragma",
        "primary",
        "query",
        "raise",
        "recursive",
        "references",
        "regexp",
        "reindex",
        "release",
        "rename",
        "replace",
        "restrict",
        "right",
        "rollback",
        "row",
        "savepoint",
        "select",
        "set",
        "table",
        "temp",
        "temporary",
        "then",
        "to",
        "transaction",
        "trigger",
        "union",
        "unique",
        "update",
        "using",
        "vacuum",
        "values",
        "view",
        "virtual",
        "when",
        "where",
        "with",
        "without",
    ],
    ignore_case=True,
)

style = Style.from_dict(
    {
        "completion-menu.completion": "bg:#008888 #ffffff",
        "completion-menu.completion.current": "bg:#00aaaa #000000",
        "scrollbar.background": "bg:#88aaaa",
        "scrollbar.button": "bg:#222222",
    }
)

class CliPrompt(object):
    def __init__(self, manager, session_config, executor):
        self.manager = manager
        self.session_config = session_config
        self.executor = executor

    def run(self):
        home_config_path = os.path.join(os.path.expanduser('~'), ".syncany")
        if not os.path.exists(home_config_path):
            os.mkdir(home_config_path)
        history = FileHistory(os.path.join(home_config_path, "history"))
        session = PromptSession(
            lexer=PygmentsLexer(SqlLexer), completer=sql_completer, style=style, history=history
        )
        print("Python %s" % sys.version)
        print("Syncany-SQL %s -- Simple and easy-to-use sql execution engine" % version)
        lineno = 1
        while True:
            try:
                text = session.prompt("> ", multiline=Condition(lambda: self.check_complete(session.app.current_buffer.text)),
                                      cursor=CursorShape.BEAM)
                text = text.strip()
                if not text:
                    continue
                if text.lower()[:4] == "exit":
                    return 0
                self.executor.run("cli", [SqlSegment(text, lineno)])
                try:
                    self.executor.execute()
                finally:
                    lineno += 1
                    self.executor.runners.clear()
            except KeyboardInterrupt:
                continue  # Control-C pressed. Try again.
            except EOFError:
                return 130
            except Exception as e:
                print(str(e))
        return 0

    def check_complete(self, content):
        if not content.strip():
            return False
        if content[:4] == "exit":
            return False
        sql_parser = SqlParser(content)
        try:
            sql_parser.read_util_complete()
        except EOFError:
            return True
        return False
