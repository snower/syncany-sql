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
from .calculaters import mysql_funcs

SQL_COMPLETER_WORDS = [
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
]

style = Style.from_dict(
    {
        "completion-menu.completion": "bg:#008888 #ffffff",
        "completion-menu.completion.current": "bg:#00aaaa #000000",
        "scrollbar.background": "bg:#88aaaa",
        "scrollbar.button": "bg:#222222",
    }
)


class CliFileHistory(FileHistory):
    def load_history_strings(self):
        strings, time_strings, lines, time_line = [], [], [], ''
        if os.path.exists(self.filename):
            with open(self.filename, "rb") as f:
                for line_bytes in f:
                    line = line_bytes.decode("utf-8", errors="replace")
                    if line.startswith("#"):
                        time_line = line
                    elif line.startswith("+"):
                        lines.append(line[1:])
                        continue
                    if not lines:
                        continue
                    string, lines = "".join(lines)[:-1], []
                    strings.append(string)
                    time_strings.append(time_line)

                if lines:
                    string, lines = "".join(lines)[:-1], []
                    strings.append(string)
                    time_strings.append(time_line)

        if len(strings) <= 50:
            return reversed(strings)
        with open(self.filename, "wb") as f:
            for i in range(50):
                f.write(("\n%s" % time_strings[i - 50]).encode("utf-8"))
                for line in strings[i - 50].split("\n"):
                    f.write(("+%s\n" % line).encode("utf-8"))
        return reversed(strings)
    
    def store_string(self, string):
        if string.lower()[:4] == "exit":
            return 
        super(CliFileHistory, self).store_string(string)


class CliPrompt(object):
    def __init__(self, manager, session_config, executor):
        self.manager = manager
        self.session_config = session_config
        self.executor = executor

    def run(self):
        home_config_path = self.session_config.get_home()
        if not os.path.exists(home_config_path):
            os.mkdir(home_config_path)
        database_completer_words = [database["name"] for database in (self.session_config.get().get("databases") or [])
                                    if database["name"] not in ("-", "--")]
        sql_completer = WordCompleter(list(set(SQL_COMPLETER_WORDS + list(mysql_funcs.funcs.keys()) + database_completer_words)), ignore_case=True)
        history = CliFileHistory(os.path.join(home_config_path, "history"))
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
                    return
                if text[0] == "!":
                    self.run_system_cmd(text[1:])
                    continue
                if text[0] == "%":
                    self.run_magic_cmd(text[1:])
                    continue
                self.executor.run("cli", [SqlSegment(text, lineno)])
                try:
                    self.executor.execute()
                except Exception as e:
                    print(e.__class__.__name__ + ": " + str(e))
                finally:
                    lineno += 1
                    self.executor.runners.clear()
            except KeyboardInterrupt:
                continue  # Control-C pressed. Try again.
            except EOFError:
                return 130
            except Exception as e:
                print(e.__class__.__name__ + ": " + str(e))

    def check_complete(self, content):
        content = content.strip()
        if not content or content[:4] == "exit" or content[0] in ("!", "%"):
            return False
        sql_parser = SqlParser(content)
        try:
            sql_parser.read_util_complete()
        except EOFError:
            return True
        return False

    def run_system_cmd(self, cmd):
        os.system(cmd)

    def run_magic_cmd(self, cmd):
        if cmd[:3].lower() == "cd ":
            os.chdir(cmd[3:])
            self.executor.global_env_variables["cwd"] = os.getcwd()
