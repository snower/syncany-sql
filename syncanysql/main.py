# -*- coding: utf-8 -*-
# 2023/2/7
# create by: snower

import sys
import os
import signal
import traceback
import time
from syncany.logger import get_logger
from syncany.taskers.manager import TaskerManager
from syncany.database.database import DatabaseManager
from .config import GlobalConfig
from syncanysql.executor import Executor
from .parser import SqlParser, FileParser, SqlSegment
from .prompt import CliPrompt

def main():
    if os.getcwd() not in sys.path:
        sys.path.append(os.getcwd())
    if sys.stdin.isatty() and len(sys.argv) >= 2 and not sys.argv[1].endswith(".sqlx") \
            and not sys.argv[1].endswith(".sql"):
        print("usage: syncany [-h] sqlx|sql")
        print("syncany error: require sqlx or sql file")
        exit(2)

    try:
        if sys.platform != "win32":
            signal.signal(signal.SIGHUP, lambda signum, frame: executor.terminate())
            signal.signal(signal.SIGTERM, lambda signum, frame: executor.terminate())

        global_config = GlobalConfig()
        init_execute_files = global_config.load()
        if not sys.stdin.isatty() and (len(sys.argv) == 1 or (
                len(sys.argv) >= 2 and not sys.argv[1].endswith(".sqlx") and not sys.argv[1].endswith(".sql"))):
            global_config.config_logging(False)
        else:
            global_config.config_logging(True)
        global_config.load_extensions()
        manager = TaskerManager(DatabaseManager())

        try:
            with Executor(manager, global_config.session()) as executor:
                if init_execute_files:
                    executor.run("init", [SqlSegment("execute `%s`" % init_execute_files[i], i + 1) for i in range(len(init_execute_files))])
                    exit_code = executor.execute()
                    if exit_code is not None and exit_code != 0:
                        return exit_code

                if not sys.stdin.isatty() and (len(sys.argv) == 1 or (
                        len(sys.argv) >= 2 and not sys.argv[1].endswith(".sqlx") and not sys.argv[1].endswith(".sql"))):
                    start_time = time.time()
                    content = sys.stdin.read().strip()
                    if not content:
                        exit(0)
                    sql_parser = SqlParser(content[1:-1] if content[0] in ('"', "'") and content[-1] in ('"', "'") else content)
                    sqls = sql_parser.split()
                    executor.run("pipe", sqls)
                    exit_code = executor.execute()
                    get_logger().info("execute pipe sql finish with code %d %.2fms", exit_code, (time.time() - start_time) * 1000)
                    exit(exit_code)
                elif len(sys.argv) >= 2:
                    start_time = time.time()
                    file_parser = FileParser(sys.argv[1])
                    sqls = file_parser.load()
                    executor.run(sys.argv[1], sqls)
                    exit_code = executor.execute()
                    get_logger().info("execute file %s finish with code %d %.2fms", sys.argv[1], exit_code, (time.time() - start_time) * 1000)
                    exit(exit_code)
                else:
                    cli_prompt = CliPrompt(manager, global_config.session(), executor)
                    exit(cli_prompt.run())
        finally:
            manager.close()
    except SystemError:
        get_logger().error("signal exited")
        exit(130)
    except KeyboardInterrupt:
        get_logger().error("Crtl+C exited")
        exit(130)
    except Exception as e:
        get_logger().error("%s\n%s", e, traceback.format_exc())
        exit(1)

if __name__ == "__main__":
    main()