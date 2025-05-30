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
from .parser import SqlParser, FileParser


def main():
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    try:
        fileext = os.path.splitext(sys.argv[1])[1].lower() if len(sys.argv) >= 2 else None
    except:
        fileext = None
    if sys.stdin.isatty() and fileext and fileext not in (".sql", ".sqlx", ".prql"):
        print("usage: syncany [-h] sqlx|sql|prql")
        print("syncany error: require sqlx or sql or prql file")
        exit(2)

    try:
        if sys.platform != "win32":
            signal.signal(signal.SIGHUP, lambda signum, frame: executor.terminate())
            signal.signal(signal.SIGTERM, lambda signum, frame: executor.terminate())

        global_config = GlobalConfig()
        init_execute_files = global_config.load()
        if not sys.stdin.isatty() and (not fileext or fileext not in (".sql", ".sqlx", ".prql")):
            global_config.config_logging(False)
        else:
            global_config.config_logging(True)
        global_config.load_extensions()
        manager = TaskerManager(DatabaseManager())

        try:
            with Executor(manager, global_config.session()) as executor:
                if init_execute_files:
                    for init_execute_file in init_execute_files:
                        file_parser = FileParser(init_execute_file)
                        executor.run("init " + init_execute_file, file_parser.load())
                        executor.execute()

                if not sys.stdin.isatty() and (not fileext or fileext not in (".sql", ".sqlx", ".prql")):
                    start_time = time.time()
                    content = sys.stdin.read().strip()
                    if not content:
                        return
                    sql_parser = SqlParser(content[1:-1] if content[0] in ('"', "'") and content[-1] in ('"', "'") else content)
                    sqls = sql_parser.split()
                    executor.run("pipe", sqls)
                    get_logger().info("execute pipe sql start %d task", sys.argv[1], executor.get_tasker_index())
                    try:
                        executor.execute()
                    except Exception as e:
                        get_logger().info("execute pipe sql finish with Exception %s:%s %.2fms", e.__class__.__name__, e,
                                          (time.time() - start_time) * 1000)
                        raise e
                    else:
                        get_logger().info("execute pipe sql finish %.2fms", (time.time() - start_time) * 1000)
                elif len(sys.argv) >= 2:
                    start_time = time.time()
                    file_parser = FileParser(sys.argv[1])
                    sqls = file_parser.load()
                    executor.run(sys.argv[1], sqls)
                    get_logger().info("execute file %s start %d task", sys.argv[1], executor.get_tasker_index())
                    try:
                        executor.execute()
                    except Exception as e:
                        get_logger().info("execute file %s finish with Exception %s:%s %.2fms", sys.argv[1], e.__class__.__name__, e,
                                          (time.time() - start_time) * 1000)
                        raise e
                    else:
                        get_logger().info("execute file %s finish %.2fms", sys.argv[1], (time.time() - start_time) * 1000)
                else:
                    from .prompt import CliPrompt
                    cli_prompt = CliPrompt(manager, global_config.session(), executor)
                    cli_prompt.run()
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