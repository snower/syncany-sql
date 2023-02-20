# -*- coding: utf-8 -*-
# 2023/2/7
# create by: snower

import sys
import signal
import traceback
import time
from syncany.logger import get_logger
from syncany.taskers.manager import TaskerManager
from syncany.database.database import DatabaseManager
from .config import GlobalConfig
from syncanysql.executor import Executor
from .parser import FileParser
from .prompt import CliPrompt

def main():
    if len(sys.argv) >= 2 and not sys.argv[1].endswith("sqlx") and not sys.argv[1].endswith("sql"):
        print("usage: syncany [-h] sqlx|sql")
        print("syncany error: require sqlx or sql file")
        exit(2)

    try:
        if sys.platform != "win32":
            signal.signal(signal.SIGHUP, lambda signum, frame: executor.terminate())
            signal.signal(signal.SIGTERM, lambda signum, frame: executor.terminate())

        global_config = GlobalConfig()
        global_config.load()
        global_config.config_logging()
        manager = TaskerManager(DatabaseManager())

        try:
            if len(sys.argv) >= 2:
                start_time = time.time()
                file_parser = FileParser(sys.argv[1])
                sqls = file_parser.load()
                executor = Executor(manager, global_config.session())
                executor.run(sys.argv[1], sqls)
                exit_code = executor.execute()
                get_logger().info("execute file %s finish with code %d %.2fms", sys.argv[1], exit_code, (time.time() - start_time) * 1000)
                exit(exit_code)
            else:
                cli_prompt = CliPrompt(manager, global_config.session())
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