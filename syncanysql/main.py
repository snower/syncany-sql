# -*- coding: utf-8 -*-
# 2023/2/7
# create by: snower

import sys
import signal
import traceback
from syncany.logger import get_logger
from syncany.taskers.manager import TaskerManager
from syncany.database.database import DatabaseManager
from .config import Config
from .tasker import Tasker
from .file_parser import FileParser
from .prompt import CliPrompt

def main():
    if len(sys.argv) >= 2 and not sys.argv[1].endswith("sqlx") and not sys.argv[1].endswith("sql"):
        print("usage: syncany [-h] sqlx|sql")
        print("syncany error: require sqlx or sql file")
        exit(2)

    try:
        if sys.platform != "win32":
            signal.signal(signal.SIGHUP, lambda signum, frame: tasker.terminate())
            signal.signal(signal.SIGTERM, lambda signum, frame: tasker.terminate())

        config = Config()
        config.load()
        manager = TaskerManager(DatabaseManager())

        try:
            if len(sys.argv) >= 2:
                file_parser = FileParser(sys.argv[1])
                sqls = file_parser.load()
                tasker = Tasker(manager, config)
                exit(tasker.run(sys.argv[1], sqls))
            else:
                cli_prompt = CliPrompt(manager, config)
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