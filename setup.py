#  Copyright (C) 2022 Max Run Software (dev@maxrunsoftware.com)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from flask import Flask

import config
from config import Config
from web_app import init_app_web
from utils import *


config.init_logging()


_SERVER_TYPE_WEB = 1
_SERVER_TYPE_SCHEDULER = 2

def get_server_type() -> int:
    st = trim_casefold(Config.SERVER_TYPE)
    if st is not None:
        if st == "web".casefold(): return _SERVER_TYPE_WEB
        if st == "scheduler".casefold(): return _SERVER_TYPE_SCHEDULER
    raise ValueError(f"Invalid server type '{st}'")


web_app: Flask | None = init_app_web() if get_server_type() == _SERVER_TYPE_WEB else None

def main():

    st = get_server_type()
    if st == _SERVER_TYPE_WEB:
        web_app.run(
            host=Config.WEB_HOST,
            port=Config.WEB_PORT,
            debug=Config.DEBUG,
            load_dotenv=False,
        )

    elif st == _SERVER_TYPE_SCHEDULER:
        pass


if __name__ == "__main__":
    main()
