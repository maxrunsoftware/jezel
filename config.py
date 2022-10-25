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

from __future__ import annotations

import os
import secrets
from multiprocessing import Lock

from dotenv import find_dotenv, load_dotenv

from utils import *

PROJECT_NAME = "JEZEL"
_LOGGING_INITIALIZED = False
_LOGGING_INITIALIZED_LOCK = Lock()

load_dotenv(find_dotenv("config.env"))  # take environment variables from config.env
load_dotenv(find_dotenv(".env"))  # take environment variables from .env


def _get_str(name: str, default: Any | None = None) -> str | None:
    value = os.environ.get(PROJECT_NAME + "_" + name)
    value = trim(xstr(value))
    return value if value is not None else trim(xstr(default))


def _get_bool(name: str, default: bool | None = None) -> bool | None:
    v = _get_str(name, default)
    return None if v is None else bool_parse(v)


def _get_int(name: str, default: int | None = None) -> int | None:
    v = _get_str(name, default)
    return None if v is None else int(v)


def _get_float(name: str, default: float | None = None) -> float | None:
    v = _get_str(name, default)
    return None if v is None else float(v)


# https://stackoverflow.com/a/1800999
# noinspection PyPep8Naming
class ConfigMeta(type):

    # region Common

    @property
    def PROJECT_NAME(cls) -> str:
        return PROJECT_NAME

    @property
    def DEBUG(cls) -> bool:
        return _get_bool("DEBUG", False)

    @property
    def LOG_FORMAT(cls) -> str:
        format_default = "%(asctime)s %(levelname)-8s %(message)s"
        format_default_debug = "%(asctime)s, %(levelname)-8s [%(name)s] [%(filename)s:%(module)s:%(funcName)s:%(lineno)d] %(message)s"
        return _get_str("LOG_FORMAT", format_default_debug if Config.DEBUG else format_default)

    @property
    def LOG_LEVEL(cls) -> str:
        return _get_str("LOG_LEVEL", "DEBUG" if Config.DEBUG else "INFO")

    @property
    def LOG_LEVEL_SQLALCHEMY(cls) -> str:
        return _get_str("LOG_LEVEL_SQLALCHEMY", None)

    @property
    def SERVER_TYPE(cls) -> str:
        return _get_str("SERVER_TYPE", None)

    @property
    def DATABASE_URI(cls) -> str:
        # return _get_str("DATABASE_URI", "sqlite:///:memory:")
        return _get_str("DATABASE_URI", "sqlite+pysqlite:///:memory:")

    @property
    def DATABASE_TABLE(cls) -> str:
        # return _get_str("DATABASE_URI", "sqlite:///:memory:")
        return _get_str("DATABASE_TABLE", PROJECT_NAME.casefold() + "_data")


    # endregion Common

    @property
    def ADMIN_DEFAULT_USERNAME(cls) -> str:
        return _get_str("ADMIN_DEFAULT_USERNAME", "admin")

    @property
    def ADMIN_DEFAULT_PASSWORD(cls) -> str:
        return _get_str("ADMIN_DEFAULT_PASSWORD", "password")

    # region Web

    @property
    def WEB_HOST(cls) -> str:
        return _get_str("WEB_HOST", "0.0.0.0")

    @property
    def WEB_PORT(cls) -> int:
        return _get_int("WEB_PORT", 5000)

    @property
    def WEB_PRETTY_HTML(cls) -> bool:
        return _get_bool("WEB_PRETTY_HTML", True if Config.DEBUG else False)

    @property
    def WEB_SESSION_COOKIE_NAME(cls) -> str:
        return _get_str("WEB_SESSION_COOKIE_NAME", PROJECT_NAME.casefold())

    @property
    def WEB_SESSION_COOKIE_DOMAIN(cls) -> str:
        return _get_str("WEB_SESSION_COOKIE_DOMAIN", None)

    @property
    def WEB_SECRET_KEY(cls) -> str:
        return _get_str("WEB_SECRET_KEY", "dev" if Config.DEBUG else secrets.token_hex())

    @property
    def WEB_SERVER_NAME(cls) -> str:
        return _get_str("WEB_SERVER_NAME", None)

    # endregion Web

    # region Scheduler

    @property
    def SCHEDULER_PROCESS_COUNT(self):
        return _get_int("SCHEDULER_PROCESS_COUNT", None)

    # endregion Scheduler

    def attributes(cls) -> DictStrCasefold[str, Any | None]:
        d = {}
        for name in sorted(dir(ConfigMeta), key=lambda x: x.casefold()):
            if name.startswith("_"): continue
            if not hasattr(cls, name): continue
            value = getattr(cls, name)
            if callable(value): continue
            d[name] = value
        return DictStrCasefold(d)


class Config(metaclass=ConfigMeta):
    pass


def init_logging():
    global _LOGGING_INITIALIZED
    with _LOGGING_INITIALIZED_LOCK:
        if _LOGGING_INITIALIZED:
            print_error("Error: Logging already initialized")
            return None
        _LOGGING_INITIALIZED = True

    # init logging

    def parse_log_level(level_name: str ) -> int | None:
        level_name = trim(xstr(Config.LOG_LEVEL))
        if level_name is None:
            level_name = "DEBUG" if Config.DEBUG else "INFO"
        level_int = logging.getLevelName(level_name)
        if type(level_int) is int: return level_int
        level_int = logging.getLevelName(level_name.upper())
        if type(level_int) is int: return level_int
        return None

    li = parse_log_level(Config.LOG_LEVEL)
    if li is None:
        print_error(f"{__name__}.init_app() -> Invalid LOG_LEVEL '{Config.LOG_LEVEL}' specified, defaulting to 'INFO'")
        li = logging.INFO

    ln = logging.getLevelName(li)
    log_format = Config.LOG_FORMAT
    logging.basicConfig(level=li, format=log_format)
    print(f"{__name__}.init_app() -> Logging initialized with level {ln}")
    log = logging.getLogger(__name__)

    ln_sqlalchemy = trim(xstr(Config.LOG_LEVEL_SQLALCHEMY))
    if ln_sqlalchemy is None:
        li_sqlalchemy = li
    else:
        li_sqlalchemy = parse_log_level(ln_sqlalchemy)
        if li_sqlalchemy is None:
            log.warning(f"sqlalchemy log level {ln_sqlalchemy} is invalid, defaulting to {ln}")
            li_sqlalchemy = li
    ln_sqlalchemy = logging.getLevelName(li_sqlalchemy)

    log.debug(f"sqlalchemy logging intialized with level {ln_sqlalchemy}")
    logging.getLogger('sqlalchemy.engine').setLevel(li_sqlalchemy)

    log.debug(f"{Config.__module__}.Config attributes:")
    for k, v in sorted(Config.attributes().items(), key=lambda x: x[0].casefold()):
        log.debug(f"  {k}={v}")
