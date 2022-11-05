from datetime import datetime
from enum import Enum
from typing import Callable, Dict, List, Set
from uuid import UUID, uuid4

import pydantic
from orjson import orjson
from pydantic import BaseModel, Field, validate_model

import utils
from utils import datetime_now_utc

_factory_list = list
_factory_datetime = datetime_now_utc
_factory_uuid = uuid4


def _orjson_dumps(v, *, default):
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode
    return orjson.dumps(v, default=default).decode()


def _create_py_val(field: str, func: Callable):
    decorator = pydantic.validator(field, pre=True, allow_reuse=True)
    validator = decorator(func)
    return validator



def _parse_str(field: str, trim=False, casefold=False) -> classmethod:
    # https://github.com/pydantic/pydantic/issues/940#issuecomment-569765091
    def apply(value: str):
        if value is not None and trim: value = utils.trim(value)
        if value is not None and casefold: value = value.casefold()
        # if value is None and check: raise ValueError("value cannot be empty or None" if trim else "value cannot be None")
        return value

    return _create_py_val(field, apply)


def _parse_dict(field: str, trim=(False, False), casefold=(False, False)) -> classmethod:
    # https://github.com/pydantic/pydantic/issues/940#issuecomment-569765091
    def apply(value: Dict[str, str]):
        if value is None: return dict()
        d = dict()
        for k, v in value.items():
            if k is not None and trim[0]: k = trim(k)
            if k is not None and casefold[0]: k = k.casefold()
            if k is None: continue
            if v is not None and trim[1]: v = trim(v)
            if v is not None and casefold[1]: v = v.casefold()
            if v is None: continue
            d[k] = v
        value.clear()
        value.update(d)
        return value
    return _create_py_val(field, apply)


def _parse_list(field: str, check=False) -> classmethod:
    # https://github.com/pydantic/pydantic/issues/940#issuecomment-569765091
    def apply(value: List):
        if value is None: return list()
        while None in value:
            value.remove(None)
        if check:
            for item in value:
                if isinstance(item, ModelBase):
                    item.check()
        return value
    return _create_py_val(field, apply)


def _parse_list_tags(field: str) -> classmethod:
    # https://github.com/pydantic/pydantic/issues/940#issuecomment-569765091
    def apply(value: List):
        if value is None: return list()
        d: Dict[str, Set[str]] = dict()
        for tag in value:
            if tag is None: continue
            k, v = utils.trim_casefold(tag.name), utils.trim_casefold(tag.value)
            if k is None or v is None: continue
            if k not in d: d[k] = set()
            d[k].add(v)
        value.clear()
        for k in sorted(d.keys()):
            for v in sorted(d[k]):
                value.append(Tag.create(k, v, skip_validation=True))
        return value
    return _create_py_val(field, apply)


def _parse_list_uuid(field: str) -> classmethod:
    # https://github.com/pydantic/pydantic/issues/940#issuecomment-569765091
    def apply(value: List):
        return list() if value is None else [x for x in sorted({item for item in value if item is not None})]
    return _create_py_val(field, apply)


def _parse_list_configitem(field: str, check=False) -> classmethod:
    # TODO: clean config_items
    return _parse_list(field=field, check=check)


def _parse_list_schedule(field: str, check=False) -> classmethod:
    # TODO: clean schedules
    return _parse_list(field=field, check=check)


def _check_obj(field: str) -> classmethod:
    # https://github.com/pydantic/pydantic/issues/940#issuecomment-569765091
    def apply(value: ModelBase):
        if value is not None:
            value.check()
        return value

    return _create_py_val(field, apply)


class ModelBase(BaseModel):
    def check(self):
        # https://github.com/pydantic/pydantic/issues/1864
        values, fields_set, validation_error = validate_model(self.__class__, self.__dict__)
        if validation_error: raise validation_error
        try:
            object.__setattr__(self, "__dict__", values)
        except TypeError as e:
            raise TypeError("Model values must be a dict; you may not have returned a dictionary from a root validator") from e
        object.__setattr__(self, "__fields_set__", fields_set)

    def json_pretty(self):
        # return json2str(self.json(by_alias=True), indent=2)
        return str(orjson.dumps(orjson.loads(self.json(by_alias=True)), option=orjson.OPT_INDENT_2), 'utf-8')

    class Config:
        json_loads = orjson.loads
        json_dumps = _orjson_dumps
        extra = "allow"
        alias_generator = utils.str2camel
        allow_population_by_field_name = True
        # anystr_strip_whitespace = True


class Tag(ModelBase):
    name: str
    _name: classmethod = _parse_str("name", trim=True, casefold=True)

    value: str
    _value: classmethod = _parse_str("value", trim=True, casefold=True)

    @classmethod
    def create(cls, name: str, value: str, skip_validation=False):
        if skip_validation:
            field_data = {"name": name, "value": value}
            return cls.construct(**field_data)
        else:
            return cls(name=name, value=value)


class User(ModelBase):
    id: UUID = Field(default_factory=_factory_uuid)
    ver: UUID = Field(default_factory=_factory_uuid)
    created_on: datetime
    created_by_user_id: UUID
    modified_on: datetime
    modified_by_user_id: UUID

    is_admin: bool = False
    is_active: bool = True

    email: str | None
    _email: classmethod = _parse_str("email", trim=True)

    username: str
    _username: classmethod = _parse_str("username", trim=True, casefold=True)

    password_hash: str
    _password_hash: classmethod = _parse_str("password_hash", trim=True)

    password_salt: str
    _password_salt: classmethod = _parse_str("password_salt", trim=True)

    tags: List[Tag] = Field(default_factory=_factory_list)
    _tags: classmethod = _parse_list_tags("tags")

    @classmethod
    def create(cls, username: str, created_by: UUID, password_hash: str, password_salt: str):
        now = _factory_datetime()
        return cls(
            created_on=now, created_by=created_by,
            modified_on=now, modified_by=created_by,
            username=username,
            password_hash=password_hash, password_salt=password_salt,
        )



class ConfigItem(ModelBase):
    id: UUID = Field(default_factory=_factory_uuid)
    ver: UUID = Field(default_factory=_factory_uuid)
    created_on: datetime
    created_by_user_id: UUID
    modified_on: datetime
    modified_by_user_id: UUID

    name: str
    _name: classmethod = _parse_str("name", trim=True, casefold=True)

    value: str

    @classmethod
    def create(cls, created_by: UUID, name: str, value: str):
        now = _factory_datetime()
        return cls(
            created_on=now, created_by=created_by,
            modified_on=now, modified_by=created_by,
            name=name,
            value=value,
        )



class Task(ModelBase):
    id: UUID = Field(default_factory=_factory_uuid)
    ver: UUID = Field(default_factory=_factory_uuid)
    created_on: datetime
    created_by_user_id: UUID
    modified_on: datetime
    modified_by_user_id: UUID

    is_active: bool = True

    step: int = -1

    action: str
    _action: classmethod = _parse_str("name", trim=True, casefold=True)

    name: str | None = None
    _name: classmethod = _parse_str("name", trim=True)

    config_ids: List[UUID] = Field(default_factory=_factory_list)
    _config_ids: classmethod = _parse_list_uuid("config_ids")

    @classmethod
    def create(cls, action: str, created_by: str):
        now = datetime_now_utc()
        return cls(
            created_on=now, created_by=created_by,
            modified_on=now, modified_by=created_by,
            action=action,
        )


class Schedule(ModelBase):
    cron: str
    _cron: classmethod = _parse_str("cron", trim=True)

    is_active: bool = True


class Job(ModelBase):
    id: UUID = Field(default_factory=_factory_uuid)
    ver: UUID = Field(default_factory=_factory_uuid)
    created_on: datetime
    created_by_user_id: UUID
    modified_on: datetime
    modified_by_user_id: UUID

    tags: List[Tag] = Field(default_factory=_factory_list)
    _tags: classmethod = _parse_list_tags("tags")

    is_active: bool = True

    name: str
    _name: classmethod = _parse_str("name", trim=True)

    schedules: List[Schedule] = Field(default_factory=_factory_list)
    _schedules: classmethod = _parse_list_schedule("schedules", check=True)

    config_ids: List[UUID] = Field(default_factory=_factory_list)
    _config_ids: classmethod = _parse_list_uuid("config_ids")

    config_items: List[ConfigItem] = Field(default_factory=_factory_list)
    _config_items: classmethod = _parse_list_configitem("config_items", check=True)

    @classmethod
    def create(cls, name: str, created_by: str):
        now = datetime_now_utc()
        return cls(
            created_on=now, created_by=created_by,
            modified_on=now, modified_by=created_by,
            name=name,
        )


class ExecutionState(str, Enum):
    TRIGGERED = "triggered"
    QUEUED = "queued"
    STARTED = "started"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    ERROR = "error"

    def __str__(self): return self.value


class ExecutionErrorType(str, Enum):
    VALIDATION = "validation"
    TASK = "task"
    OTHER = "other"

    def __str__(self): return self.value


class TriggerEvent(ModelBase):
    id: UUID = Field(default_factory=_factory_uuid)
    job_id: UUID

    triggered_on: datetime
    triggered_schedule: Schedule | None = None
    triggered_by_user_id: UUID | None = None


class CancellationEvent(ModelBase):
    id: UUID = Field(default_factory=_factory_uuid)
    execution_id: UUID

    cancelled_on: datetime | None
    cancelled_by_user_id: UUID | None = None


class Execution(ModelBase):
    id: UUID = Field(default_factory=_factory_uuid)
    ver: UUID = Field(default_factory=_factory_uuid)

    state: ExecutionState
    state_task_id: UUID | None = None

    trigger_event: TriggerEvent
    _trigger_event: classmethod = _check_obj("trigger_event")

    started_on: datetime | None = None
    completed_on: datetime | None = None

    cancellation_event: CancellationEvent | None = None
    _cancellation_event: classmethod = _check_obj("cancellation_event")

    error_type: ExecutionErrorType | None = None
    error_str: str | None = None
    _error_str: classmethod = _parse_str("error_str", trim=True)

    job: Job

    configs: List[Config] = Field(default_factory=_factory_list)
    _configs: classmethod = _parse_list("configs")

    thread_id: UUID | None = None

    @classmethod
    def create(cls, job: Job, trigger_event: TriggerEvent, ):
        return cls(
            job=job,
            trigger_event=trigger_event,
            state=ExecutionState.TRIGGERED,
        )


class Server(ModelBase):
    id: UUID = Field(default_factory=_factory_uuid)
    started_on: datetime
    heartbeat_on: datetime


class Thread(ModelBase):
    id: UUID = Field(default_factory=_factory_uuid)
    server_id: UUID
    started_on: datetime
    heartbeat_on: datetime
    execution_id: UUID | None


def main():
    j1 = Job.create("Job1", "Steve")
    j1.schedules.append(Schedule(cron="111"))
    j1.schedules.append(Schedule(cron="222"))
    j1s = j1.json(by_alias=True)
    print(j1s)
    print(j1.json_pretty())

    j2 = Job.parse_raw(j1s)
    j2.check()

    # print(json2str(j1s, indent=2))

    # j1.schedules[0].cron = None
    # j11 = j1.copy(deep=True)
    # print(j11.json_pretty())
    # j1.check()


if __name__ == "__main__":
    main()
