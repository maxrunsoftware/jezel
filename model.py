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
        if value is None: return list()
        duplicate_check = set()
        new_list = list()
        for item in value:
            if item is None: continue
            if item in duplicate_check: continue
            duplicate_check.add(item)
            new_list.append(item)
        value.clear()
        value.extend(new_list)
    return _create_py_val(field, apply)


def _parse_list_config(field: str, check=False) -> classmethod:
    # TODO: clean config
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


def _orjson_dumps(v, *, default):
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode
    return orjson.dumps(v, default=default).decode()


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

class ModelMixinId:
    id: UUID = Field(default_factory=_factory_uuid)

class ModelMixinVer:
    ver: UUID = Field(default_factory=_factory_uuid)

class ModelMixinTimestamp:
    created_on: datetime
    created_by_user_id: UUID
    modified_on: datetime
    modified_by_user_id: UUID

class ModelMixinConfig:
    config_id: UUID


class ModelMixinIsActive:
    is_active: bool = True

class Tag(ModelBase):
    name: str
    _name: classmethod = _parse_str("name", trim=True, casefold=True)

    value: str
    _value: classmethod = _parse_str("value", trim=True, casefold=True)

    @classmethod
    def create(cls, name: str, value: str, skip_validation=False):
        if skip_validation:
            return cls.construct(name=name, value=value)
        else:
            return cls(name=name, value=value)

class ModelMixinTags:
    tags: List[Tag] = Field(default_factory=_factory_list)
    _tags: classmethod = _parse_list_tags("tags")

class User(ModelMixinId, ModelMixinVer, ModelMixinTimestamp, ModelMixinTags, ModelMixinIsActive, ModelBase):
    is_admin: bool = False
    is_system: bool = False

    email: str | None
    _email: classmethod = _parse_str("email", trim=True)

    username: str
    _username: classmethod = _parse_str("username", trim=True, casefold=True)

    password_hash: str
    _password_hash: classmethod = _parse_str("password_hash", trim=True)

    password_salt: str
    _password_salt: classmethod = _parse_str("password_salt", trim=True)

    @classmethod
    def create(cls, username: str, created_by_user_id: UUID, password_hash: str, password_salt: str):
        now = _factory_datetime()
        return cls(
            created_on=now, created_by_user_id=created_by_user_id,
            modified_on=now, modified_by_user_id=created_by_user_id,
            username=username,
            password_hash=password_hash, password_salt=password_salt,
        )




class Config(ModelMixinId, ModelMixinVer, ModelMixinTimestamp, ModelMixinTags, ModelBase):
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



class Task(ModelMixinId, ModelMixinVer, ModelMixinTimestamp, ModelMixinTags, ModelMixinConfig, ModelMixinIsActive, ModelBase):
    action: str
    _action: classmethod = _parse_str("name", trim=True, casefold=True)

    name: str | None = None
    _name: classmethod = _parse_str("name", trim=True)

    @classmethod
    def create(cls, action: str, created_by_user_id: UUID):
        now = _factory_datetime()
        return cls(
            created_on=now, created_by_user_id=created_by_user_id,
            modified_on=now, modified_by_user_id=created_by_user_id,
            action=action,
        )

class TaskConfig(ModelMixinId, ModelMixinConfig, ModelBase):
    task_id: UUID

    @classmethod
    def create(cls, config_id: UUID, task_id: UUID):
        return cls(
            config_id=config_id,
            task_id=task_id,
        )



class Schedule(ModelMixinIsActive, ModelBase):
    cron: str
    _cron: classmethod = _parse_str("cron", trim=True)

    @classmethod
    def create(cls, cron: str):
        return cls(
            cron=cron
        )


class Job(ModelMixinId, ModelMixinVer, ModelMixinTimestamp, ModelMixinTags, ModelMixinIsActive, ModelBase):
    name: str
    _name: classmethod = _parse_str("name", trim=True)

    schedules: List[Schedule] = Field(default_factory=_factory_list)
    _schedules: classmethod = _parse_list_schedule("schedules", check=True)

    @classmethod
    def create(cls, name: str, created_by_user_id: UUID):
        now = _factory_datetime()
        return cls(
            created_on=now, created_by_user_id=created_by_user_id,
            modified_on=now, modified_by_user_id=created_by_user_id,
            name=name,
        )

class JobConfig(ModelMixinId, ModelMixinConfig, ModelBase):
    job_id: UUID

    @classmethod
    def create(cls, config_id: UUID, job_id: UUID):
        return cls(
            config_id=config_id,
            job_id=job_id,
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


class TriggerEvent(ModelMixinId, ModelBase):
    job_id: UUID

    triggered_on: datetime
    triggered_schedule: Schedule | None = None
    triggered_by_user_id: UUID | None = None

    @classmethod
    def create(
            cls,
            job_id: UUID,
            triggered_schedule: Schedule | None = None,
            triggered_by_user_id: UUID | None = None
    ):
        if triggered_schedule is None and triggered_by_user_id is None:
            raise ValueError("values 'triggered_schedule' and 'triggered_by_user_id' cannot both be None")
        now = _factory_datetime()
        return cls(
            job_id=job_id,
            triggered_on=now,
            triggered_schedule=triggered_schedule,
            triggered_by_user_id=triggered_by_user_id,
        )


class CancellationEvent(ModelMixinId, ModelBase):
    execution_id: UUID

    cancelled_on: datetime
    cancelled_by_user_id: UUID

    @classmethod
    def create(
            cls,
            execution_id: UUID,
            cancelled_by_user_id: UUID,
    ):
        now = _factory_datetime()
        return cls(
            execution_id=execution_id,
            cancelled_on=now,
            cancelled_by_user_id=cancelled_by_user_id,
        )


class Execution(ModelMixinId, ModelMixinVer, ModelBase):
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
    def create(cls, job: Job, trigger_event: TriggerEvent, configs: List[Config]):
        return cls(
            job=job,
            trigger_event=trigger_event,
            state=ExecutionState.TRIGGERED,
            configs=configs,
        )


class ExecutionServer(ModelMixinId, ModelBase):
    started_on: datetime
    heartbeat_on: datetime

    @classmethod
    def create(cls):
        now = _factory_datetime()
        return cls(
            started_on=now,
            heartbeat_on=now,
        )

class ExecutionServerThread(ModelMixinId, ModelBase):
    execution_server_id: UUID
    started_on: datetime
    heartbeat_on: datetime
    execution_id: UUID | None

    @classmethod
    def create(cls, server_id: UUID):
        now = _factory_datetime()
        return cls(
            server_id=server_id,
            started_on=now,
            heartbeat_on=now,
        )



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
