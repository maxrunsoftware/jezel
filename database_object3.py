import dataclasses
import inspect
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, NamedTuple, Type, TypeVar
from uuid import UUID, uuid4

from orjson import orjson

import utils
from utils import datetime_now_utc

T = TypeVar('T', UUID, float, int, str, bool)

_factory_list = list
_factory_datetime = datetime_now_utc
_factory_uuid = uuid4

_classes_str2cls: Dict[str, type] = dict()
_classes_cls2str: Dict[type, str] = dict()
_field_names_camel2snake: Dict[type, Dict[str, str]] = dict()
_field_names_snake2camel: Dict[type, Dict[str, str]] = dict()




def _orjson_dumps(v, *, default):
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode
    return orjson.dumps(v, default=default).decode()


class DatabaseRow(NamedTuple):
    id: UUID
    ver: int
    parent_id: UUID | None
    type: str
    data: str


class SerializerWriter(ABC):

    @classmethod
    @abstractmethod
    def create_writer(cls, type: type):
        raise NotImplementedError

    @abstractmethod
    def export_row(self, name: str, value: str | None) -> DatabaseRow:
        raise NotImplementedError

    @abstractmethod
    def set_id(self, value: UUID):
        raise NotImplementedError

    @abstractmethod
    def set_parent_id_optional(self, value: UUID | None):
        raise NotImplementedError

    def set_parent(self, value: UUID):
        if value is None: raise ValueError("parent_id cannot be None")
        self.set_parent_id_optional(value)

    @abstractmethod
    def set_uuid_optional(self, name: str, value: UUID | None):
        raise NotImplementedError

    def set_uuid(self, name: str, value: UUID | None):
        if value is None: raise ValueError(f"{name} cannot be None")
        self.set_uuid_optional(name, value)

    @abstractmethod
    def set_int_optional(self, name: str, value: int | None):
        raise NotImplementedError

    def set_int(self, name: str, value: int | None):
        if value is None: raise ValueError(f"{name} cannot be None")
        self.set_int_optional(name, value)

    @abstractmethod
    def set_float_optional(self, name: str, value: float | None):
        raise NotImplementedError

    def set_float(self, name: str, value: float | None):
        if value is None: raise ValueError(f"{name} cannot be None")
        self.set_float_optional(name, value)

    @abstractmethod
    def set_bool_optional(self, name: str, value: bool | None):
        raise NotImplementedError

    def set_bool(self, name: str, value: bool | None):
        if value is None: raise ValueError(f"{name} cannot be None")
        self.set_bool_optional(name, value)

    @abstractmethod
    def set_str_optional(self, name: str, value: str | None):
        raise NotImplementedError

    def set_str(self, name: str, value: str | None):
        if value is None: raise ValueError(f"{name} cannot be None")
        self.set_str_optional(name, value)

    @abstractmethod
    def set_datetime_optional(self, name: str, value: datetime | None):
        raise NotImplementedError

    def set_datetime(self, name: str, value: datetime | None):
        if value is None: raise ValueError(f"{name} cannot be None")
        self.set_datetime_optional(name, value)


class SerializerWriterJson(SerializerWriter):
    __slots__ = {
        "_v_type": "The class being written for",
        "_v_id": "The UUID id value",
        "_v_parent_id": "The Optional[UUID] parent id value",
        "_v_data": "The Dict[str, Any | None] containing the data",
    }

    _v_type: type
    _v_id: UUID
    _v_parent_id: UUID | None
    _v_data: Dict[str, Any | None]

    @classmethod
    def create_writer(cls, type: type):
        o = cls()
        o._v_type = type
        o._v_id = None
        o._v_parent_id = None
        o._v_data = dict()
        return o

    def export_row(self, name: str, value: str | None) -> DatabaseRow:
        if self._v_id is None: raise ValueError("id not assigned")
        # data = orjson.dumps(self.v_data, default=default).decode()
        data = orjson.dumps(self._v_data, option=orjson.OPT_NAIVE_UTC).decode()
        return DatabaseRow(self._v_id, 1, self._v_parent_id, _classes_cls2str[self._v_type], data)

    def set_id(self, value: UUID):
        self._v_id = value

    def set_parent_id_optional(self, value: UUID | None):
        self._v_parent_id = value

    def set_uuid_optional(self, name: str, value: UUID | None):
        if value is None: return  # do not write None
        self._v_data[_field_names_snake2camel[self._v_type][name]] = value

    def set_int_optional(self, name: str, value: int | None):
        if value is None: return  # do not write None
        self._v_data[_field_names_snake2camel[self._v_type][name]] = value

    def set_float_optional(self, name: str, value: float | None):
        if value is None: return  # do not write None
        self._v_data[_field_names_snake2camel[self._v_type][name]] = value

    def set_bool_optional(self, name: str, value: bool | None):
        if value is None: return  # do not write None
        self._v_data[_field_names_snake2camel[self._v_type][name]] = value

    def set_str_optional(self, name: str, value: str | None):
        if value is None: return  # do not write None
        self._v_data[_field_names_snake2camel[self._v_type][name]] = value

    def set_datetime_optional(self, name: str, value: datetime | None):
        if value is None: return  # do not write None
        self._v_data[_field_names_snake2camel[self._v_type][name]] = value


class SerializerReader(ABC):
    @classmethod
    @abstractmethod
    def create_reader(cls, row: DatabaseRow):
        raise NotImplementedError

    @abstractmethod
    def get_id(self) -> UUID:
        raise NotImplementedError

    @abstractmethod
    def get_parent_id_optional(self) -> UUID | None:
        raise NotImplementedError

    def get_parent_id(self) -> UUID:
        v = self.get_parent_id_optional()
        if v is None: raise ValueError("parent_id cannot be None")
        return v

    @abstractmethod
    def get_uuid_optional(self, name: str) -> UUID | None:
        raise NotImplementedError

    def get_uuid(self, name: str) -> UUID:
        v = self.get_uuid_optional(name)
        if v is None: raise ValueError(f"{name} cannot be None")
        return v

    @abstractmethod
    def get_int_optional(self, name: str) -> int | None:
        raise NotImplementedError

    def get_int(self, name: str) -> int:
        v = self.get_int_optional(name)
        if v is None: raise ValueError(f"{name} cannot be None")
        return v

    @abstractmethod
    def get_float_optional(self, name: str) -> float | None:
        raise NotImplementedError

    def get_float(self, name: str) -> float:
        v = self.get_float_optional(name)
        if v is None: raise ValueError(f"{name} cannot be None")
        return v

    @abstractmethod
    def get_bool_optional(self, name: str) -> bool | None:
        raise NotImplementedError

    def get_bool(self, name: str) -> bool:
        v = self.get_bool_optional(name)
        if v is None: raise ValueError(f"{name} cannot be None")
        return v

    @abstractmethod
    def get_str_optional(self, name: str) -> str | None:
        raise NotImplementedError

    def get_str(self, name: str) -> str:
        v = self.get_str_optional(name)
        if v is None: raise ValueError(f"{name} cannot be None")
        return v

    @abstractmethod
    def get_datetime_optional(self, name: str) -> datetime | None:
        raise NotImplementedError

    def get_datetime(self, name: str) -> datetime:
        v = self.get_datetime_optional(name)
        if v is None: raise ValueError(f"{name} cannot be None")
        return v


class SerializerReaderJson(SerializerReader):
    __slots__ = {
        "_v_type": "The class being read for",
        "_v_id": "The UUID id value",
        "_v_parent_id": "The Optional[UUID] parent id value",
        "_v_data": "The Dict[str, Any | None] containing the data",
    }

    _v_type: type
    _v_id: UUID
    _v_parent_id: UUID | None
    _v_data: Dict[str, Any | None]

    @classmethod
    def create_reader(cls, row: DatabaseRow):
        o = cls()
        o._v_type = _classes_str2cls[row.type]
        o._v_id = row.id
        o._v_parent_id = row.parent_id
        o._v_data = orjson.loads(row.data)
        return o

    def get_id(self) -> UUID:
        return self._v_id

    def get_parent_id_optional(self) -> UUID | None:
        return self._v_parent_id

    def _get_value(self, name: str):
        v = self._v_data.get(_field_names_camel2snake[self._v_type][name])
        if v is not None: return v
        return self._v_data.get(name)  # just in case was written snake rather then camel

    def _get_value_parsed(self, name: str, type: type, type_factory: Callable = None, expected_len=None):
        v = self._get_value(name)
        if v is None: return None
        if isinstance(v, type): return v
        if not isinstance(v, str): v = str(v)
        if v is None: return None
        if isinstance(v, type): return v  # for str type
        if len(v) == 0: return None
        if type_factory is None: type_factory = type
        if expected_len is not None and len(v) == expected_len: return type_factory(v)  # for UUID type
        v = utils.trim(v)
        if v is None: return None
        return type_factory(v)

    def get_uuid_optional(self, name: str) -> UUID | None:
        return self._get_value_parsed(name, UUID, expected_len=36)

    def get_int_optional(self, name: str) -> int | None:
        return self._get_value_parsed(name, int)

    def get_float_optional(self, name: str) -> float | None:
        return self._get_value_parsed(name, float)

    def get_bool_optional(self, name: str) -> bool | None:
        return self._get_value_parsed(name, bool, type_factory=utils.bool_parse_none)

    def get_str_optional(self, name: str) -> str | None:
        return self._get_value_parsed(name, str)

    def get_datetime_optional(self, name: str) -> datetime | None:
        return self._get_value_parsed(name, datetime, type_factory=utils.datetime_parse_none)


serialization_reader: Type[SerializerReader] = SerializerReaderJson
serialization_writer: Type[SerializerWriter] = SerializerWriterJson


@dataclass(slots=True)
class DatabaseObject:
    id: UUID





@dataclass(slots=True)
class System(DatabaseObject):
    name: str

    @classmethod
    def create(cls, name: str):
        return cls(
            id=_factory_uuid(),
            name=name,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            name=r.get_str("name"),
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.id)
        w.set_str("name", self.name)


SYSTEM_CONFIG_ITEM_APP_SYSTEM = "SYSTEM".casefold()
SYSTEM_CONFIG_ITEM_APP_WEB = "WEB".casefold()
SYSTEM_CONFIG_ITEM_APP_SCHEDULER = "SCHEDULER".casefold()
SYSTEM_CONFIG_ITEM_APP_EXECUTOR = "EXECUTOR".casefold()


@dataclass(slots=True)
class SystemConfigItem(DatabaseObject):
    system_id: UUID
    app: str
    name: str
    value: str | None

    @classmethod
    def create(cls, system_id: UUID, app: str, name: str, value: str):
        return cls(
            id=_factory_uuid(),
            system_id=system_id,
            app=app,
            name=name,
            value=value,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            system_id=r.get_parent_id(),
            app=r.get_str("app"),
            name=r.get_str("name"),
            value=r.get_str_optional("value"),
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.system_id)
        w.set_str("app", self.app)
        w.set_str("name", self.name)
        w.set_str_optional("value", self.value)


@dataclass(slots=True)
class SystemActionConfigItem(DatabaseObject):
    system_id: UUID
    action: str
    name: str
    value: str

    @classmethod
    def create(cls, system_id: UUID, action: str, name: str, value: str):
        return cls(
            id=_factory_uuid(),
            system_id=system_id,
            action=action,
            name=name,
            value=value,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            system_id=r.get_parent_id(),
            action=r.get_str("action"),
            name=r.get_str("name"),
            value=r.get_str("value"),
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.system_id)
        w.set_str("action", self.action)
        w.set_str("name", self.name)
        w.set_str("value", self.value)


def dostuff():
    dcs = utils.dataclass_infos_in_module(__name__)
    print(f"Found {len(dcs)} dataclasses")
    #raise ValueError(f"{_f.name} has invalid types")

    for dc in dcs:
        for f in dc.fields.values():
            if len(f.type.subtypes) > 0: raise ValueError(f"{dc.name}.{f.name} has invalid types")

@dataclass(slots=True)
class User(DatabaseObject):
    system_id: UUID
    is_active: bool
    username: str
    password: str
    is_admin: bool
    email: str | None

    @classmethod
    def create(cls, system_id: UUID, username: str, password: str):
        return cls(
            id=_factory_uuid(),
            system_id=system_id,
            is_active=True,
            username=username,
            password=password,
            is_admin=False,
            email=None,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            system_id=r.get_parent_id(),
            is_active=r.get_bool("is_active"),
            username=r.get_str("username"),
            password=r.get_str("password"),
            is_admin=r.get_bool("is_admin"),
            email=r.get_str_optional("email"),
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.system_id)
        w.set_bool("is_active", self.is_active)
        w.set_str("username", self.username)
        w.set_str("password", self.password)
        w.set_bool("is_admin", self.is_admin)
        w.set_str_optional("email", self.email)


@dataclass(slots=True)
class Job(DatabaseObject):
    system_id: UUID
    is_active: bool
    name: str

    @classmethod
    def create(cls, system_id: UUID, name: str):
        return cls(
            id=_factory_uuid(),
            system_id=system_id,
            is_active=True,
            name=name,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            system_id=r.get_parent_id(),
            is_active=r.get_bool("is_active"),
            name=r.get_str("name"),
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.system_id)
        w.set_bool("is_active", self.is_active)
        w.set_str("name", self.name)


@dataclass(slots=True)
class JobTag(DatabaseObject):
    job_id: UUID
    name: str
    value: str

    @classmethod
    def create(cls, job_id: UUID, name: str, value: str):
        return cls(
            id=_factory_uuid(),
            job_id=job_id,
            name=name,
            value=value,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            job_id=r.get_parent_id(),
            name=r.get_str("name"),
            value=r.get_str("value"),
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.job_id)
        w.set_str("name", self.name)
        w.set_str("value", self.value)


@dataclass(slots=True)
class JobSchedule(DatabaseObject):
    job_id: UUID
    is_active: bool
    cron: str

    @classmethod
    def create(cls, job_id: UUID, cron: str):
        return cls(
            id=_factory_uuid(),
            job_id=job_id,
            is_active=True,
            cron=cron,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            job_id=r.get_parent_id(),
            is_active=r.get_bool("is_active"),
            cron=r.get_str("cron"),
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.job_id)
        w.set_bool("is_active", self.is_active)
        w.set_str("cron", self.cron)


@dataclass(slots=True)
class JobActionConfigItem(DatabaseObject):
    job_id: UUID
    action: str
    name: str
    value: str | None

    @classmethod
    def create(cls, job_id: UUID, action: str, name: str, value: str | None):
        return cls(
            id=_factory_uuid(),
            job_id=job_id,
            action=action,
            name=name,
            value=value,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            job_id=r.get_parent_id(),
            action=r.get_str("action"),
            name=r.get_str("name"),
            value=r.get_str_optional("value"),
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.job_id)
        w.set_str("action", self.action)
        w.set_str("name", self.name)
        w.set_str_optional("value", self.value)


@dataclass(slots=True)
class Task(DatabaseObject):
    job_id: UUID
    is_active: bool
    step: int
    action: str

    @classmethod
    def create(cls, job_id: UUID, action: str):
        return cls(
            id=_factory_uuid(),
            job_id=job_id,
            is_active=True,
            step=-1,
            action=action,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            job_id=r.get_parent_id(),
            is_active=r.get_bool("is_active"),
            step=r.get_int("step"),
            action=r.get_str("action"),
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.job_id)
        w.set_bool("is_active", self.is_active)
        w.set_int("step", self.step)
        w.set_str("action", self.action)


@dataclass(slots=True)
class TaskActionConfigItem(DatabaseObject):
    task_id: UUID
    action: str
    name: str
    value: str | None

    @classmethod
    def create(cls, task_id: UUID, action: str, name: str, value: str | None):
        return cls(
            id=_factory_uuid(),
            task_id=task_id,
            action=action,
            name=name,
            value=value,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            task_id=r.get_parent_id(),
            action=r.get_str("action"),
            name=r.get_str("name"),
            value=r.get_str_optional("value"),
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.task_id)
        w.set_str("action", self.action)
        w.set_str("name", self.name)
        w.set_str_optional("value", self.value)


@dataclass(slots=True)
class TriggerEvent(DatabaseObject):
    job_id: UUID
    triggered_on: datetime
    triggered_by_job_schedule_id: UUID | None
    triggered_by_user_id: UUID | None

    @classmethod
    def create(cls, job_id: UUID, triggered_by_job_schedule_id: UUID | None = None, triggered_by_user_id: UUID | None = None):
        if triggered_by_job_schedule_id is None and triggered_by_user_id is None:
            raise ValueError("values 'triggered_by_job_schedule_id' and 'triggered_by_user_id' cannot both be None")
        now = _factory_datetime()
        return cls(
            id=_factory_uuid(),
            job_id=job_id,
            triggered_on=now,
            triggered_by_job_schedule_id=triggered_by_job_schedule_id,
            triggered_by_user_id=triggered_by_user_id,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        o = cls(
            id=r.get_id(),
            job_id=r.get_parent_id(),
            triggered_on=r.get_datetime("triggered_on"),
            triggered_by_job_schedule_id=r.get_uuid_optional("triggered_by_job_schedule_id"),
            triggered_by_user_id=r.get_uuid_optional("triggered_by_user_id"),
        )
        if o.triggered_by_job_schedule_id is None and o.triggered_by_user_id is None:
            raise ValueError("values 'triggered_by_job_schedule_id' and 'triggered_by_user_id' cannot both be None")
        return o

    def serialize(self, w: SerializerWriter):
        if self.triggered_by_job_schedule_id is None and self.triggered_by_user_id is None:
            raise ValueError("values 'triggered_by_job_schedule_id' and 'triggered_by_user_id' cannot both be None")
        w.set_id(self.id)
        w.set_parent(self.job_id)
        w.set_datetime("triggered_on", self.triggered_on)
        w.set_uuid_optional("triggered_by_job_schedule_id", self.triggered_by_job_schedule_id)
        w.set_uuid_optional("triggered_by_user_id", self.triggered_by_user_id)


@dataclass(slots=True)
class CancellationEvent(DatabaseObject):
    execution_id: UUID
    cancelled_on: datetime
    cancelled_by_user_id: UUID

    @classmethod
    def create(cls, execution_id: UUID, cancelled_by_user_id: UUID):
        now = _factory_datetime()
        return cls(
            id=_factory_uuid(),
            execution_id=execution_id,
            cancelled_on=now,
            cancelled_by_user_id=cancelled_by_user_id,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            execution_id=r.get_parent_id(),
            cancelled_on=r.get_datetime("cancelled_on"),
            cancelled_by_user_id=r.get_uuid("cancelled_by_user_id"),
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.execution_id)
        w.set_datetime("cancelled_on", self.cancelled_on)
        w.set_uuid("cancelled_by_user_id", self.cancelled_by_user_id)


EXECUTION_STATE_TRIGGERED = "TRIGGERED".casefold()
EXECUTION_STATE_QUEUED = "QUEUED".casefold()
EXECUTION_STATE_STARTED = "STARTED".casefold()
EXECUTION_STATE_COMPLETED = "COMPLETED".casefold()
EXECUTION_STATE_CANCELLED = "CANCELLED".casefold()
EXECUTION_STATE_ERROR = "ERROR".casefold()


@dataclass(slots=True)
class Execution(DatabaseObject):
    system_id: UUID
    trigger_event_id: UUID
    state: str
    executing_task_id: UUID | None
    started_on: datetime | None
    completed_on: datetime | None
    cancellation_event_id: UUID | None
    error_serialized: str | None
    job_serialized: str
    execution_server_thread_id: UUID | None = None

    @classmethod
    def create(cls, system_id: UUID, trigger_event_id: UUID, job_serialized: str):
        return cls(
            id=_factory_uuid(),
            system_id=system_id,
            trigger_event_id=trigger_event_id,
            state=EXECUTION_STATE_TRIGGERED,
            executing_task_id=None,
            started_on=None,
            completed_on=None,
            cancellation_event_id=None,
            error_serialized=None,
            job_serialized=job_serialized,
            execution_server_thread_id=None,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            system_id=r.get_parent_id(),
            trigger_event_id=r.get_uuid("trigger_event_id"),
            state=r.get_str("state"),
            executing_task_id=r.get_uuid_optional("executing_task_id"),
            started_on=r.get_datetime_optional("started_on"),
            completed_on=r.get_datetime_optional("completed_on"),
            cancellation_event_id=r.get_uuid_optional("cancellation_event_id"),
            error_serialized=r.get_str_optional("error_serialized"),
            job_serialized=r.get_str_optional("job_serialized"),
            execution_server_thread_id=r.get_uuid_optional("execution_server_thread_id")
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.system_id)
        w.set_uuid_optional("trigger_event_id", self.trigger_event_id)
        w.set_str("state", self.state)
        w.set_uuid_optional("executing_task_id", self.executing_task_id)
        w.set_datetime_optional("started_on", self.started_on)
        w.set_datetime_optional("completed_on", self.completed_on)
        w.set_uuid_optional("cancellation_event_id", self.cancellation_event_id)
        w.set_str_optional("error_serialized", self.error_serialized)
        w.set_str_optional("job_serialized", self.job_serialized)
        w.set_uuid_optional("execution_server_thread_id", self.execution_server_thread_id)


@dataclass(slots=True)
class ExecutionServer(DatabaseObject):
    system_id: UUID
    started_on: datetime
    heartbeat_on: datetime

    @classmethod
    def create(cls, system_id: UUID):
        now = _factory_datetime()
        return cls(
            id=_factory_uuid(),
            system_id=system_id,
            started_on=now,
            heartbeat_on=now,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            system_id=r.get_parent_id(),
            started_on=r.get_datetime("started_on"),
            heartbeat_on=r.get_datetime("heartbeat_on"),
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.system_id)
        w.set_datetime("started_on", self.started_on)
        w.set_datetime("heartbeat_on", self.heartbeat_on)


@dataclass(slots=True)
class ExecutionServerThread(DatabaseObject):
    execution_server_id: UUID
    started_on: datetime
    heartbeat_on: datetime
    execution_id: UUID | None

    @classmethod
    def create(cls, execution_server_id: UUID):
        now = _factory_datetime()
        return cls(
            id=_factory_uuid(),
            execution_server_id=execution_server_id,
            started_on=now,
            heartbeat_on=now,
            execution_id=None,
        )

    @classmethod
    def deserialize(cls, r: SerializerReader):
        return cls(
            id=r.get_id(),
            execution_server_id=r.get_parent_id(),
            started_on=r.get_datetime("started_on"),
            heartbeat_on=r.get_datetime("heartbeat_on"),
            execution_id=r.get_uuid_optional("execution_id"),
        )

    def serialize(self, w: SerializerWriter):
        w.set_id(self.id)
        w.set_parent(self.execution_server_id)
        w.set_datetime("started_on", self.started_on)
        w.set_datetime("heartbeat_on", self.heartbeat_on)
        w.set_uuid_optional("execution_id", self.execution_id)


def _field_names_populate():
    _classes_str2cls.clear()
    _classes_cls2str.clear()
    _field_names_camel2snake.clear()
    _field_names_snake2camel.clear()

    def is_dataclass_item(member):
        if member is None: return False
        if not inspect.isclass(member): return False
        if member.__module__ != __name__: return False
        if "id" not in dir(member): return False

        def contains_dataclass(member_names):
            for member_name in member_names:
                if "_dataclass_".casefold() in member_name.casefold():
                    return True
            return False

        if not contains_dataclass(dir(member)): return False
        return True

    for tpl in inspect.getmembers(sys.modules[__name__], is_dataclass_item):
        cls = tpl[1]
        _classes_str2cls[cls.__name__] = cls
        _classes_cls2str[cls] = cls.__name__

        for f in dataclasses.fields(cls):
            snake_name = f.name
            camel_name = utils.str2camel(snake_name)
            ds = _field_names_snake2camel.get(cls)
            if ds is None:
                ds = dict()
                _field_names_snake2camel[cls] = ds
            ds[snake_name] = camel_name

            dc = _field_names_camel2snake.get(cls)
            if dc is None:
                dc = dict()
                _field_names_camel2snake[cls] = dc
            dc[camel_name] = snake_name

_field_names_populate()




def main():
    dostuff()


if __name__ == "__main__":
    main()
