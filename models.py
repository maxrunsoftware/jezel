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
import time
from dataclasses import field

from database import Database, get_database
from utils import *

log = logging.getLogger(__name__)


def _json_get(json: Mapping[str, Any | None], key: str) -> Any | None:
    v = json.get(key)
    if v is not None: return v

    key = trim(key)
    if key is None:
        raise ValueError("Key cannot be empty")
    v = json.get(key)
    if v is not None: return v

    key_camel = str2camel(key)
    v = json.get(key_camel)
    if v is not None: return v

    key_snake = str2snake(key)
    v = json.get(key_snake)
    if v is not None: return v

    key_casefold = key.casefold()
    v = json.get(key_casefold)
    if v is not None: return v

    return None


def _json_get_str(json: Mapping[str, Any | None], key: str, is_trimmed=True) -> str | None:
    v = _json_get(json, key)
    if v is None: return None
    v = str(v)
    if is_trimmed: v = trim(v)
    return v


def _json_get_by_type(json: Mapping[str, Any | None], key: str, typ: type) -> Any | None:
    if typ == str:
        return _json_get_str(json, key)
    elif typ == datetime:
        return datetime_parse_none(_json_get_str(json, key), tz=timezone.utc)
    elif typ == int:
        return int_parse_none(_json_get_str(json, key))
    elif typ == float:
        return float_parse_none(_json_get_str(json, key))
    elif typ == bool:
        return bool_parse_none(_json_get_str(json, key))
    elif typ == UUID:
        return uuid_parse_none(_json_get_str(json, key))
    elif typ == list or typ == List:
        v = _json_get(json, key)
        result = []
        if v is None: return result
        for item in v:
            if item is not None:
                result.append(item)
        return result

    elif typ == dict or typ == Mapping or typ == MutableMapping:
        v = _json_get(json, key)
        result = dict()
        if v is None: return result
        for key, value in v.items():
            key = trim(xstr(key))
            if key is not None and value is not None: result[key] = value
        return result

    elif issubclass(typ, Enum):
        v = _json_get_str(json, key)
        if v is None: return None
        return typ(v)

    elif issubclass(typ, JsonItemBase):
        v = _json_get(json, key)
        if v is None: return None
        o = typ()
        o.json_import(v)
        return o

    else:
        raise NotImplementedError(f"Type '{typ.__name__}' is not implemented")


def _json_get_assign_attributes(json: Mapping[str, Any | None], instance: Any, **kwargs):
    for attribute_name, attribute_type in kwargs.items():
        if not hasattr(instance, attribute_name):
            raise ValueError(f"Instance {type(instance).__name__} does not have attribute {attribute_name}")
        v = _json_get_by_type(json, attribute_name, attribute_type)
        setattr(instance, attribute_name, v)


def _json_put(json: MutableMapping[str, Any | None], key: str, value: Any | None):
    if value is None: return
    if "_" in key: key = str2camel(key)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        json[key] = value.isoformat()
    elif isinstance(value, int):
        json[key] = value
    elif isinstance(value, float):
        json[key] = value
    elif isinstance(value, UUID):
        json[key] = value.hex
    elif isinstance(value, str):
        json[key] = value
    elif isinstance(value, bool):
        json[key] = str(value).lower()
    elif issubclass(type(value), Enum):
        json[key] = str(value)
    elif issubclass(type(value), JsonItemBase):
        v = value.json_export_obj()
        if v is not None:
            json[key] = v

    else:
        json[key] = value


@dataclass
class JsonItemBase:
    pass


@dataclass
class TaskAction(JsonItemBase):
    action: str | None = None

    @classmethod
    def create_random(cls):
        o = cls()
        o.action = random_pick(["sql", "sftp", "ftp", "ftps", "zip", "email"])
        return o

    def json_import(self, json: Mapping[str, Any | None]):
        self.action = _json_get_by_type(json, "action", str)

    def json_export_obj(self) -> Mapping[str, Any] | None:
        d = dict()
        o = trim(xstr(self.action))
        if o is None: return None
        _json_put(d, "action", o)

    def _clean(self):
        self.action = trim(xstr(self.action))

    @property
    def json_is_valid(self):
        self._clean()
        return self.action is not None


@dataclass
class TaskSchedule(JsonItemBase):
    cron_str: str | None = None
    is_active: bool = True

    @classmethod
    def create_random(cls):
        o = cls()
        o.cron_str = " ".join([
            "*" if random_bool() else str(random_int(max=59)),
            "*" if random_bool() else str(random_int(max=23)),
            "*" if random_bool() else str(random_int(min=1, max=31)),
            "*" if random_bool() else str(random_int(min=1, max=12)),
            "*" if random_bool() else str(random_int(min=0, max=6))
        ])
        o.is_active = random_pick([True, True, False])
        return o

    def json_import(self, json: Mapping[str, Any | None]):
        self.cron_str = _json_get_by_type(json, "cron_str", str)
        self.is_active = _json_get_by_type(json, "is_active", bool)

    def json_export_obj(self) -> Mapping[str, Any] | None:
        d = dict()
        cron_str_v = trim(xstr(self.cron_str))
        if cron_str_v is None: return None
        _json_put(d, "cron_str", cron_str_v)
        _json_put(d, "is_active", self.is_active)

    def _clean(self):
        self.cron_str = trim(xstr(self.cron_str))
        if self.is_active is None: self.is_active = True

    @property
    def json_is_valid(self):
        self._clean()
        return self.cron_str is not None


@dataclass
class Task(JsonItemBase):
    id: int | None = None
    version_id: int | None = None
    created_on: datetime | None = None
    created_by: str | None = None
    modified_on: datetime | None = None
    modified_by: str | None = None
    name: str | None = None
    is_active: bool = True
    tags: MutableMapping[str, str] = field(default_factory=DictStrCasefold)
    schedules: List[TaskSchedule] = field(default_factory=list)
    actions: MutableMapping[int, TaskAction] = field(default_factory=dict)

    @classmethod
    def create_random(cls):
        rnames = ["system"] + list(random_names(8))
        o = cls()
        o.created_on = random_datetime(max=datetime_now_utc())
        o.created_by = random_pick(rnames)
        o.modified_on = o.created_on if random_bool() else random_datetime(min=o.created_on, max=datetime_now_utc())
        o.modified_by = o.created_by if random_bool() else random_pick(rnames)
        o.name = random_adjective() + " " + random_noun()
        o.is_active = random_pick([True, True, False])
        for _ in range(0 if random_bool(25) else 1 , random_int(max=5)):
            o.tags[random_noun()] = random_verb()
        for _ in range(0, random_int(max=5)):
            o.schedules.append(TaskSchedule.create_random())
        for i in range(0, random_int(max=5)):
            o.actions[i + 1] = TaskAction.create_random()
        return o

    def json_import(self, json: Mapping[str, Any | None]):
        self.id = _json_get_by_type(json, "id", int)
        self.version_id = _json_get_by_type(json, "version_id", int)
        self.created_on = _json_get_by_type(json, "created_on", datetime)
        self.created_by = _json_get_by_type(json, "created_by", str)
        self.modified_on = _json_get_by_type(json, "modified_on", datetime)
        self.modified_by = _json_get_by_type(json, "modified_by", str)
        self.name = _json_get_by_type(json, "name", str)
        self.is_active = _json_get_by_type(json, "is_active", bool)

        self.tags = DictStrCasefold[str, str]()
        for k, v in _json_get_by_type(json, "tags", dict).items():
            v = trim(xstr(v))
            if v is not None:
                self.tags[k] = v

        self.schedules = []
        for o in _json_get_by_type(json, "schedules", list):
            sched = TaskSchedule()
            sched.json_import(o)
            if sched.json_is_valid:
                self.schedules.append(sched)

        self.actions = dict()
        for k, v in _json_get_by_type(json, "actions", dict).items():
            action = TaskAction()
            action.json_import(v)
            if action.json_is_valid:
                self.actions[int(k)] = action

    def json_export_obj(self) -> Mapping[str, Any]:
        d = dict()
        _json_put(d, "id", self.id)
        _json_put(d, "version_id", self.version_id)
        _json_put(d, "created_on", self.created_on)
        _json_put(d, "created_by", self.created_by)
        _json_put(d, "modified_on", self.modified_on)
        _json_put(d, "modified_by", self.modified_by)
        _json_put(d, "name", self.name)
        _json_put(d, "is_active", self.is_active)

        tags_v = {}
        o = self.tags
        if o is not None:
            for k, v in o.items():
                k = trim(xstr(k))
                if k is None: continue
                v = trim(xstr(v))
                if v is None: continue
                tags_v[k] = v
        _json_put(d, "tags", tags_v)

        schedules_v = []
        o = self.schedules
        if o is not None:
            for sched in o:
                if sched is None: continue
                s = sched.json_export_obj()
                if s is None: continue
                if len(s) < 1: continue
                schedules_v.append(s)
        _json_put(d, "schedules", schedules_v)

        actions_v = {}
        o = self.actions
        if o is not None:
            for i, action in o.items():
                if action is None: continue
                act = action.json_export_obj()
                if act is None: continue
                actions_v[i] = act
        _json_put(d, "actions", actions_v)

        return d


class TaskExecutionStatus(str, Enum):
    CREATED = "created"
    VALIDATING = "validating"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"

    def __str__(self): return self.value


class TaskExecutionErrorType(str, Enum):
    OTHER = "other"
    VALIDATION_ERROR = "validation_error"
    EXECUTION_STEP_ERROR = "execution_step_error"

    def __str__(self): return self.value


@dataclass
class TaskExecution(JsonItemBase):
    id: int | None = None
    version_id: int | None = None
    status: TaskExecutionStatus = TaskExecutionStatus.CREATED
    current_step: int | None = None
    created_on: datetime | None = None
    validating_on: datetime | None = None
    queued_on: datetime | None = None
    started_on: datetime | None = None
    completed_on: datetime | None = None
    cancelled_on: datetime | None = None
    cancelled_by: str | None = None
    cancelled_step: int | None = None
    error_type: TaskExecutionErrorType | None = None
    error_str: str | None = None
    error_step: int | None = None

    task: Task | None = None
    triggering_schedule: TaskSchedule | None = None
    triggered_manually: bool = False
    triggered_manually_on: datetime | None = None
    triggered_manually_by: str | None = None

    @classmethod
    def create_random(cls, task: Task):
        # @formatter:off
        if task.actions is None or len(task.actions) < 1: return None
        o = cls()
        st = o.status = random_pick(TaskExecutionStatus)
        statuses = [TaskExecutionStatus(s) for s in ["created", "validating", "queued", "running", "completed"]]
        if o.status == TaskExecutionStatus.CANCELLED: st = random_pick(statuses[:-1])
        if st in statuses: o.created_on = random_datetime(min=task.modified_on, max=datetime_now_utc())
        if st in statuses[1:]: o.validating_on = random_datetime(min=o.created_on, max=datetime_now_utc())
        if st in statuses[2:]: o.queued_on = random_datetime(min=o.validating_on, max=datetime_now_utc())
        if st in statuses[3:]: o.started_on = random_datetime(min=o.queued_on, max=datetime_now_utc())
        if st in statuses[4:]: o.completed_on = random_datetime(min=o.started_on, max=datetime_now_utc())
        if o.status == TaskExecutionStatus.CANCELLED:
            o.cancelled_on = random_datetime(min=coalesce(o.completed_on, o.started_on, o.queued_on, o.validating_on, o.created_on), max=datetime_now_utc())
            o.cancelled_by = random_name()
            o.cancelled_step = None if o.started_on is None else random_pick(task.actions.keys())
        if o.status == TaskExecutionStatus.RUNNING: o.current_step = random_pick(task.actions.keys())
        if o.status == TaskExecutionStatus.COMPLETED:
            o.error_type = random_pick(TaskExecutionErrorType)
            if o.error_type == TaskExecutionErrorType.VALIDATION_ERROR: o.error_str = "Some VALIDATION error or exception"
            if o.error_type == TaskExecutionErrorType.OTHER: o.error_str = "Some other error"
            if o.error_type == TaskExecutionErrorType.EXECUTION_STEP_ERROR:
                o.error_step = random_pick(task.actions.keys())
                o.error_str = f"Step {o.error_step} failed for some reason"

        if len(task.schedules) > 0 and random_pick([True, True, True, False]):
            o.triggering_schedule = random_pick(task.schedules)
            o.triggered_manually = False
        else:
            o.triggered_manually = True
            o.triggered_manually_on = random_datetime(min=task.modified_on, max=o.created_on)
            o.triggered_manually_by = random_name()

        o.task = task
        return o
        # @formatter:on

    def json_import(self, json: Mapping[str, Any | None]):
        self.id = _json_get_by_type(json, "id", int)
        self.version_id = _json_get_by_type(json, "version_id", int)
        self.status = _json_get_by_type(json, "status", TaskExecutionStatus)
        self.current_step = _json_get_by_type(json, "current_step", int)
        self.created_on = _json_get_by_type(json, "created_on", datetime)
        self.validating_on = _json_get_by_type(json, "validating_on", datetime)
        self.queued_on = _json_get_by_type(json, "queued_on", datetime)
        self.started_on = _json_get_by_type(json, "started_on", datetime)
        self.completed_on = _json_get_by_type(json, "completed_on", datetime)
        self.cancelled_on = _json_get_by_type(json, "cancelled_on", datetime)
        self.cancelled_by = _json_get_by_type(json, "cancelled_by", str)
        self.cancelled_step = _json_get_by_type(json, "cancelled_step", int)
        self.error_type = _json_get_by_type(json, "error_type", TaskExecutionErrorType)
        self.error_str = _json_get_by_type(json, "error_str", str)
        self.error_step = _json_get_by_type(json, "error_step", int)
        self.task = _json_get_by_type(json, "task", Task)
        self.triggering_schedule = _json_get_by_type(json, "triggering_schedule", TaskSchedule)
        self.triggered_manually = _json_get_by_type(json, "triggered_manually", bool)
        self.triggered_manually_on = _json_get_by_type(json, "triggered_manually_on", datetime)
        self.triggered_manually_by = _json_get_by_type(json, "triggered_manually_by", str)

    def json_export_obj(self) -> Mapping[str, Any]:
        d = dict()
        _json_put(d, "id", self.id)
        _json_put(d, "version_id", self.version_id)
        _json_put(d, "status", self.status)
        _json_put(d, "current_step", self.current_step)
        _json_put(d, "created_on", self.created_on)
        _json_put(d, "validating_on", self.validating_on)
        _json_put(d, "queued_on", self.queued_on)
        _json_put(d, "started_on", self.started_on)
        _json_put(d, "completed_on", self.completed_on)
        _json_put(d, "cancelled_on", self.cancelled_on)
        _json_put(d, "cancelled_by", self.cancelled_by)
        _json_put(d, "cancelled_step", self.cancelled_step)
        _json_put(d, "error_type", self.error_type)
        _json_put(d, "error_str", self.error_str)
        _json_put(d, "error_step", self.error_step)
        _json_put(d, "task", self.task)
        _json_put(d, "triggering_schedule", self.triggering_schedule)
        _json_put(d, "triggered_manually", self.triggered_manually)
        _json_put(d, "triggered_manually_on", self.triggered_manually_on)
        _json_put(d, "triggered_manually_by", self.triggered_manually_by)
        return d


class ModelDatabase:
    _VALID_TYPES = [Task, TaskExecution]
    _VALID_TYPES_DICT = DictStrCasefold({t.__name__: t for t in _VALID_TYPES})

    def __init__(self, database: Database = None):
        if database is None: database = get_database()
        self.db = database

    def _check_is_valid_type(self, obj: Any):
        for valid_type in self._VALID_TYPES:
            if isinstance(obj, valid_type):
                return
        raise ValueError(f"Object {obj} of type '{type(obj).__name__}' is not valid, valid types are " + str([o.__name__ for o in self._VALID_TYPES]))

    def _construct_load_obj(self, row):
        t = self._VALID_TYPES_DICT[row.type]
        o = t()
        json_objs = json2obj(row.json)
        o.json_import(json_objs)
        o.id = row.id
        o.version_id = row.version_id
        return o

    def save(self, obj: Task | TaskExecution):
        log.debug(f"Saving {type(obj).__name__} with id={xstr(obj.id)}")
        self._check_is_valid_type(obj)
        obj_id = obj.id
        if obj_id is None:
            # Insert
            data_id, data_version_id = self.db.insert(data_type=obj.__class__.__name__, data_json=json2str(obj.json_export_obj()))
            obj.id = data_id
            obj.version_id = data_version_id
        else:
            # Update
            data_version_id = self.db.update(data_id=obj_id, data_json=json2str(obj.json_export_obj()), data_version_id=obj.version_id)
            obj.version_id = data_version_id

    def delete(self, obj: int | Task | TaskExecution) -> bool:
        if isinstance(obj, int):
            return self.db.delete(data_id=obj)

        self._check_is_valid_type(obj)
        obj_id = obj.id
        if obj_id is None:
            # Not an existing object
            return False
        else:
            # Delete
            return self.db.delete(data_id=obj_id, data_version_id=obj.version_id)

    def get(self, data_id: int) -> Task | TaskExecution | None:
        row = self.db.get(data_id=data_id)
        if row is None: return None
        return self._construct_load_obj(row)

    def get_all(self, cls) -> List[Task | TaskExecution]:
        result = []
        start = time.time()
        for row in self.db.get_all(cls.__name__):
            o = self._construct_load_obj(row)
            result.append(o)
        end = time.time()
        log.debug(f"Time taken to run the code was {end - start} seconds")
        return result

    def get_tasks(self) -> List[Task]:
        return self.get_all(Task)

    def get_task_executions(self) -> List[TaskExecution]:
        return self.get_all(TaskExecution)

    def create_random_items(self):
        task_num = random_int(10, 20)
        # task_num = 100
        log.info(f"Creating random items (Tasks={task_num})")

        for _ in range(0, task_num):
            t = Task.create_random()
            self.save(t)
            print(f"Task(id={t.id}, schedules={len(t.schedules)}, actions={len(t.actions)})")
            task_execution_count = random_int(1, 10)
            # if random_bool(20): task_execution_count = 0
            # task_execution_count = 100
            for _ in range(1, task_execution_count):
                te = TaskExecution.create_random(t)
                if te is not None:
                    print(f"Created task execution for Task(id={t.id})")
                    self.save(te)
                else:
                    print(f"Skipped task execution for Task(id={t.id})")


_model_database: ModelDatabase | None = None


def get_model_database() -> ModelDatabase:
    global _model_database
    if _model_database is None:
        _model_database = ModelDatabase()
    return _model_database
