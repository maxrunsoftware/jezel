from datetime import datetime
from typing import Dict, List
from uuid import UUID, uuid4

import pydantic
from orjson import orjson
from pydantic import BaseModel, Field, validate_model

import utils
from utils import datetime_now_utc, json2str


def orjson_dumps(v, *, default):
    # orjson.dumps returns bytes, to match standard json.dumps we need to decode
    return orjson.dumps(v, default=default).decode()


def parse_str(field: str, check=False, trim=False, casefold=False) -> classmethod:
    # https://github.com/pydantic/pydantic/issues/940#issuecomment-569765091
    def apply(value: str):
        if value is not None and trim: value = utils.trim(value)
        if value is not None and casefold: value = value.casefold()
        if value is None and check:
            if trim:
                raise ValueError("value cannot be empty or None")
            else:
                raise ValueError("value cannot be None")
        return value

    decorator = pydantic.validator(field, pre=True, allow_reuse=True)
    validator = decorator(apply)
    return validator


def parse_dict(field: str, trim=(False, False), casefold=(False, False)) -> classmethod:
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

    decorator = pydantic.validator(field, pre=True, allow_reuse=True)
    validator = decorator(apply)
    return validator


def parse_list(field: str, check=False) -> classmethod:
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

    decorator = pydantic.validator(field, pre=True, allow_reuse=True)
    validator = decorator(apply)
    return validator


class ModelBase(BaseModel):
    def check(self):
        # https://github.com/pydantic/pydantic/issues/1864
        values, fields_set, validation_error = validate_model(
            self.__class__, self.__dict__
        )
        if validation_error:
            raise validation_error
        try:
            object.__setattr__(self, "__dict__", values)
        except TypeError as e:
            raise TypeError(
                "Model values must be a dict; you may not have returned "
                + "a dictionary from a root validator"
            ) from e
        object.__setattr__(self, "__fields_set__", fields_set)

    def json_pretty(self, indent=2):
        return json2str(self.json(by_alias=True), indent=indent)

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson_dumps
        extra = "allow"
        alias_generator = utils.str2camel
        allow_population_by_field_name = True
        # anystr_strip_whitespace = True


class Tag(ModelBase):
    name: str
    _name: classmethod = parse_str("name", trim=True, casefold=True, check=True)

    value: str
    _value: classmethod = parse_str("value", trim=True, casefold=True, check=True)


class ConfigItem(ModelBase):
    name: str
    _name: classmethod = parse_str("name", trim=True, casefold=True, check=True)

    value: str

    action: str | None
    _action: classmethod = parse_str("action", trim=True, casefold=True)


class Config(ModelBase):
    id: UUID = Field(default_factory=uuid4)
    ver: UUID = Field(default_factory=uuid4)

    created_on: datetime

    created_by: str
    _created_by: classmethod = parse_str("created_by", trim=True, check=True)

    modified_on: datetime

    modified_by: str
    _modified_by: classmethod = parse_str("modified_by", trim=True, check=True)

    tags: List[Tag] = Field(default_factory=list)
    _tags: classmethod = parse_list("tags", check=True)

    name: str
    _name: classmethod = parse_str("name", trim=True, check=True)

    items: List[ConfigItem] = Field(default_factory=list)
    _items: classmethod = parse_list("items", check=True)

    @classmethod
    def create(
            cls,
            name: str,
            created_by: str
    ):
        now = datetime_now_utc()
        return cls(
            name=name,
            created_on=now,
            created_by=created_by,
            modified_on=now,
            modified_by=created_by,
        )


class Schedule(ModelBase):
    is_active: bool = True

    cron: str
    _cron: classmethod = parse_str("cron", trim=True, check=True)


class Job(ModelBase):
    id: UUID = Field(default_factory=uuid4)
    ver: UUID = Field(default_factory=uuid4)

    created_on: datetime

    created_by: str
    _created_by: classmethod = parse_str("created_by", trim=True, check=True)

    modified_on: datetime

    modified_by: str
    _modified_by: classmethod = parse_str("modified_by", trim=True, check=True)

    tags: List[Tag] = Field(default_factory=list)
    _tags: classmethod = parse_list("tags", check=True)

    is_active: bool = True

    name: str
    _name: classmethod = parse_str("name", trim=True, check=True)

    schedules: List[Schedule] = Field(default_factory=list)
    _schedules: classmethod = parse_list("schedules", check=True)

    configs: List[UUID] = Field(default_factory=list)
    _configs: classmethod = parse_list("configs")

    @classmethod
    def create(
            cls,
            name: str,
            created_by: str
    ):
        now = datetime_now_utc()
        return cls(
            name=name,
            created_on=now,
            created_by=created_by,
            modified_on=now,
            modified_by=created_by,
        )


def main():
    j1 = Job.create("Job1", "Steve")
    j1.schedules.append(Schedule(cron="111"))
    j1.schedules.append(Schedule(cron="222"))
    j1s = j1.json(by_alias=True)
    print(j1s)

    j2 = Job.parse_raw(j1s)
    j2.check()

    # print(json2str(j1s, indent=2))


    # j1.schedules[0].cron = None
    # j11 = j1.copy(deep=True)
    # print(j11.json_pretty())
    # j1.check()


if __name__ == "__main__":
    main()
