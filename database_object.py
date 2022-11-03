from __future__ import annotations

import logging
import typing
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, MutableMapping, Set, Tuple
from uuid import UUID

from database import Row, RowColumns, Table
from utils import bool_parse_none, datetime_parse_none, DictStrCasefold, float_parse_none, int_parse_none, json2obj, json2str, str2camel, str2snake, trim, uuid_parse_none, xstr

log = logging.getLogger(__name__)

class SerializableWriter:
    def __init__(self):
        self.dic = dict()

    def put_list_serializable_base(self, key, items: List[SerializableBase]):
        lis = []
        if items is not None:
            for item in items:
                if item is None: continue
                w = SerializableWriter()
                item.serialize_write(w)
                d = w.dic
                if len(d) > 0:
                    lis.append(d)
        if len(lis) > 0:
            self.put(key=key, value=lis)

    def put_dict_str_str(self, key, items: Mapping[str, str], trim_keys=True, trim_values=True):
        d = dict()
        if items is not None:
            for k, v in items.items():
                if trim_keys: k = trim(k)
                if trim_values: v = trim(v)
                if k is None or v is None: continue
                d[k] = v
        if len(d) > 0:
            self.put(key=key, value=d)

    def put(self, key: str, value: Any | None):
        d = self.dic
        if value is None: return
        if "_" in key: key = str2camel(key)
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            d[key] = value.isoformat()
        elif isinstance(value, int):
            d[key] = value
        elif isinstance(value, float):
            d[key] = value
        elif isinstance(value, UUID):
            d[key] = value.hex
        elif isinstance(value, str):
            d[key] = value
        elif isinstance(value, bool):
            d[key] = str(value).lower()
        elif issubclass(type(value), Enum):
            d[key] = str(value)
        else:
            d[key] = value


class SerializableReader:
    def __init__(self, dic: Mapping[str, Any | None]):
        assert dic is not None
        assert isinstance(dic, Mapping)
        self._dic = dic

    def _get_raw(self, key: str) -> Any | None:
        d = self._dic
        v = d.get(key)
        if v is not None: return v

        key = trim(key)
        if key is None:
            raise ValueError("Key cannot be empty")
        v = d.get(key)
        if v is not None: return v

        key_camel = str2camel(key)
        v = d.get(key_camel)
        if v is not None: return v

        key_snake = str2snake(key)
        v = d.get(key_snake)
        if v is not None: return v

        key_casefold = key.casefold()
        v = d.get(key_casefold)
        if v is not None: return v

        return None

    def _get_str(self, key: str, is_trimmed=True) -> str | None:
        v = self._get_raw(key)
        if v is None: return None
        v = str(v)
        if is_trimmed: v = trim(v)
        return v

    def get_list_serializable_base(self, key: str, cls: type):
        objs = []
        for o in self.get(key, list):
            oo = cls()
            oo.deserialize_read(SerializableReader(o))
            objs.append(oo)
        return objs

    def get(self, key: str, typ: type | None, default: Any | None = None, is_trimmed=True) -> Any | None:
        o = self._get(key=key, typ=typ, is_trimmed=is_trimmed)
        return default if o is None else o

    def _get(self, key: str, typ: type | None, is_trimmed=True) -> Any | None:
        if typ is None:
            return self._get_raw(key)

        origin = typing.get_origin(typ)
        if origin is not None:
            if origin is typing.Union or origin.__name__ == "UnionType":  # Need to compare str names because "from __future__ import annotations" messes up the type system on dataclasses
                nonetype = type(None)
                subtypes = [subtyp for subtyp in typing.get_args(typ) if subtyp != nonetype]
                if len(subtypes) > 1:
                    raise ValueError(f"Error parsing '{key}' because it contains multiple types {typ}")
                return self.get(key=key, typ=subtypes[0], is_trimmed=is_trimmed)

        if typ == str:
            return self._get_str(key, is_trimmed=is_trimmed)
        elif typ == datetime:
            return datetime_parse_none(self._get_str(key), tz=timezone.utc)
        elif typ == int:
            return int_parse_none(self._get_str(key))
        elif typ == float:
            return float_parse_none(self._get_str(key))
        elif typ == bool:
            return bool_parse_none(self._get_str(key))
        elif typ == UUID:
            return uuid_parse_none(self._get_str(key))
        elif typ == list or typ == List:
            v = self._get_raw(key)
            result = []
            if v is None: return result
            for item in v:
                if item is not None:
                    result.append(item)
            return result

        elif typ == DictStrCasefold:
            v = self._get_raw(key)
            result = DictStrCasefold()
            if v is None: return result
            for key, value in v.items():
                key = trim(xstr(key))
                if key is not None and value is not None: result[key] = value
            return result

        elif typ == dict or typ == Mapping or typ == MutableMapping:
            v = self._get_raw(key)
            result = dict()
            if v is None: return result
            for key, value in v.items():
                key = trim(xstr(key))
                if key is not None and value is not None: result[key] = value
            return result

        elif issubclass(typ, Enum):
            v = self._get_str(key)
            if v is None: return None
            return typ(v)

        else:
            raise NotImplementedError(f"Type '{typ.__name__}' is not implemented")



class Serializable(ABC):

    @abstractmethod
    def serialize(self) -> str: raise NotImplementedError

    @classmethod
    @abstractmethod
    def deserialize(cls, string: str): raise NotImplementedError



class SerializableBase(Serializable):

    def serialize(self) -> str:
        sw = SerializableWriter()
        self.serialize_write(sw)
        s = json2str(sw.dic)
        return s

    @abstractmethod
    def serialize_write(self, w: SerializableWriter): raise NotImplementedError

    @classmethod
    def deserialize(cls, string: str):
        o = cls()
        d = json2obj(string)
        sr = SerializableReader(d)
        o.deserialize_read(sr)
        return o

    @abstractmethod
    def deserialize_read(self, r: SerializableReader): raise NotImplementedError

class SerializableClassesCache:
    @staticmethod
    def class_fullname(obj):
        cls = obj
        if not isinstance(cls, type):
            cls = obj.__class__
        module = None
        try:
            module = cls.__module__
            if trim(module) is None or module in ["builtins", "__builtin__"]: module = None
        except AttributeError:
            pass
        name = None
        try:
            name = cls.__qualname__
        except AttributeError:
            pass
        if trim(name) is None: name = cls.__name__
        if trim(name) is None: name = None
        if name is None:
            raise ValueError("Could not determine full class name for obj: " + repr(obj))
        return ".".join([x for x in [module, name] if x is not None])

    @staticmethod
    def get_subclasses_recursive(cls):
        """
        Gets all subclasses of a class
        https://stackoverflow.com/a/3862957
        """
        return set(cls.__subclasses__()).union([s for c in cls.__subclasses__() for s in SerializableClassesCache.get_subclasses_recursive(c)])

    def serializable_subclasses_rescan(self):
        for cls in self.get_subclasses_recursive(Serializable):
            self.add_class(cls)

    def add_class(self, cls, name=None):
        if name is None:
            name = self.class_fullname(cls)
            if name not in self.cache:
                self.cache[name] = cls
        else:
            self.cache[name] = cls
        return cls

    def get_class(self, name: str):
        assert isinstance(name, str)

        cls = self.cache.get(name)
        if cls is not None: return cls

        self.serializable_subclasses_rescan()
        cls = self.cache.get(name)
        if cls is not None: return cls

        items = [tpl for tpl in self.cache.items()]
        name_casefold = name.casefold()
        for k, v in items:
            k = k.casefold()
            if k == name_casefold:
                return self.add_class(v, k)

        for k, v in items:
            k = k.split(".").pop()
            if k == name:
                return self.add_class(v, k)

        name_casefold_last = name_casefold.split(".").pop()
        for k, v in items:
            k = k.split(".").pop().casefold()
            if k == name_casefold_last:
                return self.add_class(v, k)

        raise TypeError(f"Could not find registered type for class name '{name}'")

    def __init__(self):
        self.cache: Dict[str, type] = dict()
        self.serializable_subclasses_rescan()




def _deserialize_tags(dmedium: str):
    if dmedium is None: return dict()
    o = json2obj(dmedium)
    return o

def _serialize_tags(tags: Mapping[str, str]):
    assert tags is not None
    assert isinstance(tags, Mapping)
    return json2str(dict(tags))

def _deserialize_obj(data: Tuple[str, str]):
    cls = serializable_classes.get_class(data[0])
    obj = cls.deserialize(data[1])
    return obj

def _serialize_obj(obj: Serializable) -> Tuple[str, str]:
    assert isinstance(obj, Serializable)
    dsmall = serializable_classes.class_fullname(type(obj))
    dlarge = obj.serialize()
    return dsmall, dlarge

serializable_classes = SerializableClassesCache()


class DatabaseObject:



    @classmethod
    def create_from_row(cls, db: DatabaseObjectDatabase, row: Row) -> DatabaseObject:
        o = cls(db=db)
        o._id = row.id
        o._ver = row.ver
        o._tags = _deserialize_tags(row.dmedium)
        o._obj_data = (row.dsmall, row.dlarge)
        o._obj = None
        return o

    @classmethod
    def create_from_serializable(cls, db: DatabaseObjectDatabase, obj: Serializable, **kwargs) -> DatabaseObject:
        o = cls(db=db)
        o._id = None
        o._ver = None
        o._tags = dict()
        for k, v in kwargs.items():
            k = trim(xstr(k))
            if k is None: continue
            if v is None: continue
            o._tags[k] = v
        o._obj_data = None
        o._obj = obj
        return o

    def __init__(self, db: DatabaseObjectDatabase):
        assert db is not None
        assert isinstance(db, DatabaseObjectDatabase)
        self._db = db
        self._table = db.table
        self._id: int | UUID | None = None
        self._ver: int | None = None
        self._tags: Dict[str, str] = dict()
        self._obj: Serializable | None = None
        self._obj_data: Tuple[str, str] | None = None

    @property
    def id(self):
        return self._id

    @property
    def ver(self):
        return self._ver

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags

    @property
    def obj(self):
        o = self._obj
        od = self._obj_data
        if o is not None:
            if od is not None: self._obj_data = None  # clear raw data to free memory
            return o
        if od is None:
            return None

        o = _deserialize_obj(od)
        self._obj = o
        self._obj_data = None
        return o

    @obj.setter
    def obj(self, value: Serializable):
        self._obj = value
        self._obj_data = None

    def save(self):
        o, od = self._obj, self._obj_data
        if o is None and od is None:
            raise ValueError(f"{self.__class__.__name__}.obj cannot be None")
        dmedium = _serialize_tags(self._tags)
        if o is not None:
            dsmall, dlarge = _serialize_obj(o)
        else:
            dsmall, dlarge = od

        row = Row(self._id, self._ver, dsmall, dmedium, dlarge)
        row_result = self._table.save(row)
        if self._id != row_result.id: self._id = row_result.id
        self._ver = row_result.ver



class DatabaseObjectDatabase:
    def __init__(self, table: Table):
        self._table = table

    def get_types(self) -> Set[type]:
        return set([serializable_classes.get_class(type_name) for type_name in self._table.select_dsmalls()])

    # def get_all_by_type(self, types: Iterable[type]) -> List[Serializable]:
    #     results = []
    #     type_names = [serializable_classes.class_fullname(t) for t in types]
    #     return results

    def get_all(self, types: List[type] = None) -> List[DatabaseObject]:
        types = set() if types is None else {t for t in types if t is not None}
        if len(types) == 0:
            rows = self._table.select_all()
        else:
            type_names = [serializable_classes.class_fullname(t) for t in types]
            rows = self._table.select_where_dsmalls(dsmalls=type_names)

        return [DatabaseObject.create_from_row(self, row) for row in rows]


    @property
    def table(self):
        return self._table
