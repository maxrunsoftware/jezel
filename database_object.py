
from __future__ import annotations

import logging
from _typeshed import SupportsKeysAndGetItem
from abc import ABC, abstractclassmethod, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, ItemsView, Iterable, Iterator, KeysView, Mapping, MutableMapping, overload, Set, Tuple, ValuesView
from uuid import UUID

from config import Config
from database import ColumnIdType, Database, Row, RowColumns, Table, TableDefinition
from utils import coalesce, DictStrCasefold, json2obj, json2str, trim, trim_casefold

log = logging.getLogger(__name__)


class Serializable(ABC):

    @abstractmethod
    def serialize(self) -> str: raise NotImplementedError

    @classmethod
    @abstractmethod
    def deserialize(cls, string: str): raise NotImplementedError





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


serializable_classes = SerializableClassesCache()







class DatabaseObject:

    @staticmethod
    def deserialize_tags(row: Row):
        if row is None or row.dmedium is None: return dict()
        o = json2obj(row.dmedium)
        return o

    @staticmethod
    def serialize_tags(tags: Mapping[str, str]):
        return json2str(dict(tags))

    @staticmethod
    def deserialize_obj(row: Row):
        if row is None or row.dsmall is None or row.dlarge is None: return None
        cls = serializable_classes.get_class(row.dsmall)
        obj_json = json2obj(row.dlarge)
        obj = cls.deserialize(obj_json)
        return obj

    @staticmethod
    def serialize_obj(obj: Serializable) -> Tuple[str, str]:
        dsmall = serializable_classes.class_fullname(type(obj))
        dlarge = obj.serialize()
        return dsmall, dlarge

    def __init__(self, table: Table, row: Row = None):
        self._table = table
        self._id: int | UUID | None = None if row is None else row.id
        self._ver: int | None = None if row is None else row.ver
        self._tags: Dict[str, str] = self.deserialize_tags(row)
        self._obj: Serializable | None = self.deserialize_obj(row)

    @property
    def id(self): return self._id

    @property
    def ver(self): return self._ver

    @property
    def tags(self) -> Dict[str, str]: return self._tags

    @property
    def obj_is_loaded(self) -> bool:
        return self._obj is not None

    @property
    def obj(self) -> Serializable:
        if self._obj is None:
            self.refresh(overwrite_existing=False)
        return self._obj

    @obj.setter
    def obj(self, value: Serializable):
        self._obj = value

    def save(self):
        if self._obj is None:
            raise ValueError(f"{self.__class__.__name__}.obj cannot be None")
        dmedium = self.serialize_tags(self._tags)
        dsmall, dlarge = self.serialize_obj(self._obj)
        row = Row(self._id, self._ver, dsmall, dmedium, dlarge)
        if row.ver is None:
            row =
            self._table.insert()

    def refresh(self, overwrite_existing=False):
        id = self.id
        if id is None: return
        row = self._table.select_single(id=self.id)
        if overwrite_existing:
            if row is None:
                self._ver = None
                self._tags.clear()
                self._obj = None
            else:
                self._ver = row.ver
                self._tags.clear()
                self._tags.update(self.deserialize_tags(row))
                self._obj = self.deserialize_obj(row)
        else:
            if row is None:
                pass
            else:
                if self._ver is None: self._ver = row.ver
                if len(self._tags) == 0:
                    self._tags.clear()
                    self._tags.update(self.deserialize_tags(row))
                if self._obj is None: self._obj = self.deserialize_obj(row)



class DatabaseObjectDatabase:
    def __init__(self, table: Table):
        self._table = table

    @property
    def types(self) -> Set[type]:
        return set([serializable_classes.get_class(type_name) for type_name in self._table.select_dsmalls()])




    @property
    def table(self):
        return self._table




