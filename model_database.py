import uuid
from uuid import UUID
from threading import Lock
from typing import Callable, Dict, Generic, List, Mapping

from model import ModelBase

from typing import TypeVar, Type


T = TypeVar('T', constraint=ModelBase)


class ModelDatabaseType(Generic[T]):
    def __init__(self, model_type):
        if not issubclass(model_type, ModelBase):
            raise TypeError(f"type {model_type.__name__} is not a subclass of {ModelBase.__name__}")
        self._model_type = model_type
        self._has_ver = "ver" in model_type.fields
        self._data: Dict = dict()
        self._lock: Lock = Lock()

    @property
    def model_type(self):
        return self._model_type

    def _check_type(self, obj):
        if obj is None:
            raise ValueError(f"expected type {self._model_type.__name__} but got None")
        if not isinstance(obj, self._model_type):
            raise TypeError(f"expected type {self._model_type.__name__} but got type {obj.__class__.__name__}")

    def save(self, obj: T):
        self._check_type(obj)
        with self._lock:
            db = self._data
            obj_existing = db.get(obj.id)
            if obj_existing is not None:
                if self._has_ver:
                    if obj_existing.ver != obj.ver:
                        raise ValueError(f"Concurrency Error:  existing={obj_existing}  current={obj}")
            if self._has_ver:
                obj.ver = uuid.uuid4()
            db[obj.id] = obj.copy(deep=True)

    def delete(self, obj: T):
        self._check_type(obj)
        with self._lock:
            db = self._data
            obj_existing = db.get(obj.id)
            if obj_existing is not None:
                if self._has_ver:
                    if obj_existing.ver != obj.ver:
                        raise ValueError(f"Concurrency Error:  existing={obj_existing}  current={obj}")
            db.pop(obj.id)

    def get_all(self) -> Mapping[UUID, T]:
        with self._lock:
            return {o.id: o.copy(deep=True) for o in self._data.values()}

    def get_by_id(self, id: UUID) -> T | None:
        with self._lock:
            return self._data.get(id)

class ModelDatabase:
    def __init__(self):
        def all_subclasses(cls):
            return set(cls.__subclasses__()).union([s for c in cls.__subclasses__() for s in all_subclasses(c)])
        scs = [cls for cls in all_subclasses(ModelBase) if hasattr(cls, "id")]
        self._db: Dict[type, ModelDatabaseType] = {t: ModelDatabaseType(t) for t in scs}

    def __getitem__(self, model_type: Type[T]) -> ModelDatabaseType[T]:
        return self._db[model_type]




