import uuid
from typing import Dict, List
from uuid import UUID

from model import CancellationEvent, Config, Execution, Job, ModelBase, Server, Thread, TriggerEvent, User
from utils import Object


class MemoryDatabaseItem:
    def __init__(self):
        self._d: Dict = dict()

    def save(self, obj):
        obj.check()
        db = self._d
        obj_existing = db.get(obj.id)
        if obj_existing is not None:
            if hasattr(obj_existing, "ver"):
                if obj_existing.ver != obj.ver:
                    raise ValueError("Concurrency Error")
        if hasattr(obj_existing, "ver"):
            obj.ver = uuid.uuid4()
        db[obj.id] = obj.copy(deep=True)

    def delete(self, id: UUID, ver: UUID | None = None):
        db = self._d
        obj_existing = db.get(id)
        if obj_existing is not None:
            if hasattr(obj_existing, "ver"):
                if obj_existing.ver != ver:
                    raise ValueError("Concurrency Error")
            db.pop(id)

    def get_all(self) -> List:
        return [o.copy(deep=True) for o in self._d.values()]



class ModelDatabase:

    def __init__(self):
        def all_subclasses(cls):
            return set(cls.__subclasses__()).union([s for c in cls.__subclasses__() for s in all_subclasses(c)])

        scs = [cls for cls in all_subclasses(ModelBase) if hasattr(cls, "id")]
        self._db: Dict[type, MemoryDatabaseItem] = {t: MemoryDatabaseItem() for t in scs}

    def job_get_all(self) -> List[Job]:
        return self._db[Job].get_all()

    def job_save(self, job: Job):
        self._db[Job].save(job)

    def job_delete(self, job: Job):
        self._db[Job].delete(job.id, job.ver)

