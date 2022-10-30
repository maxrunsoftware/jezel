from abc import ABC, abstractmethod
from typing import Any, Mapping

from config import Config
from database3 import ColumnIdType, Database, Table, TableDefinition
from utils import coalesce, json2obj, json2str


class DatabaseObjectSerializer(ABC):
    @abstractmethod
    def small_serialize(self, obj: Any) -> str:
        raise NotImplementedError

    @abstractmethod
    def small_deserialize(self, string: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def medium_serialize(self, obj: Any) -> str:
        raise NotImplementedError

    @abstractmethod
    def medium_deserialize(self, string: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def large_serialize(self, obj: Any) -> str:
        raise NotImplementedError

    @abstractmethod
    def large_deserialize(self, string: str) -> Any:
        raise NotImplementedError

class DatabaseObjectSerializerBase(DatabaseObjectSerializer, ABC):
    def small_serialize(self, obj) -> str:
        return obj

    def small_deserialize(self, string: str) -> Any:
        return string

class DatabaseObjectSerializerJson(DatabaseObjectSerializerBase):

    def medium_serialize(self, obj: Mapping[str, str]) -> str:
        return json2str(obj)

    def medium_deserialize(self, string: str) -> Mapping[str, str]:
        return json2obj(string)

    def large_serialize(self, obj: Mapping[str, str]) -> str:
        return json2str(obj)

    def large_deserialize(self, string: str) -> Mapping[str, Any]:
        return json2obj(string)



class Meta:
    pass



class DatabaseObject:
    def __init__(self, table: Table):
        self._table = table









