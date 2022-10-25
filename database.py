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

from sqlalchemy import create_engine, MetaData, StaticPool, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

from config import Config
from utils import *

log = logging.getLogger(__name__)

_md_convention = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s_%(referred_column_0_N_name)s",
    "pk": "pk_%(table_name)s",
}

_md = MetaData(naming_convention=_md_convention)


@dataclass(frozen=True)
class DatabaseRow:
    id: int
    version_id: int
    type: str
    json: str


class DatabaseTableBase(DeclarativeBase):
    metadata = _md


class DatabaseTable(DatabaseTableBase):
    __tablename__ = Config.DATABASE_TABLE
    data_id: Mapped[int] = mapped_column(primary_key=True)
    data_version_id: Mapped[int] = mapped_column(nullable=False)
    data_type: Mapped[str] = mapped_column(String(100), index=True)
    data_json: Mapped[str] = mapped_column(Text)
    __mapper_args__ = {"version_id_col": data_version_id}

    @property
    def data(self):
        return json2obj(self.data_json)

    @data.setter
    def data(self, value):
        self.data_json = json2str(value)  # noqa

    def __str__(self) -> str:
        return self.__class__.__name__ + f"(id={self.data_id}, version_id={self.data_version_id}, data_json[len]={len(self.data_json)})"

    def __repr__(self) -> str:
        return self.__class__.__name__ + f"(id={self.data_id}, version_id={self.data_version_id}, data_json={self.data_json})"

    def as_item(self):
        return DatabaseRow(
            id=self.data_id,
            version_id=self.data_version_id,
            type=self.data_type,
            json=self.data_json,
        )


class Database:
    _TABLE_TYPE = DatabaseTable

    @staticmethod
    def _table_row_convert_func_impl(table_row: Any | None) -> Any | None:
        return None if table_row is None else table_row.as_item()

    _TABLE_ROW_CONVERT_FUNC = _table_row_convert_func_impl

    def __init__(self, uri: str = None):
        db_uri = coalesce(uri, Config.DATABASE_URI)
        if "sqlite" in db_uri and ":memory:" in db_uri:
            e = create_engine(db_uri, connect_args={"check_same_thread": False}, poolclass=StaticPool)
            self.is_memory_database = True
            log.warning("Running in-memory database")
        else:
            e = create_engine(db_uri)
            self.is_memory_database = False
        self._engine = e

    def begin_session(self):
        return Session(self._engine)

    def recreate_tables(self):
        with self._engine.begin() as connection:
            self._TABLE_TYPE.metadata.drop_all(connection)
            self._TABLE_TYPE.metadata.create_all(connection)
            connection.commit()

    def create_tables(self):
        with self._engine.begin() as connection:
            self._TABLE_TYPE.metadata.create_all(connection)
            connection.commit()

    @staticmethod
    def _insert(cls, data_type: str, data_json: str, session: Session) -> Tuple[int, int]:
        o = cls()
        o.data_type = data_type
        o.data_json = data_json
        session.add(o)
        session.flush()
        return o.data_id, o.data_version_id

    def insert(self, data_type: str, data_json: str, session: Session = None) -> Tuple[int, int]:
        log.debug(f"Inserting (data_type={data_type}, data_json={len(data_json)})")
        if session is not None:
            return self._insert(cls=self._TABLE_TYPE, data_type=data_type, data_json=data_json, session=session)
        with self.begin_session() as session:
            r = self._insert(cls=self._TABLE_TYPE, data_type=data_type, data_json=data_json, session=session)
            session.commit()
            return r

    @staticmethod
    def _update(cls, data_id: int, data_json: str, data_version_id: int, session: Session) -> int:
        o = session.get(cls, data_id)
        if o is None:
            raise ValueError(f"{cls.__name__}({data_id}) does not exist")
        data_version_id_actual = o.data_version_id
        if data_version_id != data_version_id_actual:
            raise ValueError(f"{cls.__name__}({data_id}) with row version {data_version_id_actual} does not match row version to update {data_version_id}")
        o.data_json = data_json
        session.add(o)
        session.flush()
        return o.data_version_id

    def update(self, data_id: int, data_json: str, data_version_id: int, session: Session = None) -> int:
        if session is not None:
            return self._update(cls=self._TABLE_TYPE, data_id=data_id, data_json=data_json, data_version_id=data_version_id, session=session)
        with self.begin_session() as session:
            r = self._update(cls=self._TABLE_TYPE, data_id=data_id, data_json=data_json, data_version_id=data_version_id, session=session)
            session.commit()
            return r

    @staticmethod
    def _delete(cls, data_id: int, session: Session, data_version_id: int = None) -> bool:
        o = session.get(cls, data_id)
        if o is None: return False
        if data_version_id is not None and data_version_id != o.data_version_id: return False
        session.delete(o)
        return True

    def delete(self, data_id: int, data_version_id: int = None, session: Session = None) -> bool:
        if session is not None:
            return self._delete(cls=self._TABLE_TYPE, data_id=data_id, session=session, data_version_id=data_version_id)
        with self.begin_session() as session:
            r = self._delete(cls=self._TABLE_TYPE, data_id=data_id, session=session, data_version_id=data_version_id)
            session.commit()
            return r

    @staticmethod
    def _get(cls, data_id: int, session: Session, data_version_id: int = None) -> Any | None:
        o = session.get(cls, data_id)
        if data_version_id is not None and data_version_id != o.data_version_id: return None
        return o

    def get(self, data_id: int, session: Session = None, data_version_id: int = None):
        if session is not None:
            return self._TABLE_ROW_CONVERT_FUNC(self._get(cls=self._TABLE_TYPE, data_id=data_id, session=session, data_version_id=data_version_id))
        with self.begin_session() as session:
            return self._TABLE_ROW_CONVERT_FUNC(self._get(cls=self._TABLE_TYPE, data_id=data_id, session=session, data_version_id=data_version_id))

    @staticmethod
    def _get_all(cls, session: Session, data_type: str = None) -> Iterable[Any]:
        if data_type is None:
            return session.query(cls)
        else:
            return session.query(cls).filter_by(data_type=data_type)

    def get_all(self, data_type: str = None, session: Session = None) -> List[Any]:
        if session is not None:
            return [self._TABLE_ROW_CONVERT_FUNC(o) for o in self._get_all(cls=self._TABLE_TYPE, data_type=data_type, session=session)]
        with self.begin_session() as session:
            return [self._TABLE_ROW_CONVERT_FUNC(o) for o in self._get_all(cls=self._TABLE_TYPE, data_type=data_type, session=session)]


_database: Database | None = None


def get_database() -> Database:
    global _database
    if _database is None:
        _database = Database()
    return _database
