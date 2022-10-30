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

from __future__ import annotations

from typing import Sequence

from sqlalchemy import Connection, create_engine, MetaData, StaticPool, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

from config import Config
from utils import *
import sys

log = logging.getLogger(__name__)

class DatabaseRowSlim:
    id: int | None = None
    version_id: int | None = None
    type: str
    value_slim: str

class DatabaseRow(DatabaseRowSlim):
    value: str


class TableBaseDefinition:
    def __init__(self):
        self._is_init_called = False
        self.naming_conventions: MutableMapping[str, str] = {
            # https://docs.sqlalchemy.org/en/20/core/constraints.html#configuring-a-naming-convention-for-a-metadata-collection
            "ix": "ix_%(table_name)s_%(column_0_N_name)s",
            "uq": "uq_%(table_name)s_%(column_0_N_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s_%(referred_column_0_N_name)s",
            "pk": "pk_%(table_name)s",
        }
        self._tables: List[TableDefinition] = []

        # will be set after init
        self.metadata: MetaData | None = None
        self.table_class_base: type | None = None

    @property
    def tables(self) -> Sequence[TableDefinition]: return self._tables

    def add(self, table: TableDefinition):
        table.table_base_definition = self
        self._tables.append(table)

    @property
    def is_init_called(self): return self._is_init_called

    def init(self):
        if self._is_init_called:
            raise ValueError("init() has already been called")
        self._is_init_called = True
        naming_conventions = dict(self.naming_conventions)
        md = self.metadata = MetaData(naming_convention=naming_conventions)

        class LocalDatabaseTableBase(DeclarativeBase):
            metadata = md

        self.table_class_base = LocalDatabaseTableBase

        for table in self._tables:
            table.table_base_definition = self  # set it again just to be safe someone didn't change it
            table._init()


class TableDefinition:
    def __init__(self):
        self._is_init_called = False
        self.table_name: str | None = None
        self.index_column_type = True
        self.index_column_data_slim = False

        # will be set after adding to TableBaseDefinition
        self.table_base_definition: TableBaseDefinition | None = None

        # will be set after init
        self.table_class: type | None = None

    def _init(self):
        if self._is_init_called:
            raise ValueError("init() has already been called")
        self._is_init_called = True

        class LocalDatabaseTable(self.table_base_definition.table_class_base):
            __tablename__ = self.table_name
            id: Mapped[int] = mapped_column(primary_key=True)
            version_id: Mapped[int] = mapped_column(nullable=False)
            type: Mapped[str] = mapped_column(Text, index=self.index_column_type)
            value_slim: Mapped[str] = mapped_column(Text, index=self.index_column_data_slim)
            value: Mapped[str] = mapped_column(Text)
            __mapper_args__ = {"version_id_col": version_id}

            def __str__(self) -> str:
                return self.__class__.__name__ + f"(id={self.id}, version_id={self.version_id}, type={self.type})"

            def __repr__(self) -> str:
                return self.__class__.__name__ + f"(id={self.id}, version_id={self.version_id}, type='{self.type}', value='{self.value}, value_slim='{self.value_slim})"

        self.table_class = LocalDatabaseTable




class Database:
    def __init__(self, db_uri: str, table_base_definition: TableBaseDefinition):
        if "sqlite" in db_uri and ":memory:" in db_uri:
            e = create_engine(db_uri, connect_args={"check_same_thread": False}, poolclass=StaticPool)
            self._is_memory_database = True
            log.warning(f"Running in-memory database  {db_uri}")
        else:
            e = create_engine(db_uri)
            self._is_memory_database = False
        self._engine = e

        if not table_base_definition.is_init_called:
            table_base_definition.init()

        self._table_base_definition = table_base_definition

        table_names = [t.table_name for t in self._table_base_definition.tables]
        log.debug(f"Successfully initialized {self.__class__.__name__} for db_uri={db_uri} for {len(table_names)} tables {table_names}")

    @property
    def table_base_definition(self):
        return self._table_base_definition

    @property
    def is_memory_database(self): return self._is_memory_database

    @property
    def engine(self): return self._engine

    def session_begin(self):
        s = Session(self.engine)
        if s is None:
            raise ValueError("Could not create Session object")
        return s

    def connection_begin(self):
        c = self.engine.begin()
        if c is None:
            raise ValueError("Could not create Connection object")
        return c

    def tables_drop(self, connection: Connection = None):
        if connection is not None:
            self.table_base_definition.metadata.drop_all(connection)
        else:
            with self.connection_begin() as connection:
                self.tables_drop(connection)
                connection.commit()


    def tables_create(self, connection: Connection = None):
        if connection is not None:
            self.table_base_definition.metadata.create_all(connection)
        else:
            with self.connection_begin() as connection:
                self.tables_create(connection)
                connection.commit()

    def tables_recreate(self, connection: Connection = None):
        if connection is not None:
            self.table_base_definition.metadata.drop_all(connection)
            self.table_base_definition.metadata.create_all(connection)
        else:
            with self.connection_begin() as connection:
                self.tables_recreate(connection)
                connection.commit()



    @staticmethod
    def _insert(cls, data_type: str, data_json: str, session: Session) -> Tuple[int, int]:
        o = cls()
        o.type = data_type
        o.data_json = data_json
        session.add(o)
        session.flush()
        return o.id, o.version_id

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



class DatabaseTable:
    def __init__(self, database: Database, table_definition: TableDefinition):
        self._database = database
        self._table_definition = table_definition

    @property
    def database(self):
        return self._database

    @property
    def table_definition(self):
        return self._table_definition

    @property
    def table_name(self):
        return self.table_definition.table_name

    def session_begin(self):
        return self.database.session_begin()

    def insert(self, row: DatabaseRow, session: Session = None, flush: bool = True):
        if session is not None:
            o = self.table_definition.table_class()
            o.type = row.type
            o.value = row.value
            o.value_slim = row.value_slim
            session.add(o)
            if flush:
                session.flush()
                row.id = o.id
                row.version_id = o.version_id
        else:
            with self.session_begin() as session:
                self.insert(row=row, session=session, flush=True)
                session.commit()


    def update(self, row: DatabaseRow | DatabaseRowSlim, session: Session = None, flush: bool = True):
        if session is not None:
            o = session.get(self.table_definition.table_class, row.id)
            if o is None:
                raise ValueError(f"{cls.__name__}({data_id}) does not exist")
            data_version_id_actual = o.data_version_id
            if data_version_id != data_version_id_actual:
                raise ValueError(f"{cls.__name__}({data_id}) with row version {data_version_id_actual} does not match row version to update {data_version_id}")
            o.data_json = data_json
            session.add(o)
            session.flush()

            o = self.table_definition.table_class()
            o.type = row.type
            o.value = row.value
            o.value_slim = row.value_slim
            session.add(o)
            if flush:
                session.flush()
                row.id = o.id
                row.version_id = o.version_id
        else:
            with self.session_begin() as session:
                self.update(row=row, session=session, flush=True)
                session.commit()

