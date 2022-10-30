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

import uuid
from abc import ABC
from dataclasses import dataclass
from enum import auto, Flag
from typing import Callable, final, NamedTuple, Sequence

import sqlalchemy
from sqlalchemy import bindparam, BindParameter, Column, Connection, create_engine, Index, Integer, MetaData, null, PrimaryKeyConstraint, Select, StaticPool, String, Text
from sqlalchemy.sql.functions import count
from sqlalchemy import func

import utils
from utils import *

log = logging.getLogger(__name__)

_ROW_VERSION_DEFAULT = 1
_ROW_VERSION_STEP = 1

def row_id_uuid_gen(): return uuid.uuid4()
_ROW_ID_UUID_GEN = row_id_uuid_gen

class DatabaseError(ValueError):

    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)

class IdError(DatabaseError):

    def __init__(self, msg: str, table_name: str, id: int):
        self.table_name = table_name
        self.id = id
        super().__init__(msg)

class ConcurrencyError(DatabaseError):

    def __init__(self, msg: str, table_name: str, id: int, ver: int, actual_row_ver: int):
        self.table_name = table_name
        self.id = id
        self.ver = ver
        self.actual_row_ver = actual_row_ver
        super().__init__(msg)


@final
class Row(NamedTuple):
    id: int | UUID | None
    ver: int | None
    type: str | None
    meta: str | None
    data: str | None


    def __str__(self):
        names = ["id", "ver", "type", "meta", "data"]
        vals = [("<None>" if v is None else ("'" + v + "'") if isinstance(v, str) else str(v)) for v in [*self]]
        return self.__class__.__name__ + "(" + ", ".join([f"{n}={v}" for n, v in zip(names, vals)]) + ")"

    def __repr__(self):
        names = ["id", "ver", "type", "meta", "data"]
        vals = [(str(None) if v is None else ("'" + v + "'") if isinstance(v, str) else f"UUID('{v}')" if isinstance(v, UUID) else str(v)) for v in [*self]]
        return self.__class__.__name__ + "(" + ", ".join([f"{n}={v}" for n, v in zip(names, vals)]) + ")"


@final
@dataclass(slots=True)
class RowMutable:
    id: int | UUID | None
    ver: int | None
    type: str | None
    meta: str | None
    data: str | None

    @classmethod
    def from_row(cls, row: Row) -> RowMutable:
        return cls(*row)

    def to_row(self) -> Row:
        return Row(self.id, self.ver, self.type, self.meta, self.data)

class RowColumns(Flag):
    ID = auto()
    VER = auto()
    TYPE = auto()
    META = auto()
    DATA = auto()

    ALL = ID | VER | TYPE | META | DATA

    def __str__(self):
        items = [
            "ID" if self.ID in self else "__",
            "VER" if self.VER in self else "___",
            "TYPE" if self.TYPE in self else "____",
            "META" if self.META in self else "____",
            "DATA" if self.DATA in self else "____"
        ]
        if len(items) == 0: return None
        s = "|".join(items)
        return s

    @classmethod
    def combine(cls, values: Sequence[RowColumns]):
        if values is None: return None
        if len(values) == 0: return None
        val = None
        for v in set(values):
            if v is None: continue
            if v is cls.ALL: return cls.ALL
            if val is None:
                val = v
            else:
                if v not in val: val = val | v
            if val == cls.ALL: return cls.ALL
        return val

class ColumnIdType(Enum):
    INT = auto()
    UUID = auto()

    @property
    def sqlalchemy_type(self):
        if self is ColumnIdType.INT: return Integer
        elif self is ColumnIdType.UUID: return sqlalchemy.Uuid
        else: raise NotImplementedError

@dataclass
class TableDefinition:
    tbl_name: str = None
    tbl_schema_name: str = None

    col_id_name: str = "id"
    col_id_type: ColumnIdType = ColumnIdType.INT

    col_ver_name: str = "ver"

    col_type_name: str = "type"
    col_type_length: int = 200
    col_type_indexed: bool = True

    col_meta_name: str = "meta"
    col_meta_length: int = None
    col_meta_indexed: bool = False

    col_data_name: str = "data"
    col_data_length: int = None
    col_data_indexed: bool = False

    cols_id_ver_indexed: bool = True
    cols_id_ver_index_unique: bool = True

    def set(self, **kwargs): return dataclass_set(self, **kwargs)

    def create_table_detail(self, metadata: MetaData) -> TableDetail:
        _collen = lambda x: Text if x is None else String(x)

        cols = [
            Column(self.col_id_name, self.col_id_type.sqlalchemy_type, nullable=False),
            Column(self.col_ver_name, Integer, nullable=False),
            Column(self.col_type_name, _collen(self.col_type_length), nullable=False),
            Column(self.col_meta_name, _collen(self.col_meta_length), nullable=False),
            Column(self.col_data_name, _collen(self.col_data_length), nullable=False),
            PrimaryKeyConstraint("id", name=f"pk_{self.tbl_name}"),

            Index(f"ix_{self.tbl_name}_type", "type") if self.col_type_indexed else None,
            Index(f"ix_{self.tbl_name}_meta", "meta") if self.col_meta_indexed else None,
            Index(f"ix_{self.tbl_name}_data", "data") if self.col_data_indexed else None,

            Index(f"uq_{self.tbl_name}_id_ver", "id", "ver", unique=self.cols_id_ver_index_unique) if self.cols_id_ver_indexed else None,
        ]
        cols = [c for c in cols if c is not None]  # remove indexes we did not end up creating
        table = sqlalchemy.Table(self.tbl_name, metadata, *cols, schema=self.tbl_schema_name,)

        return TableDetail.create(table, cols[0], cols[1], cols[2], cols[3], cols[4], self.col_id_type)








class Database:
    _TABLE_KEY_NAMING_CONVENTIONS: MutableMapping[str, str] = {
        # https://docs.sqlalchemy.org/en/20/core/constraints.html#configuring-a-naming-convention-for-a-metadata-collection
        "ix": "ix_%(table_name)s_%(column_0_N_name)s",
        "uq": "uq_%(table_name)s_%(column_0_N_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s_%(referred_column_0_N_name)s",
        "pk": "pk_%(table_name)s",
    }

    def __init__(self, db_uri: str, tables: Sequence[TableDefinition]):
        if "sqlite" in db_uri and ":memory:" in db_uri:
            e = create_engine(db_uri, connect_args={"check_same_thread": False}, poolclass=StaticPool)
            self._is_memory_database = True
            log.warning(f"Running in-memory database  {db_uri}")
        else:
            e = create_engine(db_uri)
            self._is_memory_database = False
        self._engine = e
        log.debug(f"Successfully created {self.__class__.__name__} for db_uri={db_uri}")

        self._metadata = MetaData(naming_convention=dict(self._TABLE_KEY_NAMING_CONVENTIONS))
        self._tables: [str, Table] = {}
        for td in tables:
            table_detail = td.create_table_detail(self._metadata)
            dtable = Table(self.connection_begin, table_detail)
            self._tables[td.tbl_name] = dtable

        log.debug(f"Initialized {self.__class__.__name__} for db_uri={db_uri} with {len(self._tables)} tables {[t.table_detail.tbl.key for t in self._tables.values()]}")

    @property
    def is_memory_database(self):
        return self._is_memory_database

    @property
    def engine(self):
        return self._engine

    @property
    def tables(self):
        return self._tables

    @property
    def metadata(self):
        return self._metadata

    def connection_begin(self):
        c = self.engine.begin()
        # Check this to be sure we don't run into infinite recursion for other self calling methods
        if c is None: raise ValueError("Could not create Connection object")
        return c

    def tables_drop(self, connection: Connection = None):
        if connection is not None:
            self.metadata.drop_all(connection)
        else:
            with self.connection_begin() as connection:
                self.tables_drop(connection)
                connection.commit()

    def tables_create(self, connection: Connection = None):
        if connection is not None:
            self.metadata.create_all(connection)
        else:
            with self.connection_begin() as connection:
                self.tables_create(connection)
                connection.commit()

    def tables_recreate(self, connection: Connection = None):
        if connection is not None:
            self.metadata.drop_all(connection)
            self.metadata.create_all(connection)
        else:
            with self.connection_begin() as connection:
                self.tables_recreate(connection)
                connection.commit()


class TableDetailColumn(NamedTuple):
    col: Column
    name: str
    bp_where: BindParameter
    bp_where_name: str
    bp_value: BindParameter
    bp_value_name: str

    @classmethod
    def create(cls, col: Column):
        return cls(
            col=col,
            name=col.name,
            bp_where=bindparam("w_" + col.name),
            bp_where_name="w_" + col.name,
            bp_value=bindparam("v_" + col.name),
            bp_value_name="v_" + col.name,
        )


class TableDetail(NamedTuple):
    tbl: sqlalchemy.Table
    tbl_key: str
    tbl_name: str

    col_id: TableDetailColumn
    col_ver: TableDetailColumn
    col_type: TableDetailColumn
    col_meta: TableDetailColumn
    col_data: TableDetailColumn

    col_id_type: ColumnIdType

    @classmethod
    def create(cls, tbl: sqlalchemy.Table, col_id: Column, col_ver: Column, col_type: Column, col_meta: Column, col_data: Column, col_id_type: ColumnIdType):
        return cls(
            tbl=tbl,
            tbl_key=tbl.key,
            tbl_name=tbl.name,
            col_id=TableDetailColumn.create(col_id),
            col_ver=TableDetailColumn.create(col_ver),
            col_type=TableDetailColumn.create(col_type),
            col_meta=TableDetailColumn.create(col_meta),
            col_data=TableDetailColumn.create(col_data),
            col_id_type=col_id_type,
        )







class TableMixin(ABC):

    class _StatementCache:
        def __init__(self):
            self._cache = dict()

        def get(self, key: str, fac: Callable[[], Any]):
            d = self._cache
            stmt = d.get(key)
            if stmt is None:
                stmt = fac()
                d[key] = stmt
            return stmt

    @abstractmethod
    def _begin_tran(self) -> Connection:
        raise NotImplementedError

    @property
    @abstractmethod
    def _table_detail(self) -> TableDetail:
        raise NotImplementedError

    @property
    @abstractmethod
    def _table_stmt_cache(self) -> _StatementCache:
        raise NotImplementedError



class TableSelectMixin(TableMixin, ABC):
    _log_select = log.getChild("select")

    def select(self, cols: RowColumns, where: Callable[[Select], Tuple[Select, Dict]], c: Connection = None):
        if c is None:
            with self._begin_tran() as c:
                return self.select(cols=cols, where=where, c=c)
        t, sc = self._table_detail, self._table_stmt_cache
        # TODO: Implement statement cache
        rcols = [
            t.col_id.col if RowColumns.ID in cols else null(),
            t.col_ver.col if RowColumns.VER in cols else null(),
            t.col_type.col if RowColumns.TYPE in cols else null(),
            t.col_meta.col if RowColumns.META in cols else null(),
            t.col_data.col if RowColumns.DATA in cols else null(),
        ]
        stmt = sqlalchemy.select(*rcols)
        stmt, dic = where(stmt)
        r = c.execute(stmt, dic).tuples()

        return [Row(*tupl) for tupl in r.all()]


    def select_single(self, cols: RowColumns, id: int, c: Connection = None) -> Row | None:
        t = self._table_detail

        def where(stmt: Select):
            stmt = stmt.where(t.col_id.col == t.col_id.bp_where)
            dic = {t.col_id.bp_where: id}
            return stmt, dic

        r = self.select(cols=cols, where=where, c=c)
        return r[0] if r else None

    def select_count(self, where: Callable[[Select], Tuple[Select, Dict]], c: Connection = None) -> int:
        if c is None:
            with self._begin_tran() as c:
                return self.select_count(where=where, c=c)

        t, sc = self._table_detail, self._table_stmt_cache
        stmt = sqlalchemy.select(count())
        stmt = stmt.select_from(t.tbl)
        stmt, dic = where(stmt)
        result = c.execute(stmt, dic).scalar()
        return result

    def select_all(self, cols: RowColumns, c: Connection = None) -> List[Row]:
        t = self._table_detail

        def where(stmt: Select):
            return stmt, {}
        return self.select(cols=cols, where=where, c=c)

    def select_count_all(self, c: Connection = None) -> int:
        def where(stmt: Select):
            return stmt, {}
        return self.select_count(where=where, c=c)

    def select_types(self, c: Connection = None) -> Set[str]:
        if c is None:
            with self._begin_tran() as c:
                return self.select_types(c=c)

        t, sc = self._table_detail, self._table_stmt_cache
        stmt = sqlalchemy.select(sqlalchemy.distinct(t.col_type.col))
        stmt = stmt.select_from(t.tbl)
        result = c.execute(stmt).scalars().all()
        return set(result)


class TableInsertMixin(TableMixin, ABC):
    _log_insert = log.getChild("insert")

    def insert(self, rows: List[Row], c: Connection = None) -> List[Row]:
        if c is None:
            with self._begin_tran() as c:
                r = self.insert(rows=rows, c=c)
                c.commit()
                return r

        t, sc = self._table_detail, self._table_stmt_cache
        ver_default = _ROW_VERSION_DEFAULT
        uuid_gen = _ROW_ID_UUID_GEN
        rows_new: List[RowMutable] = []
        if t.col_id_type is ColumnIdType.INT:
            for row in rows:
                r = RowMutable.from_row(row)
                r.id = None
                r.ver = ver_default
                rows_new.append(r)
        elif t.col_id_type is ColumnIdType.UUID:
            for row in rows:
                r = RowMutable.from_row(row)
                if r.id is None: r.id = uuid_gen()
                r.ver = ver_default
                rows_new.append(r)
        rows = rows_new

        def build_stmt():
            st = t.tbl.insert()
            _cols = {
                t.col_ver.col: t.col_ver.bp_value,
                t.col_type.col: t.col_type.bp_value,
                t.col_meta.col: t.col_meta.bp_value,
                t.col_data.col: t.col_data.bp_value,
            }
            if t.col_id_type is ColumnIdType.UUID: _cols[t.col_id.col] = t.col_id.bp_value
            st = st.values(_cols)
            return st
        stmt = sc.get("insert", build_stmt)

        if t.col_id_type is ColumnIdType.INT:
            # TODO: Most DBs do not support multiple inserts while getting the IDs back, but support the ones that do
            for row in rows:
                dic = {t.col_ver.bp_value_name: row.ver, t.col_type.bp_value_name: row.type, t.col_meta.bp_value_name: row.meta, t.col_data.bp_value_name: row.data}
                r = c.execute(stmt, dic)
                row.id = r.inserted_primary_key[0]
                # r.close()

        elif t.col_id_type is ColumnIdType.UUID:
            dics = [{
                t.col_id.bp_value_name: row.id,
                t.col_ver.bp_value_name: row.ver,
                t.col_type.bp_value_name: row.type,
                t.col_meta.bp_value_name: row.meta,
                t.col_data.bp_value_name: row.data
            } for row in rows]
            c.execute(stmt, dics)

        else: raise NotImplementedError

        return [r.to_row() for r in rows]






class TableDeleteMixin(TableMixin, ABC):
    _log_delete = log.getChild("delete")

    @abstractmethod
    def select_single(self, cols: RowColumns, id: int, c: Connection = None) -> Row | None:
        raise NotImplementedError

    def delete(self, rows: List[Row], c: Connection = None):
        if c is None:
            with self._begin_tran() as c:
                r = self.delete(rows, c)
                c.commit()
                return r

        t, sc = self._table_detail, self._table_stmt_cache

        def build_stmt():
            st = t.tbl.delete()
            st = st.where(t.col_id.col == t.col_id.bp_where)
            st = st.where(t.col_ver.col == t.col_ver.bp_where)
            return st
        stmt = sc.get("delete", build_stmt)

        def check_delete_failed(_result, _row):
            _ct = _result.rowcount
            if _ct == 1: return
            _result_row = self.select_single(cols=RowColumns.ID | RowColumns.VER | RowColumns.TYPE, id=_row.id, c=c)
            if _result_row is None: return  # did not exist
            errormsg = f"DELETE failed {t.tbl.key}({t.col_id.name}={_row.id}, {t.col_ver.name}={_row.ver}, {t.col_type.name}={_result_row.type}"
            if _row.ver != _result_row.ver:
                raise ConcurrencyError(f"{errormsg} _row verion does not match database {_result_row.ver}", table_name=t.tbl.key, ver=_row.ver, actual_row_ver=_result_row.ver, id=_row.id)
            raise DatabaseError(f"{errormsg} for unknown reason  {stmt}  {stmt.compile().params}")

        for row in rows:
            dic = {t.col_id.bp_where_name: row.id, t.col_ver.bp_where_name: row.ver}
            result = c.execute(stmt, dic)
            check_delete_failed(result, row)
            # result.close()

    def delete_types(self, types: List[str], c: Connection = None) -> int:
        if c is None:
            with self._begin_tran() as c:
                r = self.delete_types(types=types, c=c)
                c.commit()
                return r

        t, sc = self._table_detail, self._table_stmt_cache

        st = t.tbl.delete()
        st = st.where(t.col_type.col == t.col_type.bp_where)

        count = 0
        for type in types:
            dic = {t.col_type.bp_where_name: type}
            result = c.execute(st, dic)
            count += result.rowcount
        return count


    def delete_all(self, c: Connection = None) -> int:
        if c is None:
            with self._begin_tran() as c:
                r = self.delete_all(c=c)
                c.commit()
                return r

        t, sc = self._table_detail, self._table_stmt_cache

        st = t.tbl.delete()
        result = c.execute(st)
        return result.rowcount


class TableUpdateMixin(TableMixin, ABC):
    _log_update = log.getChild("update")

    @abstractmethod
    def select_single(self, cols: RowColumns, id: int, c: Connection = None) -> Row | None:
        raise NotImplementedError

    def update(self, rows: List[Row], c: Connection = None, populate_missing_fields=True) -> List[Row]:
        if c is None:
            with self._begin_tran() as c:
                r = self.update(rows=rows, c=c, populate_missing_fields=populate_missing_fields)
                c.commit()
                return r

        t, sc = self._table_detail, self._table_stmt_cache
        ver_step = _ROW_VERSION_STEP
        rows: List[RowMutable] = [RowMutable.from_row(row) for row in rows]

        def build_stmt():
            st = t.tbl.update()
            st = st.where(t.col_id.col == t.col_id.bp_where)
            st = st.where(t.col_ver.col == t.col_ver.bp_where)
            st = st.values({
                t.col_ver.col: t.col_ver.bp_value,
                t.col_type.col: t.col_type.bp_value,
                t.col_meta.col: t.col_meta.bp_value,
                t.col_data.col: t.col_data.bp_value,
            })
            return st
        stmt = sc.get("update", build_stmt)

        def check_update_failed(_result, _row):
            _ct = _result.rowcount
            if _ct == 1: return
            _result_row = self.select_single(cols=RowColumns.ID | RowColumns.VER | RowColumns.TYPE, id=_row.id, c=c)
            errormsg = f"UPDATE failed {t.tbl.key}({t.col_id.name}={_row.id}, {t.col_ver.name}={_row.ver}"
            if _result_row is not None: errormsg += f", {t.col_type.name}={_result_row.type}"
            errormsg += ")"
            if _result_row is None:
                raise IdError(f"{errormsg} id {_row.id} does not exist", table_name=t.tbl.key, id=_row.id)
            if _row.ver != _result_row.ver:
                raise ConcurrencyError(f"{errormsg} _row verion does not match database {_result_row.ver}", table_name=t.tbl.key, ver=_row.ver, actual_row_ver=_result_row.ver, id=_row.id)
            raise DatabaseError(f"{errormsg} for unknown reason  {stmt}  {stmt.compile().params}")

        for row in rows:
            row_ver_new = row.ver + ver_step
            dic = {
                t.col_id.bp_where_name: row.id,
                t.col_ver.bp_where_name: row.ver,

                t.col_ver.bp_value_name: row_ver_new,
                t.col_type.bp_value_name: row.type,
                t.col_meta.bp_value_name: row.meta,
                t.col_data.bp_value_name: row.data
            }
            result = c.execute(stmt, dic)
            check_update_failed(result, row)
            # result.close()
            if populate_missing_fields:
                flags = RowColumns.ID | RowColumns.VER
                if row.type is None: flags = flags | RowColumns.TYPE
                if row.meta is None: flags = flags | RowColumns.META
                if row.data is None: flags = flags | RowColumns.DATA
                if flags != RowColumns.ID | RowColumns.VER:
                    result_row = self.select_single(cols=flags, id=row.id, c=c)
                    if row.type is None: row.type = result_row.type
                    if row.meta is None: row.meta = result_row.meta
                    if row.data is None: row.data = result_row.data
            row.ver = row_ver_new

        return [r.to_row() for r in rows]




class Table(TableSelectMixin, TableInsertMixin, TableUpdateMixin, TableDeleteMixin):

    def _begin_tran(self) -> Connection:
        return self._begin_tran_factory()

    @property
    def _table_detail(self) -> TableDetail:
        return self._table_detail_v

    @property
    def table_detail(self) -> TableDetail:
        return self._table_detail_v

    @property
    def _table_stmt_cache(self) -> TableMixin._StatementCache:
        return self._table_stmt_cache_v

    def __init__(self, begin_tran_factory: Callable[[], Connection], table_detail: TableDetail):
        super().__init__()
        self._begin_tran_factory = begin_tran_factory
        self._table_detail_v = table_detail
        self._table_stmt_cache_v = TableMixin._StatementCache()


















def test():
    count = 10000

    class TestSetup(NamedTuple):
        rows: List[Row]
        chunk_size: int
        column_id_type: ColumnIdType
        database_uri: str
        table_name: str

    class RandomDataGen:
        def __init__(self, col_id_type: ColumnIdType):
            import itertools
            self._types = itertools.cycle(random_verbs(10))
            self._col_id_type = col_id_type

        @property
        def next_type(self):
            return next(self._types).capitalize() if random_bool() else next(self._types)

        @property
        def next_meta(self):
            return " ".join(random_nouns(random_int(2, 50)))

        @property
        def next_data(self):
            return " ".join(random_names(random_int(2, 200)))

        @property
        def next_id(self):
            if self._col_id_type is ColumnIdType.INT: return None
            return _ROW_ID_UUID_GEN()

        def next_row(self, include_id=False):
            id = self.next_id if include_id else None
            return Row(id=id, ver=1, type=self.next_type, meta=self.next_meta, data=self.next_data)

        def gen_rows(self, row_count: int, include_id=False):
            return [self.next_row(include_id=include_id) for _ in range(row_count)]

    def fmtint(_int):
        if isinstance(_int, Sequence):
            _int = len(_int)
        return "{:,}".format(_int)

    import config
    config.init_logging()
    # main()

    column_id_types = [ColumnIdType.INT, ColumnIdType.UUID]
    for column_id_type in column_id_types:
        dg = RandomDataGen(col_id_type=column_id_type)
        rows = dg.gen_rows(count, include_id=True)
        ts = TestSetup(
            rows=rows,
            chunk_size=int(len(rows) / 10),
            column_id_type=column_id_type,
            database_uri=config.Config.DATABASE_URI,
            table_name=config.Config.DATABASE_TABLE,
        )
        tdef = TableDefinition(tbl_name=ts.table_name, col_id_type=ts.column_id_type)
        db = Database(ts.database_uri, [tdef])
        tbl = db.tables[ts.table_name]
        db.tables_create()

        rows_insert = []
        with Profiler(name=f"INSERT  [{ts.column_id_type}]  {fmtint(ts.rows)}"):
            for rows_chunked in chunks(ts.rows, ts.chunk_size):
                rows_result = tbl.insert(rows_chunked)
                rows_insert.extend(rows_result)
        print(f"Inserted {len(rows_insert)}\n")

        rows_select_cols_possible_all = [RowColumns.ID, RowColumns.VER, RowColumns.TYPE, RowColumns.META, RowColumns.DATA]
        rows_select_cols_possible = set([x for x in [RowColumns.combine(tpl) for tpl in powerset(rows_select_cols_possible_all)] if x is not None])
        rows_select_cols_possible = sorted(rows_select_cols_possible, key=lambda x: x.value)
        for rows_select_col in rows_select_cols_possible:
            with Profiler(name=f"SELECT_ALL  {rows_select_col}  [{ts.column_id_type}]  {fmtint(ts.rows)}"):
                rows_result = tbl.select_all(rows_select_col)
                print(f" - Retrieved: {fmtint(rows_result)}")
            print()

        with Profiler(name=f"SELECT_TYPES  [{ts.column_id_type}]  {fmtint(rows_insert)}"):
            types = tbl.select_types()
            print(f"Types[{len(types)}]: {types}")
        print()

        assert len(rows_insert) == len(ts.rows)
        rows_update_to_update = []
        for row, row_new in zip(rows_insert, dg.gen_rows(len(rows_insert), include_id=False)):
            rows_update_to_update.append(Row(row.id, row.ver, row_new.type, row_new.meta, row_new.data))
        assert len(rows_update_to_update) == len(rows_insert)
        rows_update = []
        with Profiler(name=f"UPDATE  [{ts.column_id_type}]  {fmtint(rows_update)}"):
            for rows_chunked in chunks(rows_update_to_update, ts.chunk_size):
                rows_update.extend(tbl.update(rows_chunked))
        assert len(rows_update) == len(ts.rows)
        print(f"Updated {len(rows_update)}\n")
        for row_insert, row_update_to_update, row_update in zip(rows_insert, rows_update_to_update, rows_update):
            try:
                assert row_insert.id == row_update_to_update.id
                assert row_insert.id == row_update.id
                assert row_insert.ver == row_update_to_update.ver
                assert (row_insert.ver + 1) == row_update.ver
            except AssertionError:
                print(f"row_insert:  {row_insert}")
                print(f"row_update:  {row_update}")
                raise

        with Profiler(name=f"DELETE  [{ts.column_id_type}]  {fmtint(rows_update)}"):
            for rows_chunked in chunks(rows_update, ts.chunk_size):
                tbl.delete(rows_chunked)
        assert tbl.select_count_all() == 0

        delete_type_rows = []
        for type in ["a a a", "B B B"]:
            for r in dg.gen_rows(100):
                delete_type_rows.append(Row(None, None, type, r.meta, r.data))
        assert tbl.select_count_all() == 0
        tbl.insert(delete_type_rows)
        assert tbl.select_count_all() == 200
        assert 100 == tbl.delete_types(["a a a"])
        assert tbl.select_count_all() == 100
        assert 0 == tbl.delete_types(["c c c"])
        assert tbl.select_count_all() == 100
        assert 100 == tbl.delete_types(["B B B"])
        assert tbl.select_count_all() == 0

        delete_all_rows = []
        for type in ["a a a", "B B B"]:
            for r in dg.gen_rows(100):
                delete_all_rows.append(Row(None, None, type, r.meta, r.data))
        assert tbl.select_count_all() == 0
        tbl.insert(delete_all_rows)
        assert tbl.select_count_all() == 200
        assert 200 == tbl.delete_all()
        assert tbl.select_count_all() == 0



def main():
    test()

if __name__ == "__main__":
    main()
