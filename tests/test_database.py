from typing import Sequence

import pytest

import config
import database
from database import ColumnIdType, Database, Row, TableDefinition
from utils import int_prefix

p = pytest.mark.parametrize

config.init_logging()

CACHED_ROW_COUNT = 100000


def gen_rows(num):
    padding_size = len(str(num - 1))
    pfx = ("a_", "b_", "c_")
    rows = []
    for i in range(num):
        sfx = int_prefix(i, padding_size)
        rows.append(Row(None, None, pfx[0] + sfx, pfx[1] + sfx, pfx[2] + sfx))
    return rows


ROWS = gen_rows(CACHED_ROW_COUNT)

_db = Database(db_uri="sqlite+pysqlite:///:memory:")
_test_tables_dict = {
    "int": _db.add_table(TableDefinition(tbl_name="test_table_int", col_id_type=ColumnIdType.INT)),
    "uuid": _db.add_table(TableDefinition(tbl_name="test_table_uuid", col_id_type=ColumnIdType.UUID)),
}


@pytest.fixture(scope="function")
def table(request):
    for t in _test_tables_dict.values():
        t.delete_all()
        cnt = t.select_count_all()
        if cnt: raise Exception(f"Could not delete all rows_original from table {t.table_detail.tbl_name}")

    yield _test_tables_dict[request.param]

    for t in _test_tables_dict.values():
        t.delete_all()
        cnt = t.select_count_all()
        if cnt: raise Exception(f"Could not delete all rows_original from table {t.table_detail.tbl_name}")


@pytest.fixture(scope="function")
def rows(request):
    return ROWS[:request.param]


@p("rows", [1, 10, 100, 1000], indirect=True)
@p("action", ["single", "bulk"])
@p("table", ["int", "uuid"], indirect=True)
class TestInsert:
    def test_insert(self, table, rows: Sequence[Row], action):
        if action == "single":
            rows_result = [table.insert([r])[0] for r in rows]
        elif action == "bulk":
            rows_result = table.insert(rows)
        else:
            raise NotImplementedError
        rows_old, rows_new = rows, rows_result
        assert table.select_count_all() == len(rows_old)
        assert len(set([r.id for r in rows_new])) == len(rows_old)
        for old, new in zip(rows_old, rows_new):
            assert new.id is not None
            assert new.ver == database._ROW_VERSION_DEFAULT
            assert new.dsmall == old.dsmall
            assert new.dmedium == old.dmedium
            assert new.dlarge == old.dlarge


@p("rows", [1, 10, 100, 1000], indirect=True)
@p("table", ["int", "uuid"], indirect=True)
@p("action", ["single", "bulk"])
class TestUpdate:
    def test_update(self, table, rows: Sequence[Row], action):
        rows_original = table.insert(rows)
        rows_modified = [Row(r.id, r.ver, f"XXX_{i}", f"YYY_{i}", f"ZZZ_{i}") for i, r in enumerate(rows_original)]
        assert len(rows_modified) == len(rows_original)

        if action == "single":
            rows_updated = [table.update([r])[0] for r in rows_modified]
        elif action == "bulk":
            rows_updated = table.update(rows_modified)
        else:
            raise NotImplementedError
        assert len(rows_updated) == len(rows_original)

        rows_queried = table.select_all()
        assert len(rows_queried) == len(rows_original)

        for original, modified, updated, queried in zip(rows_original, rows_modified, rows_updated, rows_queried):
            assert modified.id == original.id
            assert updated.id == original.id
            assert queried.id == original.id

            assert modified.ver == original.ver
            assert updated.ver == (original.ver + database._ROW_VERSION_STEP)
            assert queried.ver == (original.ver + database._ROW_VERSION_STEP)

            assert modified.dsmall != original.dsmall
            assert modified.dmedium != original.dmedium
            assert modified.dlarge != original.dlarge

            assert updated.dsmall == modified.dsmall
            assert updated.dmedium == modified.dmedium
            assert updated.dlarge == modified.dlarge

            assert queried.dsmall == modified.dsmall
            assert queried.dmedium == modified.dmedium
            assert queried.dlarge == modified.dlarge


@p("rows", [1, 10, 100, 1000], indirect=True)
@p("action", ["single", "bulk"])
@p("table", ["int", "uuid"], indirect=True)
class TestDelete:
    def test_delete_all(self, table, rows: Sequence[Row], action):
        rows_original = table.insert(rows)
        assert table.select_count_all() == len(rows)
        if action == "single":
            for row in rows_original:
                table.delete([row])
        elif action == "bulk":
            table.delete(rows_original)
        else:
            raise NotImplementedError
        assert table.select_count_all() == 0
