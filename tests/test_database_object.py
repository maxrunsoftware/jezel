import dataclasses
from dataclasses import dataclass

import pytest

import config
from database import ColumnIdType, Database, TableDefinition
from database_object import DatabaseObject, DatabaseObjectDatabase, SerializableBase, SerializableReader, SerializableWriter

p = pytest.mark.parametrize

config.init_logging()

_db = Database(db_uri="sqlite+pysqlite:///:memory:")

_test_obj_databases = {
    "int": DatabaseObjectDatabase(_db.add_table(TableDefinition(tbl_name="test_table_dbo_int", col_id_type=ColumnIdType.INT))),
    "uuid": DatabaseObjectDatabase(_db.add_table(TableDefinition(tbl_name="test_table_dbo_uuid", col_id_type=ColumnIdType.UUID))),
}


@dataclass
class TestObjectPerson(SerializableBase):
    first_name: str | None = None
    last_name: str | None = None
    city: str | None = None
    age: int | None = None

    def serialize_write(self, w: SerializableWriter):
        for f in dataclasses.fields(self):
            w.put(f.name, getattr(self, f.name))

    def deserialize_read(self, r: SerializableReader):
        for f in dataclasses.fields(self):
            setattr(self, f.name, r.get(f.name, f.type))


@dataclass
class TestObjectPersonA(TestObjectPerson):
    pass


@dataclass
class TestObjectPersonB(TestObjectPerson):
    pass


@dataclass
class TestObjectPersonC(TestObjectPerson):
    pass


@pytest.fixture(scope="function")
def db(request):
    for t in _test_obj_databases.values():
        t.table.delete_all()
        cnt = t.table.select_count_all()
        if cnt: raise Exception(f"Could not delete all rows from table {t.table.table_detail.tbl_name}")

    yield _test_obj_databases[request.param]

    for t in _test_obj_databases.values():
        t.table.delete_all()
        cnt = t.table.select_count_all()
        if cnt: raise Exception(f"Could not delete all rows from table {t.table.table_detail.tbl_name}")


@p("db", ["int", "uuid"], indirect=True)
class TestDatabaseObjectDatabase:

    def test_save(self, db: DatabaseObjectDatabase):
        assert len(db.get_all()) == 0
        items = [DatabaseObject.create_from_serializable(db, item) for item in [
            TestObjectPersonA("Avril", "Applegate", "Anaheim", 1),
            TestObjectPersonB("Ben", "Bears", "Baltimore", 2),
            TestObjectPersonB("Brad", "Boing", "Boston", 22),
            TestObjectPersonC("Carl", "Caldwell", "Carre", 3),
            TestObjectPersonC("Cerene", "Cellos", "Cleveland", 3),
            TestObjectPersonC("Cillo", "Cilpate", "Condy", 33),
        ]]
        for item in items:
            assert item.id is None
            assert item.ver is None

        for item in items:
            item.save()

        for item in items:
            assert item.id is not None
            assert item.ver is not None

        items_returned = db.get_all()
        assert len(items_returned) == len(items)
        items_returned = sorted(items_returned, key=lambda x: x.obj.first_name)
        for old, new in zip(items, items_returned):
            assert old.id == new.id
            assert old.ver == new.ver
            assert len(old.tags) == len(new.tags)
            assert old.obj.first_name == new.obj.first_name
            assert old.obj.last_name == new.obj.last_name
            assert old.obj.city == new.obj.city
            assert old.obj.age == new.obj.age

    def test_types(self, db: DatabaseObjectDatabase):
        assert len(db.get_all()) == 0
        items = [DatabaseObject.create_from_serializable(db, item) for item in [
            TestObjectPersonA("Avril", "Applegate", "Anaheim", 1),
            TestObjectPersonB("Ben", "Bears", "Baltimore", 2),
            TestObjectPersonB("Brad", "Boing", "Boston", 22),
            TestObjectPersonC("Carl", "Caldwell", "Carre", 3),
            TestObjectPersonC("Cerene", "Cellos", "Cleveland", 3),
            TestObjectPersonC("Cillo", "Cilpate", "Condy", 33),
        ]]
        for item in items:
            item.save()
        assert db.get_types() == {type(o.obj) for o in items}

    def test_tags(self, db: DatabaseObjectDatabase):
        item = DatabaseObject.create_from_serializable(db, TestObjectPersonA("Avril", "Applegate", "Anaheim", 1))
        assert len(item.tags) == 0
        item.tags["foo"] = "bar"
        item.save()
        assert len(item.tags) == 1
        item_result = db.get_all()[0]
        assert item.tags == item_result.tags

        item.tags["hello"] = "world"
        item.save()
        assert len(item.tags) == 2
        item_result = db.get_all()[0]
        assert item_result.tags == item.tags

        item.tags.clear()
        item.save()
        assert len(item.tags) == 0
        item_result = db.get_all()[0]
        assert item_result.tags == item.tags

    def test_save_load_performance(self, db: DatabaseObjectDatabase):
        assert len(db.get_all()) == 0
        items = [DatabaseObject.create_from_serializable(db, item) for item in [
            TestObjectPersonA("Avril", "Applegate", "Anaheim", 1),
            TestObjectPersonB("Ben", "Bears", "Baltimore", 2),
            TestObjectPersonB("Brad", "Boing", "Boston", 22),
            TestObjectPersonC("Carl", "Caldwell", "Carre", 3),
            TestObjectPersonC("Cerene", "Cellos", "Cleveland", 3),
            TestObjectPersonC("Cillo", "Cilpate", "Condy", 33),
        ]]
        for item in items:
            assert item.id is None
            assert item.ver is None

        for item in items:
            item.save()

        for item in items:
            assert item.id is not None
            assert item.ver is not None

        items_returned = db.get_all()
        assert len(items_returned) == len(items)
        items_returned = sorted(items_returned, key=lambda x: x.obj.first_name)
        for old, new in zip(items, items_returned):
            assert old.id == new.id
            assert old.ver == new.ver
            assert len(old.tags) == len(new.tags)
            assert old.obj.first_name == new.obj.first_name
            assert old.obj.last_name == new.obj.last_name
            assert old.obj.city == new.obj.city
            assert old.obj.age == new.obj.age
