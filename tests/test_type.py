import pytest

import greenplumpython as gp


@pytest.fixture
def db():
    db = gp.database(host="localhost", dbname="gpadmin")
    yield db
    db.close()


def test_type_cast(db: gp.Database):
    rows = [(i,) for i in range(10)]
    series = gp.values(rows, db, column_names=["val"]).save_as("series")
    regclass = gp.get_type("regclass", db)
    table_name = regclass(series["tableoid"]).rename("table_name")
    for row in table_name.to_table().fetch():
        assert row["table_name"] == "series"
