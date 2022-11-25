from os import environ

import pytest

import greenplumpython as gp
from tests import db


def test_db():
    db = gp.database(
        host="localhost",
        dbname=environ.get("TESTDB", "gpadmin"),
        user=environ.get("PGUSER"),
        password=environ.get("PGPASSWORD"),
    )
    result = db.execute("SELECT version()")
    for row in result:
        assert "Greenplum" in row["version"] or row["version"].startswith("PostgreSQL")
    db.close()


def test_db_get_table(db: gp.Database):
    rows = [(1,) for _ in range(10)]
    gp.to_table(rows, db=db, column_names=["val"]).save_as("numbers", temp=True)
    numbers = db.table("numbers")
    assert sum(row["val"] for row in numbers) == 10


def test_set_option():
    assert gp.options_dict["sql_on"] is False
    gp.set_option("sql_on", True)
    assert gp.options_dict["sql_on"] is True
    gp.set_option("sql_on", False)
    assert gp.options_dict["sql_on"] is False
    with pytest.raises(Exception) as exc_info:
        gp.set_option("mode", 0)
    assert str(exc_info.value) == 'Option named "mode" not exists.'
