from os import environ

import pytest

import greenplumpython as gp
from . import db


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


def test_db_get_dataframe(db: gp.Database):
    rows = [(1,) for _ in range(10)]
    db.create_dataframe(rows=rows, column_names=["val"]).save_as(
        "numbers", temp=True, column_names=["val"]
    )
    numbers = db.create_dataframe(table_name="numbers")
    assert sum(row["val"] for row in numbers) == 10


def test_print_sql():
    assert gp.config.print_sql is False
    gp.config.print_sql = True
    assert gp.config.print_sql is True
    gp.config.print_sql = False
    assert gp.config.print_sql is False
