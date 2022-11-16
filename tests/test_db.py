from os import environ

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


def test_dg_get_table(db: gp.Database):
    rows = [(1,) for _ in range(10)]
    db.make_table(rows, column_names=["val"]).save_into("numbers", temp=True)
    numbers = db.open_table("numbers")
    assert sum(row["val"] for row in numbers) == 10
