from os import environ

import greenplumpython as gp


def test_db():
    db = gp.database(
        host="localhost",
        dbname="gpadmin",
        user=environ.get("POSTGRES_USER"),
        password=environ.get("POSTGRES_PASSWORD"),
    )
    result = db.execute("SELECT version()")
    for row in result:
        assert "Greenplum" in row["version"] or row["version"].startswith("PostgreSQL")
    db.close()
