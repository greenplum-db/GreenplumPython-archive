from os import environ

import pytest

import greenplumpython as gp


@pytest.fixture()
def db():
    # for the connection both work for GitHub Actions and concourse
    db = gp.database(
        host="localhost",
        dbname=environ.get("TESTDB", "gpadmin"),
        user=environ.get("PGUSER"),
        password=environ.get("PGPASSWORD"),
    )
    yield db
    db.close()
