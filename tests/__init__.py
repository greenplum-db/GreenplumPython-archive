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
    db._execute("DROP SCHEMA IF EXISTS test CASCADE; CREATE SCHEMA test;", has_results=False)
    yield db
    db.close()


gp.config.print_sql = True
