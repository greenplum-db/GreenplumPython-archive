from os import environ

import pytest

import greenplumpython as gp


@pytest.fixture()
def db():
    # for the connection both work for GitHub Actions and concourse
    db = gp.database(
        params={
            "host": environ["PGHOST"],
            "dbname": environ["TESTDB"],
            "user": environ["PGUSER"],
            "password": environ["PGPASSWORD"],
        }
    )
    db._execute(
        """
        CREATE EXTENSION IF NOT EXISTS plpython3u;
        CREATE EXTENSION IF NOT EXISTS vector;
        """,
        has_results=False,
    )
    db._execute(
        """
        DROP SCHEMA IF EXISTS test CASCADE;
        CREATE SCHEMA test;
        """,
        has_results=False,
    )
    yield db
    db.close()


@pytest.fixture()
def con():
    host = "localhost"
    dbname = environ["TESTDB"]
    con = f"postgresql://{host}/{dbname}"
    yield con


gp.config.print_sql = True
