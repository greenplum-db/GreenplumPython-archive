from os import environ

import pytest

import greenplumpython as gp


@pytest.fixture()
def db():
    db = gp.database(
        host="localhost",
        dbname=environ.get("POSTGRES_DB", "gpadmin"),
        user=environ.get("POSTGRES_USER"),
        password=environ.get("POSTGRES_PASSWORD"),
    )
    yield db
    db.close()
