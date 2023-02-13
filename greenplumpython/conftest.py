from os import environ
from typing import Any, Dict

import pytest

import greenplumpython as gp


@pytest.fixture(autouse=True)
def init_namepsace(doctest_namespace: Dict[str, Any]):
    # for the connection both work for GitHub Actions and concourse
    db = gp.database(
        host="localhost",
        dbname=environ.get("TESTDB", "gpadmin"),
        user=environ.get("PGUSER"),
        password=environ.get("PGPASSWORD"),
    )
    doctest_namespace["db"] = db
    doctest_namespace["gp"] = gp
    yield db
    db.close()


@pytest.fixture(autouse=True)
def init_gp_config():
    gp.config.print_sql = False
