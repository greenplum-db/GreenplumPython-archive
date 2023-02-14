from os import environ
from typing import Any, Dict

import pytest

import greenplumpython as gp
import greenplumpython.pandas as pd


@pytest.fixture(autouse=True)
def init_namepsace(doctest_namespace: Dict[str, Any]):
    # for the connection both work for GitHub Actions and concourse
    host = "localhost"
    dbname = environ.get("TESTDB", "postgres")
    user = environ.get("PGUSER")
    password = environ.get("PGPASSWORD")

    db = gp.database(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
    )
    con = f"postgresql://{host}/{dbname}"
    doctest_namespace["db"] = db
    doctest_namespace["con"] = con
    doctest_namespace["gp"] = gp
    doctest_namespace["pd"] = pd
    yield db
    db.close()


@pytest.fixture(autouse=True)
def init_gp_config():
    gp.config.print_sql = False
