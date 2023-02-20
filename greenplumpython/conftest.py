from os import environ
from typing import Any, Dict

import psycopg2
import pytest

import greenplumpython as gp
import greenplumpython.pandas as pd

_DBHOST = environ.get("PGHOST", "localhost")
_DBPORT = environ.get("PGPORT", 5432)
_DBNAME = environ.get("TESTDB", "gpadmin")
_DBUSER = environ.get("PGUSER", "gpadmin")
_DBPSWD = environ.get("PGPASSWORD")


@pytest.fixture(autouse=True)
def init_namepsace(doctest_namespace: Dict[str, Any]):
    # for the connection both work for GitHub Actions and concourse
    uri = f"postgresql://{_DBHOST}/{_DBNAME}"
    db = gp.database(uri)

    conn = psycopg2.connect(uri)
    conn.set_session(autocommit=True)

    cursor = conn.cursor()

    doctest_namespace["db"] = db
    doctest_namespace["con"] = uri
    doctest_namespace["gp"] = gp
    doctest_namespace["pd"] = pd
    doctest_namespace["cursor"] = cursor
    doctest_namespace["db_host"] = _DBHOST
    doctest_namespace["db_port"] = _DBPORT
    doctest_namespace["db_name"] = _DBNAME
    doctest_namespace["db_user"] = _DBUSER
    doctest_namespace["db_password"] = _DBPSWD

    yield db, cursor, conn
    db.close()
    cursor.close()
    conn.close()


@pytest.fixture(autouse=True)
def init_gp_config():
    gp.config.print_sql = False
