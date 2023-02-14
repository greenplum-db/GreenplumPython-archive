from os import environ
from typing import Any, Dict

import psycopg2
import pytest

import greenplumpython as gp

_DBHOST = environ.get("PGHOST", "localhost")
_DBPORT = environ.get("PGPORT", 6000)
_DBNAME = environ.get("TESTDB", "gpadmin")
_DBUSER = environ.get("PGUSER", "gpadmin")
_DBPSWD = environ.get("PGPASSWORD")


@pytest.fixture(autouse=True)
def init_namepsace(doctest_namespace: Dict[str, Any]):
    # for the connection both work for GitHub Actions and concourse
    db = gp.database(host=_DBHOST, port=_DBPORT, dbname=_DBNAME, user=_DBUSER, password=_DBPSWD)

    conn = psycopg2.connect(
        host=_DBHOST, port=_DBPORT, user=_DBUSER, password=_DBPSWD, database=_DBNAME
    )
    conn.set_session(autocommit=True)

    cursor = conn.cursor()

    doctest_namespace["db"] = db
    doctest_namespace["gp"] = gp
    doctest_namespace["cursor"] = cursor

    yield db, cursor, conn
    db.close()
    cursor.close()
    conn.close()


@pytest.fixture(autouse=True)
def init_gp_config():
    gp.config.print_sql = False
