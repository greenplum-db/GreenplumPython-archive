import os

import pytest

from greenplumpython.core.gp_connection import GPConnection
from greenplumpython.core.gpdatabase import GPDatabase
from greenplumpython.tests.testdb import db, host, password, port, user


@pytest.fixture(scope="session", autouse=True)
def db_conn():
    # Will be executed before the first test
    connection = GPConnection()
    sql_conn = connection.connect(host, port, db, user, password)
    yield sql_conn
    # Will be executed after the last test
    connection.close(1)


def test_get_table(db_conn):
    data = GPDatabase(db_conn)
    frame = data.get_table("employee", "public")
    assert frame.table_metadata.name == "employee"
    assert frame.table_metadata.signature[1]["payment"] == "int4"


def test_execute_query(db_conn):
    data = GPDatabase(db_conn)
    dataq = data.execute_query("select name from employee")
    assert len(dataq) > 0


def test_has_table(db_conn):
    data = GPDatabase(db_conn)
    result = data.check_table_if_exist("employee", "public")
    assert result == True
    result = data.check_table_if_exist("employee1", "public")
    assert result == False
