from greenplumpython.core.gp_connection import GPConnection
from greenplumpython.core.gpdatabase import GPDatabase
from greenplumpython.tests.testdb import host, port, db, user, password
import os
import pytest

@pytest.fixture(scope='session', autouse=True)
def test_database_conn_success():
    connection = GPConnection()
    conn = connection.connect(host, port, db, user, password)
    assert conn is not None

def test_database_conn_fail():
    print(host)
    with pytest.raises(Exception) as e:
        connection = GPConnection()
        assert connection.connect(host, port, 'no_exist_db', user, password)

def test_database_conn_list():
    connection = GPConnection()
    conn1 = connection.connect(host, port, db, user, password)
    assert conn1 is not None
    conn2 = connection.connect(host,port, db, user, password)
    assert conn2 is not None
    conn_temp = connection.get_connection(1)
    assert conn_temp is not None

def test_database_conn_close():
    connection = GPConnection()
    conn = connection.connect(host,port, db, user, password)
    conn2 = connection.connect(host,port, db, user, password)
    assert len(connection.connection_pool) == 2
    connection.close(1)
    assert len(connection.connection_pool) == 1

def test_database_conn_id_not_exist():
    with pytest.raises(Exception) as e:
        connection = GPConnection()
        connection.connect(host,port, db, user, password)
        assert connection.get_connection(-1)
