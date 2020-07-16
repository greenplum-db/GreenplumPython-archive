from greenplumpython.core.gp_connection import GPConnection
from greenplumpython.core.gpdatabase import GPDatabase
import os
import pytest

host = os.getenv('GPHOST')
if host is None or host is '':
    host = "localhost"
port = os.getenv('GPPORT')
if port is None or port is '':
    port = 6000
else:
    port = int(port)
db = os.getenv('GPDATABASE')
if db is None or db is '':
    db = "gppython"
user = os.getenv('GPUSER')
if user is None or user is '':
    user = "gpadmin"
password = os.getenv('GPPASSWORD')

@pytest.fixture(scope='session', autouse=True)
def test_database_conn_success():
    connection = GPConnection()
    conn = connection.connect(host, port, db, user, password)
    assert conn is not None

def test_database_conn_fail():
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
