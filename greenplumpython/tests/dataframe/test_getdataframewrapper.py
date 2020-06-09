
from greenplumpython.connection.gp import GPConnection
from greenplumpython.dataframe import sql
import os
import pytest

host = os.getenv('GPHOST')
if host is None or host is '':
    host = "localhost"
db = os.getenv('GPDATABASE')
if db is None or db is '':
    db = "test"
user = os.getenv('GPUSER')
if user is None or user is '':
    user = "gpadmin"
password = os.getenv('GPPASSWORD')

@pytest.fixture(scope='session', autouse=True)
def db_conn():
    # Will be executed before the first test
    sql_conn = GPConnection(host, db, user, password).get_connection()
    yield sql_conn
    # Will be executed after the last test
    sql_conn.close()

def test_get_dataframe_from_table(db_conn):
    data1 = sql.get_dataframe_from_table("employee", db_conn)
    assert data1.empty == False

def test_get_dataframe_wrapper_from_table(db_conn):
    data = sql.get_dataframe_wrapper_from_table("employee",db_conn)
    assert data.empty == True

def test_get_dataframe_wrapper_from_sql(db_conn):
    dataq = sql.get_dataframe_wrapper_from_sql("select name from employee", db_conn)
    assert dataq.empty == True