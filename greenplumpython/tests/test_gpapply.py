from greenplumpython.gpapply import gpApply
from greenplumpython.tests.testdb import host, port, db, user, password
from greenplumpython.core.gp_connection import GPConnection
from greenplumpython.core.gpdatabase import GPDatabase
from greenplumpython.core import sql
from greenplumpython.core.gptable_metadata import GPTableMetadata

import pytest
@pytest.fixture(scope='session', autouse=True)
def db_conn():
    # Will be executed before the first test
    sql_conn = GPConnection().connect(host, port, db, user, password)
    yield sql_conn
    # Will be executed after the last test
    sql_conn.close()

def recsum(a, b):
    x = dict()
    x['a'] = a+b
    return x


def aqi_vs_temp(id, city, p_date, temp, humidity, aqi):
    a = aqi/temp
    return (city, a)

def test_gpapply_case1(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("basic", "public")
    output_col = [{"a":"int4"}]
    output = GPTableMetadata("basic_output", output_col, 'randomly')
    index = "a"
    gpApply(table, recsum, data, output)
    res = data.execute_query("select * from basic_output")
    assert res.iat[0,0] ==4 or res.iat[1,0] == 4

def test_gpapply_case2(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("weather", "public")
    output_columns = [{"city": "text"},{"a": "float"}]
    output = GPTableMetadata("weather_output", output_columns, 'randomly')
    gpApply(table, aqi_vs_temp, data, output)
    res = data.execute_query("select * from weather_output")
    assert res.iat[0,1] == 13.0 or res.iat[0,1] == 6.0
