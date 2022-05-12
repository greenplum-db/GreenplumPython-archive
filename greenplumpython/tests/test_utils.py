import pytest

from greenplumpython.core.gp_connection import GPConnection
from greenplumpython.core.gpdatabase import GPDatabase
from greenplumpython.tests.testdb import db, host, password, port, user
from greenplumpython.utils.apply_utils import createTypeFunc, randomStringType


@pytest.fixture(scope="session", autouse=True)
def db_conn():
    # Will be executed before the first test
    sql_conn = GPConnection().connect(host, port, db, user, password)
    yield sql_conn
    # Will be executed after the last test
    sql_conn.close()


def test_create_type(db_conn):
    data = GPDatabase(db_conn)
    typeName = randomStringType()
    output_columns = [{"x": "int"}, {"y": "text"}, {"z": "float"}]
    type_sql = createTypeFunc(output_columns, typeName)
    data.execute(type_sql)
    df = data.execute_query("select typname from pg_type where typname=lower('%s');" % typeName)
    assert df.shape[0] == 1
