from greenplumpython.core.gpapply import gpApply
from greenplumpython.tests.testdb import host, port, db, user, password
from greenplumpython.core.gp_connection import GPConnection
from greenplumpython.core.gpdatabase import GPDatabase
from greenplumpython.core import sql
from greenplumpython.core.gptable_metadata import GPTableMetadata
import time
from datetime import timedelta


def py_func_1(a, b):
    x = dict()
    result = 0
    for j in range(1, 100):
        result = a
        for i in range(1, 1000000):
            result = result + b

    x["a"] = result
    return x


def py_func_2(a, b):
    x = dict()
    result = 0
    for i in range(1, 1000000):
        result = 0
        result = a / b
    x["a"] = result
    return x


if __name__ == "__main__":
    db_conn = GPConnection().connect(host, port, db, user, password)
    data = GPDatabase(db_conn)
    table = data.get_table("basic", "public")
    output_col = [{"a": "int4"}]
    output = GPTableMetadata("basic_output", output_col, "randomly")
    start_time = time.monotonic()

    gpApply(table, py_func_1, data, output, "plc_python3_shared", "plcontainer", True)
    end_time = time.monotonic()
    print("plus container", timedelta(seconds=end_time - start_time))
    res = data.execute_query("select * from basic_output")
    print(res)
    start_time2 = time.monotonic()

    gpApply(table, py_func_1, data, output, "", "plpythonu")
    end_time2 = time.monotonic()
    res = data.execute_query("select * from basic_output")
    print("plus pythonu", timedelta(seconds=end_time2 - start_time2))
    print(res)

    table_n = data.get_table("basic_numeric", "public")
    output_col = [{"a": "numeric"}]
    output = GPTableMetadata("basic_numeric_output", output_col, "randomly")

    gpApply(table_n, py_func_2, data, output, "plc_python3_shared", "plcontainer", True)
    end_time = time.monotonic()
    print("divide container", timedelta(seconds=end_time - start_time))
    res = data.execute_query("select * from basic_numeric_output")
    print(res)
    start_time2 = time.monotonic()

    gpApply(table_n, py_func_2, data, output, "", "plpythonu")
    end_time2 = time.monotonic()
    res = data.execute_query("select * from basic_numeric_output")
    print("divide pythonu", timedelta(seconds=end_time2 - start_time2))
    print(res)
    db_conn.close()
