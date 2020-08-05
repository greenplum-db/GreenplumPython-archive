from greenplumpython.core.gpapply import gpApply
from greenplumpython.tests.testdb import host, port, db, user, password
from greenplumpython.core.gp_connection import GPConnection
from greenplumpython.core.gpdatabase import GPDatabase
from greenplumpython.core import sql
from greenplumpython.core.gptable_metadata import GPTableMetadata

import pytest,os,pg
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

def recsum2(a, b):
    return (a*10, b*10)

def recsum3(a, b):
    return (a*10, "hello")

def recsumerr(a, b):
    i = 10 + 'hello'
    return (0, 0)

def recsum4(a, b, junk1, junk2):
    if junk1 != 12:
        return
    if junk2 != 13:
        return
    return (a*10, b*10)

def recsum5(a, b, junk1, junk2):
    if junk1 != 12:
        return
    if junk2 != 'Hello':
        return
    return (a*10, b*10)

def inc(id, city, wdate, temp, humidity, aqi):
    x = dict()
    x['id'] = id+1
    return x

def aqi_vs_temp(id, city, wdate, temp, humidity, aqi):
    a = aqi/temp
    return (id, city, a)

def test_gpapply_case1(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("basic", "public")
    output_col = [{"a":"int4"}]
    output = GPTableMetadata("basic_output", output_col, 'randomly')
    gpApply(table, recsum, data, output, '', 'plpythonu')
    res = data.execute_query("select * from basic_output")
    assert res.iat[0,0] ==4 or res.iat[1,0] == 4

def test_gpapply_case1_returndata(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("basic", "public")
    output_col = [{"a":"int4"}]
    output = GPTableMetadata(None, output_col, 'randomly')
    res = gpApply(table, recsum, data, output, '', 'plpythonu')
    assert res.iat[0,0] ==4 or res.iat[1,0] == 4


def test_gpapply_case2(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("weather", "public")
    output_columns = [{"id": "int"}, {"city": "text"},{"a": "float"}]
    output = GPTableMetadata("weather_output", output_columns, 'randomly')
    gpApply(table, aqi_vs_temp, data, output, '', 'plpythonu')
    res = data.execute_query("select * from weather_output")
    assert res.iat[0,2] == 13.0 or res.iat[0,2] == 6.0

def test_gpapply_case2_returndata(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("weather", "public")
    output_columns = [{"id": "int"}, {"city": "text"},{"a": "float"}]
    output = GPTableMetadata(None, output_columns, 'randomly')
    res = gpApply(table, aqi_vs_temp, data, output, '', 'plpythonu')
    assert res.iat[0,2] == 13.0 or res.iat[0,2] == 6.0

def test_gpapply_result_table_distributed_by(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("basic", "public")
    output_columns = [{"c1": "int4"},{"c2": "int4"}]
    output = GPTableMetadata("basic_output3", output_columns, ['c1'])
    gpApply(table, recsum2, data, output, '', 'plpythonu')
    res = data.execute_query("select c1, c2 from basic_output3 order by c1")
    assert res.iat[0,0] == 10 and res.iat[0,1] == 30
    assert res.iat[1,0] == 20 and res.iat[1,1] == 40

def test_gpapply_invalidType_signature(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_columns = [{"a": "invalidType"}]
        output = GPTableMetadata("basic_output5", output_columns, 'randomly')
        assert gpApply(table, recsum2, data, output)

def test_gpapply_empty_signature(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_columns = []
        assert GPTableMetadata("basic_output6", output_columns, 'randomly')

def test_gpapply_result_table_column_num_not_match(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_columns = [{"c1": "int4"}]
        output = GPTableMetadata("basic_output4", output_columns, ['c1'])
        assert gpApply(table, recsum2, data, output, '', 'plpythonu')

def test_gpapply_result_table_column_type_not_match(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_columns = [{"c1": "int4"},{"c2": "int4"}]
        output = GPTableMetadata("basic_output", output_columns, 'randomly')
        gpApply(table, recsum3, data, output, '', 'plpythonu')
        assert data.execute_query("select c1, c2 from basic_output order by c1")

def test_gpapply_pyfunc_error(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_columns = [{"c1": "int4"},{"c2": "int4"}]
        output = GPTableMetadata("basic_output", output_columns, 'randomly')
        assert gpApply(table, recsumerr, data, output, '', 'plpythonu')

def test_gpapply_distributedby_column(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("weather", "public")
    output_columns = [{"id": "int"}, {"city": "text"},{"a": "float"}]
    output = GPTableMetadata("weather_output", output_columns, ['city'])
    assert output.distribute_on_str == "DISTRIBUTED BY (city)"
    gpApply(table, aqi_vs_temp, data, output, '', 'plpythonu')
    res = data.execute_query("select * from weather_output")
    assert res.iat[0,2] == 13.0 or res.iat[0,2] == 6.0

@pytest.mark.skipif(os.getenv('TESTCONTAINER') == '0', reason="no container installed")
def test_gpapply_plcontainer(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_col = [{"a":"int4"}]
        output = GPTableMetadata("basic_output", output_col, 'randomly')
        assert gpApply(table, recsum, data, output, 'plc_python_shared', 'plcontainer', True)

def test_view(db_conn):
    data = GPDatabase(db_conn)
    data.execute("DROP VIEW IF EXISTS tableview;")
    data.execute('CREATE VIEW tableview AS SELECT * FROM "weather";')
    table_view = data.get_table("tableview", "public")
    output = GPTableMetadata(None, [{"id": "int"}], 'randomly', True)
    res = gpApply(table_view, inc, data, output, '', 'plpythonu')

def test_gpapply_error1_non_table(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_col = [{"a":"int4"}]
        output = GPTableMetadata("basic_output", output_col, 'randomly')
        assert gpApply(None, recsum, data, output, '', 'plpythonu', True)

def test_gpapply_error2_non_func(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_col = [{"a":"int4"}]
        output = GPTableMetadata("basic_output", output_col, 'randomly')
        assert gpApply(table, None, data, output, '', 'plpythonu', True)

def test_gpapply_error3_non_connection(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_col = [{"a":"int4"}]
        output = GPTableMetadata("basic_output", output_col, 'randomly')
        assert gpApply(table, recsum, None, output, '', 'plpythonu', True)

def test_gpapply_trans_rollback(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_columns = [{"c1": "int4"}]
        output = GPTableMetadata("basic_output4", output_columns, 'randomly')
        assert gpApply(table, recsum, data, output, '', 'plpythonu')
        assert data.check_table_if_exist("basic_output4", "public") == False

def test_gpapply_output_name_schema_table(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("weather", "public")
    output_columns = [{"id": "int"}, {"city": "text"},{"a": "float"}]
    data.execute('DROP TABLE IF EXISTS "test_Schema.testGPapply";')
    data.execute('DROP TABLE IF EXISTS test_Schema.testGPapply;')
    output = GPTableMetadata("test_Schema.testGPapply", output_columns, 'randomly', True)
    gpApply(table, aqi_vs_temp, data, output, '', 'plpythonu')
    res = data.execute_query('select * from "test_Schema.testGPapply"')
    assert res.iat[0,2] == 13.0 or res.iat[0,2] == 6.0
    data.execute('DROP TABLE IF EXISTS "test_Schema.testGPapply";')

    # non case sensitive
    output.set_case_sensitive(False);
    gpApply(table, aqi_vs_temp, data, output, '', 'plpythonu')
    res = data.execute_query('select * from test_Schema.testGPapply')
    assert res.iat[0,2] == 13.0 or res.iat[0,2] == 6.0

def test_fn_wrong_type(db_conn):
    with pytest.raises(ValueError):
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_col = [{"a":"int4"}]
        output = GPTableMetadata("basic_output", output_col, 'randomly')
        assert gpApply(table, 'bad_function', None, output, '', 'plpythonu',True)

def test_table_exist(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("basic", "public")
    output_col = [{"a":"int4"}]
    output = GPTableMetadata("basic_output", output_col, 'randomly')
    #0 clear.existing is TRUE, when the table doesn't exist (OK)
    data.execute("DROP TABLE IF EXISTS %s;" % output.name)
    gpApply(table, recsum, data, output, '', 'plpythonu', True)
    res = data.execute_query("select * from basic_output")
    assert res.iat[0,0] ==4 or res.iat[1,0] == 4
    #1 clear.existing is TRUE, when the table exists        (OK)
    gpApply(table, recsum, data, output, '', 'plpythonu', True)
    res = data.execute_query("select * from basic_output")
    assert res.iat[0,0] ==4 or res.iat[1,0] == 4
    #2 clear.existing is FALSE, when the table doesn't exist(OK)
    # clear existing table
    data.execute("DROP TABLE IF EXISTS %s;" % output.name)
    res = data.execute_query("SELECT 1 FROM pg_class WHERE relname='%s';" % output.name)
    assert res.empty == True
    gpApply(table, recsum, data, output, '', 'plpythonu',False)
    res = data.execute_query("select * from basic_output")
    assert res.iat[0,0] ==4 or res.iat[1,0] == 4
    #3 clear.existing is FALSE, when the table exists       (ERROR)
    with pytest.raises(Exception):
        assert gpApply(table, recsum, data, output, '', 'plpythonu', False)

def test_additional_junk_params(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("basic", "public")
    output_col = [{"c1": "int4"},{"c2": "int4"}]
    output = GPTableMetadata("basic_output", output_col, 'randomly')
    #0 clear.existing is TRUE, when the table doesn't exist (OK)
    data.execute("DROP TABLE IF EXISTS %s;" % output.name)
    gpApply(table, recsum4, data, output, '', 'plpythonu', True, arg1=[12, "int4"], arg2=[13, "int4"])
    res = data.execute_query("select * from basic_output")
    assert res.iat[0,0] ==20 or res.iat[1,0] == 20

def test_additional_junk_params_text(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("basic", "public")
    output_col = [{"c1": "int4"},{"c2": "int4"}]
    output = GPTableMetadata("basic_output", output_col, 'randomly')
    #0 clear.existing is TRUE, when the table doesn't exist (OK)
    data.execute("DROP TABLE IF EXISTS %s;" % output.name)
    gpApply(table, recsum5, data, output, '', 'plpythonu',True, arg1=[12, "int4"], arg2=["'Hello'", "text"])
    res = data.execute_query("select * from basic_output")
    assert res.iat[0,0] ==20 or res.iat[1,0] == 20