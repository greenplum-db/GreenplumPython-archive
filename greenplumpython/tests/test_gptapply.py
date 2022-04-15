from greenplumpython.core.gptapply import gptApply
from greenplumpython.tests.testdb import host, port, db, user, password
from greenplumpython.core.gp_connection import GPConnection
from greenplumpython.core.gpdatabase import GPDatabase
from greenplumpython.core import sql
from greenplumpython.core.gptable_metadata import GPTableMetadata

import pytest,os
@pytest.fixture(scope='session', autouse=True)
def db_conn():
    # Will be executed before the first test
    sql_conn = GPConnection().connect(host, port, db, user, password)
    yield sql_conn
    # Will be executed after the last test
    sql_conn.close()

def recsum(a, b, c):
    x = dict()
    #x['a'] = a[0]+b[0]
    x['a'] = a + b[0]
    return x['a']

def recsum2(a, b):
    return (a*10, b*10)


def aqi_vs_temp(id, city, wdate, temp, humidity, aqi, adjust):
    a = aqi[0]//temp[0]
    return (city[0], a)

def aqi_vs_temp_two(id, city, wdate, temp, humidity, aqi, adjust1, adjust2):
    a = aqi[0]//temp[0]
    return (city[0], a)

def test_gptapply_case1(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("basic", "public")
    output_col = [{"a":"int4"}]
    output = GPTableMetadata("basic_output_t", output_col, 'randomly')
    index = "a"
    gptApply(table, index, recsum, data, output, '', 'plpython3u', True, arg1=[1, "int4"])
    res = data.execute_query("select * from basic_output_t")
    assert res.iat[0,0] ==4 or res.iat[1,0] == 4

def test_gptapply_case1_returndata(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("basic", "public")
    output_col = [{"a":"int4"}]
    output = GPTableMetadata(None, output_col, 'randomly')
    index = "a"
    res = gptApply(table, index, recsum, data, output, '', 'plpython3u', True, arg1=[1, "int4"])
    assert res.iat[0,0] ==4 or res.iat[1,0] == 4

def test_gptapply_case2(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("weather", "public")
    output_columns = [{"city": "text"},{"a": "float"}]
    output = GPTableMetadata("weather_output_t", output_columns, 'randomly')
    index = "id"
    gptApply(table, index, aqi_vs_temp, data, output, '', 'plpython3u', True, arg1=[1, "int4"])
    res = data.execute_query("select * from weather_output_t")
    assert res.iat[0,1] == 13.0 or res.iat[0,1] == 6.0

def test_gptapply_case2_returndata(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("weather", "public")
    output_columns = [{"city": "text"},{"a": "float"}]
    output = GPTableMetadata(None, output_columns, 'randomly')
    index = "id"
    res = gptApply(table, index, aqi_vs_temp, data, output,'', 'plpython3u', True, arg1=[1, "int4"])
    assert res.iat[0,1] == 13.0 or res.iat[0,1] == 6.0

def test_gptapply_case3(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("weather", "public")
    output_columns = [{"city": "text"},{"a": "float"}]
    output = GPTableMetadata("weather_output_t", output_columns, 'randomly')
    index = "id"
    gptApply(table, index, aqi_vs_temp_two, data, output, '', 'plpython3u', True, arg1=[1, "int4"], arg2=[2, "int4"])
    res = data.execute_query("select * from weather_output_t")
    assert res.iat[0,1] == 13.0 or res.iat[0,1] == 6.0

def avg_weather(id, city, temp, humidity, aqi):
    t = float(sum(temp)) / float(len(temp))
    t = format(t, '.2f')
    h = float(sum(humidity)) / float(len(humidity))
    h = format(h, '.2f')
    a = float(sum(aqi)) / float(len(aqi))
    a = format(a, '.2f')
    return (city, t, h, a)

def test_gptapply_case4(db_conn):
    data = GPDatabase(db_conn)
    table = data.get_table("weather", "public")
    output_columns = [{"city": "text"}, {"avg_temp": "float"}, {"avg_humidity": "float"}, {"avg_aqi": "float"}]
    output = GPTableMetadata("weather_output_d", output_columns, ['city'])
    assert output.distribute_on_str == "DISTRIBUTED BY (city)"
    index = "city"
    gptApply(table, index, avg_weather, data, output, '', 'plpython3u')
    res = data.execute_query("select * from weather_output_d order by avg_aqi")
    #assert res.iat[0,3] == 121.0 and res.iat[0,0] == "['New York']"
    assert res.iat[0, 3] == 121.0 and res.iat[0, 0] == "New York"

@pytest.mark.skipif(os.getenv('TESTCONTAINER') == '0', reason="no container installed")
def test_gptapply_plcontainer(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("weather", "public")
        output_columns = [{"city": "text"}, {"avg_temp": "float"}, {"avg_humidity": "float"}, {"avg_aqi": "float"}]
        output = GPTableMetadata("weather_output_d", output_columns, ['city'])
        assert output.distribute_on_str == "DISTRIBUTED BY (city)"
        index = "city"
        assert gptApply(table, index, avg_weather, data, output, 'plc_python_shared')

def test_gptapply_error1_non_table(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_col = [{"a":"int4"}]
        output = GPTableMetadata("basic_output", output_col, 'randomly')
        index = "a"
        assert gptApply(None, index, recsum, data, output, '', 'plpython3u')

def test_gptapply_error2_non_index(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_col = [{"a":"int4"}]
        output = GPTableMetadata("basic_output", output_col, 'randomly')
        assert gptApply(table, None, recsum, data, output, '', 'plpython3u')

def test_gptapply_error3_non_connection(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_col = [{"a":"int4"}]
        output = GPTableMetadata("basic_output", output_col, 'randomly')
        index = "a"
        assert gptApply(table, index, recsum, None, output, '', 'plpython3u',True)

def test_gptapply_error4_non_func(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_col = [{"a":"int4"}]
        output = GPTableMetadata("basic_output", output_col, 'randomly')
        index = "a"
        assert gptApply(table, index, None, data, output, '', 'plpython3u',True)

def test_gptapply_invalidType_signature(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_columns = [{"a": "invalidType"}]
        index = "a"
        output = GPTableMetadata("basic_output5", output_columns, 'randomly')
        assert gptApply(table, index, recsum, data, output,'', 'plpython3u', True)

def test_gptapply_empty_signature(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_columns = []
        index = "a"
        output = GPTableMetadata("basic_output5", output_columns, 'randomly')
        assert gptApply(table, index, recsum, data, output,'', 'plpython3u', True)

def test_gptapply_result_table_column_num_not_match(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_columns = [{"c1": "int4"}]
        output = GPTableMetadata("basic_output4", output_columns, ['c1'])
        assert gptApply(table, index, recsum2, data, output,'', 'plpython3u', True)

def test_gptapply_trans_rollback(db_conn):
    with pytest.raises(Exception) as e:
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_columns = [{"c1": "int4"}]
        output = GPTableMetadata("basic_output4", output_columns, 'randomly')
        assert gptApply(table, index, recsum, data, output,'', 'plpython3u', True)
        assert data.check_table_if_exist("basic_output4", "public") == False

def test_output_name_schema_table(db_conn):
    data = GPDatabase(db_conn)
    data.execute('DROP TABLE IF EXISTS "test_Schema.testGPTapply";')
    data.execute('DROP TABLE IF EXISTS test_Schema.testGPTapply;')
    table = data.get_table("weather", "public")
    output_columns = [{"city": "text"}, {"avg_temp": "float"}, {"avg_humidity": "float"}, {"avg_aqi": "float"}]
    output = GPTableMetadata("test_Schema.testGPTapply", output_columns, 'randomly', True)
    gptApply(table, "id", avg_weather, data, output, '', 'plpython3u')
    res = data.execute_query('select * from "test_Schema.testGPTapply"')
    assert res.iat[0,3] == 312.0 or res.iat[0,3] == 121.0
    data.execute('DROP TABLE IF EXISTS test_Schema.testGPTapply;')

    # non case sensitive
    output.set_case_sensitive(False)
    gptApply(table, "id", avg_weather, data, output, '', 'plpython3u')
    res = data.execute_query('select * from test_Schema.testGPTapply')
    assert res.iat[0,3] == 121.0 or res.iat[0,3] == 312.0

def test_fn_wrong_type(db_conn):
    with pytest.raises(ValueError):
        data = GPDatabase(db_conn)
        table = data.get_table("basic", "public")
        output_col = [{"a":"int4"}]
        output = GPTableMetadata("basic_output_t", output_col, 'randomly')
        index = "a"
        assert gptApply(table, index, 'bad_function', data, output, '', 'plpython3u', arg1=[1, "int4"])
