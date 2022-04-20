import greenplumpython as gp
import pandas as pd

# init connection
connection = gp.GPConnection()

# connect to GPDB
dbIP = "127.0.0.1"
dbPort = 7000
dbname = "postgres"
username = "gpadmin"
password = ""
conn = connection.connect(dbIP, dbPort, dbname, username, password)

# init GPDB Database instance
gpdb_instance = gp.GPDatabase(conn)


# create table t2 in database
schema = "public"
table_name = "t2"

gpdb_instance.execute("DROP TABLE IF EXISTS t2;")
gpdb_instance.execute("CREATE TABLE t2 (id int, name int);")
gpdb_instance.execute("INSERT INTO t2 (id, name) (WITH numbers AS (SELECT * FROM generate_series(1, 5)) SELECT generate_series, generate_series +2 FROM numbers);")
res = gpdb_instance.execute_query("select * from t2")
print("----------------Table t2-------------------")
print(res)


# # a dataframe warpper for input table
Dataframe_Wrapper_instance_input = gpdb_instance.get_table(table_name, schema)

# ----------------- GPAPPLY -----------------------

# set output table columns info 
columns_output_types = list()
column_type_dict = {'a' : 'int4'}
columns_output_types.append(column_type_dict)

# set output table info without need to store results in a new table
Table_Metadata_output = gp.GPTableMetadata("new_table", columns_output_types, distribute_on='RANDOMLY')

# apply function
def input_py_func(id, name):
    return id + name

# set extra_args to be call within input_py_func
#input_py_func_extra_args = {'extra' : 'int4'}
# run gpapply and return result is a pandas dataframe
#dataframe = gp.gpApply(Dataframe_Wrapper_instance_input, input_py_func, gpdb_instance, Table_Metadata_output, clear_existing = True, runtime_id = 'plc_r', runtime_type = 'plpython3u', **input_py_func_extra_args)
dataframe = gp.gpApply(Dataframe_Wrapper_instance_input, input_py_func, gpdb_instance, Table_Metadata_output, clear_existing = True, runtime_id = 'plc_r', runtime_type = 'plpython3u')
res = gpdb_instance.execute_query("select * from new_table")
print("----------------gpapply id+name--------------------")
print(res)

# ----------------- GPTAPPLY -----------------------

# set output table columns info
columns_output_types = list()
column_type_dict = {'id' : 'int4'}
columns_output_types.append(column_type_dict)
column_type_dict = {'a' : 'int4[]'}
columns_output_types.append(column_type_dict)

# group apply function
def input_py_func_groupby(id, name):
    l = []
    for row in name:
        l.append(row)
    return id, l

# set output table info and store results in a new table
Table_Metadata_output = gp.GPTableMetadata("new_table", columns_output_types, distribute_on='RANDOMLY')
group_by_index = "id"

# run gpTapply
#gp.gpTapply(Dataframe_Wrapper_instance_input, input_py_func_groupby, group_by_index, GPDatabase_instance, Table_Metadata_output = None, clear_existing = True, runtime_id = 'plc_r', runtime_type = 'plcontainer', **input_py_func_extra_args)
gp.gptApply(Dataframe_Wrapper_instance_input, group_by_index, input_py_func_groupby, gpdb_instance, Table_Metadata_output, clear_existing = True, runtime_id = 'plc_r', runtime_type = 'plpython3u')
res = gpdb_instance.execute_query("select * from new_table")
print("-------------gptapply aggregate name-------------------")
print(res)