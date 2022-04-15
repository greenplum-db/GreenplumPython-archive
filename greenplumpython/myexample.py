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

# a dataframe warpper for input table
schema = "public"
table_name = "t2"
Dataframe_Wrapper_instance_input = gpdb_instance.get_table(table_name, schema)

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
