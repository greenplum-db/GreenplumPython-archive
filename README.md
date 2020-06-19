## GreenplumPython

### Usage

#### Main Funcions Usage

```python
import GPDatabase as gp
import Pandas as pd

# init connection
connection = gp.Connection()

# connect to GPDB
conn = connection.connect(dbIP, dbPort, dbname, username, password, ...)

# init GPDB Database instance
GPDatabase_instance = gp.GPDatabase(conn)

# a dataframe warpper for input table
Dataframe_Wrapper_instance_input = gpdb_instance.get_table(schema, table_name)

# set output table columns info 
columns_output_types = dict()

columns_output_types['a'] = 'int4'

# set output table info
Table_Metadata_output = gp.GPTableMeta(table_output_name, table_output_schema, columns_output_types, distributed_keys)

# apply function
def input_py_func(row_element, extra_args):
    return row_element + extra_args

# run gpapply
Dataframe_Wrapper_instance_output = gp.gpapply(Dataframe_Wrapper_instance_input, input_py_func, GPDatabase_instance, Table_Metadata_output, clear_existing = True, runtime_id = 'plc_r', runtime_type = 'plcontainer', **input_py_func_extra_args)

# get results from gpapply
pd.dataframe =  Dataframe_Wrapper_instance_output.get_pd_dataframe()

output_table_metadata = Dataframe_Wrapper_instance_output.get_metadata()

# group apply function
def input_py_func_groupby(pd.dataframe, extra_args):
    import Pandas as pd
    for row in pd.dataframe
        yield row['a'] + extra_args

# run gpTapply
Dataframe_Wrapper_instance_output = gp.gpTapply(Dataframe_Wrapper_instance_input, input_py_func_groupby, group_by_index, GPDatabase_instance, Table_Metadata_output = None, clear_existing = True, runtime_id = 'plc_r', runtime_type = 'plcontainer', **input_py_func_extra_args)

# get results from gpapply
output_table_metadata = Dataframe_Wrapper_instance_output.get_metadata()

```

#### Utils Functions Usage

```Python
import GPDatabase as gp
import Pandas as pd

# init connection instance
connection = gp.Connection()

# connect to GPDB
conn = gp.connection.connect(dbIP, dbPort, dbname, username, password, ...)

conn1 = gp.connection.connect(dbIP, dbPort, dbname, username, password, ...)

# list all connections
# id - conn
dict() = connection.get_all_connection()

# Get connection by id, None for false
conn2 = connection.get_connection(conn_id)

# close all connecions
connection.close_all()

# close one connection, -1 for false
conn_id = connection.close_connection(conn_id)

# init GPDB Database instance
GPDatabase_instance = gp.GPDatabase(conn)

# run SQL query
pd.dataframe = GPDatabase_instance.execute(SQL)

# Get metadata from table
gp.metadata = GPDatabase_instance.table_meta(table_name, schema)

# Get GPDB dataframe wrapper
gp.dataframe_warpper = GPDatabase_instance.table(table_name, schema)

# Init a GPDB dataframe wrapper
gp.dataframe_warpper1 = gp.Dataframe_Wrapper(pd.dataframe, gp.GPTableMeta)

# Init a GPTableMeta
table_output_name = 'tbl'
table_output_schema = 'public'

columns_output_types = dict()
columns_output_types['a'] = 'int4'
columns_output_types['b'] = 'text'
columns_output_types['c'] = 'float4'

distributed_keys = ['a', 'b']

meta = gp.GPTableMeta(table_output_name, table_output_schema, columns_output_types, distributed_keys)

# Check if table existed
GPDatabase_instance.has_table(table_name, schema)

```

TBA
