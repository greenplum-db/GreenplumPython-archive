## GreenplumPython
GreenplumPython is a Python 3 client that designed for Greenplum Database (> 6.0). With GreenplumPython installed in R environment, users can interact with data in Greenplum Database for analytics purpose. GreenplumPython provides a rich interface to allow user access both tables and views with minimal data transfer via pandas library. Users can easily access database without any knowledge of SQL. Moreover, GreenplumPython can allow user to execute their own Python code combine with data in Greenplum Database directly via built-in APPLY functions. GreenplumPython can work with PL/Conatiner to provide a high preformance sandbox Python 3 runtime environment.

## Install
GreenplumPython is a python 3 library, and requires the following libraries
`numpy`, `PyGreSQL`, `SQLAlchemy` and `pandas`

## Get Started
```python
import GreenplumPython as gp
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
columns_output_types = list()
column_type_tuple = ('a', 'int4')
columns_output_types.append(column_type_tuple)

# set output table info without need to store results in a new table
Table_Metadata_output = gp.GPTableMeta(None, None, columns_output_types, None)

# apply function
def input_py_func(row_element, extra_args):
    return row_element + extra_args

# run gpapply and return result is a pandas dataframe
dataframe = gp.gpapply(Dataframe_Wrapper_instance_input, input_py_func, GPDatabase_instance, Table_Metadata_output, clear_existing = True, runtime_id = 'plc_r', runtime_type = 'plcontainer', **input_py_func_extra_args)

# group apply function
def input_py_func_groupby(pd.dataframe, extra_args):
    import Pandas as pd
    for row in pd.dataframe
        yield row['a'] + extra_args

# set output table info and store results in a new table
Table_Metadata_output = gp.GPTableMeta("new_table", "schema", columns_output_types, None)

# run gpTapply
gp.gpTapply(Dataframe_Wrapper_instance_input, input_py_func_groupby, group_by_index, GPDatabase_instance, Table_Metadata_output = None, clear_existing = True, runtime_id = 'plc_r', runtime_type = 'plcontainer', **input_py_func_extra_args)

```
## Main Funcions Usage

### gpApply: 
 Apply a R function to every row of data of a data frame object.


### Description

 gpapply allows you to run a R function with input data frame in GreenplumDB through PL/container or PL/Python language.
 The Python function will be parsed, then an UDF will be created in GreenplumDB schema for execution.
 The calculation will be done in parallel with computing resources of GPDB segment hosts.


### Usage

```python
gpApply(dataframe_wrapper, py_func, GPdatabase, output_meta,
            clear_existing = True, runtime_id = "plc_python_shared", runtime_type = "plpythonu",  **kwargs)
```


### Arguments

Argument      |Description
------------- |----------------
```dataframe_wrapper```     |  gp.GPTableMeta: The input dataframe wrapper (i.e. the input table), must be the head arguments of py_func
```py_func```     |      The function to apply each row of the input table dataframe_wrapper
```GPdatabase```     |   gp.GPDatabase: The GreenplumDB connection instance 
```output_meta```     |    gp.GPTableMeta:  The output metadata (i.e. the details of output table). The output_meta.signature must be set. Please see below for the details of gp.GPTableMeta. If the output_meta.name is `None`, function will return a pd.dataframe as the results instead of store the results in a new table with name output_mate.name.
```clear_existing```     |      Bool: Whether clear existing output table stored in GreenplumDB before executing the query 
```runtime_id```     |      Used by "plcontainer" runtime type only. The runtime id is set by plcontainer to specify a runtime cnofiguration. See plcontainer for more information. The argument type is string, e.g. "plc_python_shared" 
```runtime_type```     |      value should be "plcontainer" or "plpythonu" 
```**kwargs```     |       if the function py_func contains extra arguments other than input table columns, you can append them after all of the required arguments list.


### Return Value

 A `pd.dataframe` that contains the result if the output_meta.name is set. Otherwise, it returns a `None`.


## gpTapply: 
 The difference of gptapply comparing with gpapply is, in gptapply, data inside GreenplumDB is group by a selected index.
 A Python function is applied to every row of grouped data.


### Description

 gptApply allows you to run a R function with input data frame in GreenplumDB through PL/container or PL/Python language.
 The Python function will be parsed, then an UDF will be created in GreenplumDB schema for execution.
 The calculation will be done in parallel with computing resources of GPDB segment hosts.


### Usage

```python
gpApply(dataframe_wrapper, index, py_func, GPdatabase, output_meta,
            clear_existing = True, runtime_id = "plc_python_shared", runtime_type = "plpythonu",  **kwargs)
```


### Arguments

Argument      |Description
------------- |----------------
```dataframe_wrapper```     |  gp.GPTableMeta: The input dataframe wrapper (i.e. the input table), must be the head arguments of py_func
```index```     |      The indexed column name, gptApply function will use this column to do `group by` and convert to an array
```py_func```     |      The function to apply each row of the input table dataframe_wrapper
```GPdatabase```     |   gp.GPDatabase: The GreenplumDB connection instance 
```output_meta```     |    gp.GPTableMeta:  The output metadata (i.e. the details of output table). The output_meta.signature must be set. Please see below for the details of gp.GPTableMeta. If the output_meta.name is `None`, function will return a pd.dataframe as the results instead of store the results in a new table with name output_mate.name.
```clear_existing```     |      Bool: Whether clear existing output table stored in GreenplumDB before executing the query 
```runtime_id```     |      Used by "plcontainer" runtime type only. The runtime id is set by plcontainer to specify a runtime cnofiguration. See plcontainer for more information. The argument type is string, e.g. "plc_python_shared" 
```runtime_type```     |      value should be "plcontainer" or "plpythonu" 
```**kwargs```     |       if the function py_func contains extra arguments other than input table columns, you can append them after all of the required arguments list.


### Return Value

 A `pd.dataframe` that contains the result if the output_meta.name is set. Otherwise, it returns a `None`.


## Utils Functions Usage

```Python
import GreenplumPython as gp
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

columns_output_types = list()
column_type_tuple = ('a', 'int4')
columns_output_types.append(column_type_tuple)
column_type_tuple = ('b', 'text')
columns_output_types.append(column_type_tuple)
column_type_tuple = ('c', 'float4')
columns_output_types.append(column_type_tuple)

distributed_keys = ['a', 'b']

meta = gp.GPTableMeta(table_output_name, table_output_schema, columns_output_types, distributed_keys)

# Check if table existed
GPDatabase_instance.check_table_if_exist(table_name, schema)

```