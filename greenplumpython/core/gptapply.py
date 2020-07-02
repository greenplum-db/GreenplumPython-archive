import pandas as pd
from greenplumpython.core.gpdatabase import GPDatabase
from greenplumpython.core.dataframe_wrapper import DataFrameWrapper
from greenplumpython.core.gptable_metadata import GPTableMetadata
import inspect
from greenplumpython.utils.apply_utils import *

def pythonExec(df, funcName, typeName, index, output_tbl_name, extra_args):
    func_type_name = []
    internal_select = []
    for i, col in enumerate(df.table_metadata.signature):
        for j, column in enumerate(col):
            internal_select.append("array_agg(" + column + ") AS " + column)
            func_type_name.append(column)

    for key, value in extra_args.items(): 
        func_type_name.append(str(value)) 

    select_func = "CREATE TABLE " + output_tbl_name + " AS \n" \
        + "WITH gpdbtmpa AS ( \n" \
        + "SELECT (" + funcName + "(" + ",".join(func_type_name) +")) AS gpdbtmpb FROM (SELECT " \
        + ",".join(internal_select) + " FROM " + df.table_metadata.name + " GROUP BY " + index + ") tmptbl \n ) \n" \
        + "SELECT (gpdbtmpb::" + typeName + ").* FROM gpdbtmpa DISTRIBUTED RANDOMLY;"
    return select_func

def gptApply(dataframe, index, py_func, db, output, clear_existing = True, runtime_id = 'plc_python', runtime_type = 'plcontainer', **kwargs):    
    s = inspect.getsource(py_func)
    function_name = randomString()
    typeName = randomStringType()
    params = []
    columns = inspect.getfullargspec(py_func)[0]
    for i, col in enumerate(dataframe.table_metadata.signature):
        for j, column in enumerate(col):
            params.append(column+" "+col[column] + "[]")
    
    rest_args_num = len(columns) - len(params)
    args_index = -1

    for key, value in kwargs.items(): 
        params.append(columns[args_index] + " " + str(key))

    create_type_sql = createTypeFunc(output.signature, typeName)
    function_body = "CREATE OR REPLACE FUNCTION %s(%s) RETURNS %s AS $$\n %s return %s(%s) $$ LANGUAGE plpythonu;" % (function_name,",".join(params),typeName,s,py_func.__name__,",".join(columns))
    select_sql = pythonExec(dataframe, function_name, typeName, index, output.name, kwargs)
    drop_sql = "DROP TYPE " + typeName + " CASCADE;"
    if clear_existing:
        drop_table_sql = "drop table if exists %s;" % output.name
        db.execute(drop_table_sql)
    db.execute(create_type_sql)
    db.execute(function_body)
    db.execute(select_sql)
    db.execute(drop_sql)
    return output
