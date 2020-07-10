import inspect
import random
import string
import numpy as np
from greenplumpython.utils.apply_utils import *

def pythonExec(df, funcName, typeName, output, extra_args):
    func_type_name = []

    internal_select = []
    for i, col in enumerate(df.table_metadata.signature):
        for j, column in enumerate(col):
            internal_select.append(column)
            func_type_name.append(column)

    for key, value in extra_args.items(): 
        func_type_name.append(str(value[0]))
    select_func = ""
    joined_type_name = ",".join(func_type_name)
    if output.name == None or output.name == "":
        select_func = "WITH gpdbtmpa AS (SELECT (%s(%s)) AS gpdbtmpb FROM (SELECT %s FROM %s) tmptbl) SELECT (gpdbtmpb::%s).* FROM gpdbtmpa;" % (funcName, joined_type_name, joined_type_name, df.table_metadata.name, typeName)
    else:
        select_func = "CREATE TABLE " + output.name + " AS \n" \
            + "WITH gpdbtmpa AS ( \n" \
            + "SELECT (" + funcName + "(" + joined_type_name + ")) AS gpdbtmpb FROM (SELECT " \
            + joined_type_name + " FROM " + df.table_metadata.name + ") tmptbl \n ) \n" \
            + "SELECT (gpdbtmpb::" + typeName + ").* FROM gpdbtmpa " + output.distribute_on_str + ";"
    return select_func

def gpApply(dataframe, py_func, db, output, clear_existing = True, runtime_id = 'plc_python', runtime_type = 'plpythonu', **kwargs):
    s = inspect.getsource(py_func)
    #gpdb_tbl_name = "testtbl"
    function_name = randomString()
    typeName = randomStringType()
    params = []
    columns = []
    for i, col in enumerate(dataframe.table_metadata.signature):
        for j, column in enumerate(col):
            params.append(column+" "+col[column])
            columns.append(column)
    
    rest_args_num = len(columns) - len(params)
    args_index = 0 - rest_args_num

    for key, value in kwargs.items(): 
        params.append(columns[args_index] + " " + str(value[1]))
        args_index = args_index + 1
        
    create_type_sql = createTypeFunc(output.signature, typeName)
    runtime_id_str = ''
    if runtime_type == 'plcontainer':
        runtime_id_str = '# container: %s' % (runtime_id)
    function_body = "CREATE OR REPLACE FUNCTION %s(%s) RETURNS %s AS $$\n%s\n%s\nreturn %s(%s) $$ LANGUAGE %s;" % (function_name,",".join(params),typeName,runtime_id_str,s,py_func.__name__,",".join(columns),runtime_type)
    select_sql = pythonExec(dataframe, function_name, typeName, output, kwargs)
    drop_sql = "DROP TYPE " + typeName + " CASCADE;"
    if clear_existing:
        drop_table_sql = "drop table if exists %s;" % output.name
        db.execute(drop_table_sql)
    db.execute(create_type_sql)
    db.execute(function_body)
    res = None
    if output.name == None or output.name == "":
        res = db.execute_query(select_sql)
    else:
        db.execute(select_sql)
    db.execute(drop_sql)
    return res
