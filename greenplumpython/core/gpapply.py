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
        if output.case_sensitive:
            output_name = '"'+output.name+'"'
        else:
            output_name = output.name
        select_func = "CREATE TABLE " + output_name + " AS \n" \
            + "WITH gpdbtmpa AS ( \n" \
            + "SELECT (" + funcName + "(" + joined_type_name + ")) AS gpdbtmpb FROM (SELECT " \
            + joined_type_name + " FROM " + df.table_metadata.name + ") tmptbl \n ) \n" \
            + "SELECT (gpdbtmpb::" + typeName + ").* FROM gpdbtmpa " + output.distribute_on_str + ";"
    return select_func

def gpApply(dataframe, py_func, db, output, clear_existing = True, runtime_id = 'plc_python', runtime_type = 'plpythonu', **kwargs):
    if py_func == None:
        raise ValueError("No input function provided")
    if callable(py_func) == False:
        raise ValueError("Wrong input function provided")
    s = inspect.getsource(py_func)
    function_name = randomString()
    typeName = randomStringType()
    params = []
    columns = inspect.getfullargspec(py_func)[0]

    if dataframe == None:
        raise ValueError("No input dataframe provided")

    for i, col in enumerate(dataframe.table_metadata.signature):
        for j, column in enumerate(col):
            params.append(column+" "+col[column])
    
    rest_args_num = len(columns) - len(params)
    args_index = 0 - rest_args_num

    for key, value in kwargs.items(): 
        params.append(columns[args_index] + " " + str(value[1]))
        args_index = args_index + 1
    
    if output == None or output.signature == None:
        raise ValueError("Output.signature must be provided")
        
    create_type_sql = createTypeFunc(output.signature, typeName)
    runtime_id_str = ''
    if runtime_type == 'plcontainer':
        runtime_id_str = '# container: %s' % (runtime_id)
    function_declare = "%s(%s)" % (function_name,",".join(params))
    function_body = "CREATE OR REPLACE FUNCTION %s RETURNS %s AS $$\n%s\n%s\nreturn %s(%s) $$ LANGUAGE %s;" % (function_declare,typeName,runtime_id_str,s,py_func.__name__,",".join(columns),runtime_type)
    select_sql = pythonExec(dataframe, function_name, typeName, output, kwargs)
    drop_sql = "DROP TYPE " + typeName + " CASCADE;"
    drop_function_sql = "DROP FUNCTION IF EXISTS %s;" % function_declare

    if db == None:
        raise ValueError("No database connection provided")

    try:
        with db.run_transaction() as trans:
            if clear_existing and output.name != None and output.name != "":
                if output.case_sensitive:
                    output_name = '"'+output.name+'"'
                else:
                    output_name = output.name        
                drop_table_sql = "drop table if exists %s;" % output.name
                trans.execute(drop_table_sql)
            trans.execute(create_type_sql)

            trans.execute(function_body)
            res = None
            if output.name == None or output.name == "":
                res = db.execute_transaction_query(trans, select_sql)
            else:
                trans.execute(select_sql)
            trans.execute(drop_function_sql)
            trans.execute(drop_sql)

    except Exception as e:
        raise e

    return res
