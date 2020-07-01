import inspect
import random
import string
import numpy as np
<<<<<<< HEAD
=======

numpyDTypeToGPDBType = {np.dtype('int64'): "int", np.dtype('object'): "text", np.dtype("datetime64[ns]"):"datetime"}
>>>>>>> change gpapply to run with new database & dataframe

def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return "tmp_func"+''.join(random.choice(letters) for i in range(stringLength))

def randomStringType(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return "tmp_type"+''.join(random.choice(letters) for i in range(stringLength))

def pythonExec(df, funcName, typeName, output_tbl_name, extra_args):
    func_type_name = []

    internal_select = []
    for i, col in enumerate(df.table_metadata.signature):
        for j, column in enumerate(col):
            internal_select.append("array_agg(" + column + ") AS " + column)
            func_type_name.append(column)


    select_func = "CREATE TABLE " + output_tbl_name + " AS \n" \
        + "WITH gpdbtmpa AS ( \n" \
        + "SELECT (" + funcName + "(" + ",".join(func_type_name) + ")) AS gpdbtmpb FROM (SELECT " \
        + ",".join(func_type_name) + " FROM " + df.table_metadata.name + ") tmptbl \n ) \n" \
        + "SELECT (gpdbtmpb::" + typeName + ").* FROM gpdbtmpa DISTRIBUTED RANDOMLY;"
    return select_func

    drop_func = "DROP TYPE " + typeName + " CASCADE;"

def createTypeFunc(sig, typeName):
    typeSQL = ""
    for i  in range(0,len(sig)):
        if i == 0:
            for j, col in enumerate(sig[i]):
                typeSQL += col + " " + sig[i][col]
        else:
            for j, col in enumerate(sig[i]):
                typeSQL += ",\n" + col + " " + sig[i][col]
    typeSQL = "CREATE TYPE " + typeName + " AS (\n" + typeSQL + "\n);"
    return typeSQL

def gpApply(dataframe, py_func, db, output, clear_existing = True, runtime_id = 'plc_python', runtime_type = 'plcontainer', **input_py_func_extra_args):

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
        
    create_type_sql = createTypeFunc(output.signature, typeName)
    function_body = "CREATE OR REPLACE FUNCTION %s(%s) RETURNS %s AS $$\n %s return %s(%s) $$ LANGUAGE plpythonu;" % (function_name,",".join(params),typeName,s,py_func.__name__,",".join(columns))
    select_sql = pythonExec(dataframe, function_name, typeName, output.name, input_py_func_extra_args)
    drop_sql = "DROP TYPE " + typeName + " CASCADE;"
    if clear_existing:
        drop_table_sql = "drop table if exists %s;" % output.name
        db.execute(drop_table_sql)
    db.execute(create_type_sql)
    db.execute(function_body)
    db.execute(select_sql)
    db.execute(drop_sql)
    return output
