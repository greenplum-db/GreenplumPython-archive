import inspect
import psycopg2
import random
import string

def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return "tmp_func"+''.join(random.choice(letters) for i in range(stringLength))

def randomStringType(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return "tmp_type"+''.join(random.choice(letters) for i in range(stringLength))

"""
list("a", "int4", "b", "int4")
output list("a", "int8", "b", "float")
index "col1"
"""

def pythonExec(input, index, tbl_name, funcName, typeName, output_tbl_name):
    func_type_name = input[0]
    #tbl_name = "mul_Col_Table"
    #funcName = "gpFunc"
    #typeName = "gpoutput_type"
    
    if input[0] == index:
        tbl_internal_select = input[0]
    else:
        tbl_internal_select = "array_agg(" + input[0] + ") AS " + input[0]
    
    for i in range(2, len(input)):
        if (i%2 == 0):
            func_type_name = func_type_name + ", " + input[i]
            if input[i] == index:
                tbl_internal_select = tbl_internal_select + ", " + input[i]
            else:
                tbl_internal_select = tbl_internal_select + ", array_agg(" + input[i] + ") AS " + input[i]    
    
    
    select_func = "CREATE TABLE " + output_tbl_name + " AS \n" \
        + "WITH gpdbtmpa AS ( \n" \
        + "SELECT (" + funcName + "(" + func_type_name + ")) AS gpdbtmpb FROM (SELECT " \
        + tbl_internal_select + " FROM " + tbl_name + " GROUP BY " + index + ") tmptbl \n ) \n" \
        + "SELECT (gpdbtmpb::" + typeName + ").* FROM gpdbtmpa DISTRIBUTED RANDOMLY;"
    
    return select_func

    drop_func = "DROP TYPE " + typeName + " CASCADE;"
    
def createTypeFunc(output, typeName):
    typeSQL = output[0] + " " + output[1]
    for i in range(2, len(output)):
        if (i%2 == 0):
            typeSQL = typeSQL + ",\n" + output[i] + " " + output[i+1]
        i = i + 1
    typeSQL = "CREATE TYPE " + typeName + " AS (\n" + typeSQL + "\n);"
    return typeSQL


def pythonApply(input, output, index, py_func, gpdb_tbl_name, output_tbl_name):
    s = inspect.getsource(py_func)
    #gpdb_tbl_name = "testtbl"
    function_name = randomString()
    typeName = randomStringType()
    print(function_name)
    params = []
    columns = []
    i = 0
    while i < len(input):
        if input[i] == index:
            params.append(input[i]+" "+input[i+1])
        else:
            params.append(input[i]+" "+input[i+1]+"[]")
        columns.append(input[i])
        i += 2
        
    create_type_sql = createTypeFunc(output, typeName)
    function_body = "CREATE OR REPLACE FUNCTION %s(%s) RETURNS %s AS $$\n %s return %s(%s) $$ LANGUAGE plpythonu;" % (function_name,",".join(params),typeName,s,py_func.__name__,",".join(columns))

    select_sql = pythonExec(input, index, gpdb_tbl_name, function_name, typeName, output_tbl_name)

    drop_sql = "DROP TYPE " + typeName + " CASCADE;"
    
    
    
    conn = psycopg2.connect(database="test", user="gpadmin", port="15432")
    cur = conn.cursor()
    cur.execute("drop table if exists weather_output;")
    conn.commit()
    cur.execute(create_type_sql)
    conn.commit()

    cur.execute(function_body)
    conn.commit()
    
    
    cur.execute(select_sql)
    
    cur.execute(drop_sql)
    output_select_sql = "select * from " + output_tbl_name + ";"
    cur.execute(output_select_sql)
    res = cur.fetchall()
    conn.commit()
    return res
