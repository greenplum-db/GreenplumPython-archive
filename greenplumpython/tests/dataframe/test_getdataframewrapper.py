
from greenplumpython.connection.gp import GPConnection
from greenplumpython.dataframe import sql
from pandas.io.sql import read_sql_table
sql_conn = GPConnection("172.16.110.156", "test", "gpadmin", "").get_connection()
data1 = sql.get_dataframe_from_table("employee", sql_conn)
print(data1)
#data2 = read_sql_table("employee", sql_conn)
#print(data2)
data = sql.get_dataframe_wrapper_from_table("employee",sql_conn)
print(data)

dataq = sql.get_dataframe_wrapper_from_sql("select name from employee", sql_conn)
print(dataq)