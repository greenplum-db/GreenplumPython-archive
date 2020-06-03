#GreenplumPython

##Usage

1. Connect to a database
```
connid = GPDatabase.connect(dbIP, dbname, username, password, ...)
```

1. List all connect database
```
connection_ids_array = GPDdatabase.list()
```

1. Set a default connection
```
GPDdatabase.set_connection(connid)
```

1. Close a connection
```
GPDdatabase.close(connid=1)
```

1. List all tables under this connection
```
table_names_array = GPDdatabase.tables(connid=1)
```

1. Get a table details as a pandas data frame
```
table_data_frame = GPDdatabase.read_from_table(connid = 1, schema.table)
```

1. Check whether a table has existed or not
```
GPDdatabase.has_table(connid = 1, schema.table)
```

1. Get a table metadata info
```
table_object = GPDatabase.table(connid = 1, schema.table)
```

1. Create a gpdb data frame
```
gpdb_data_frame = GPDatabase.as.dataframe(table_object)
```

TBA
