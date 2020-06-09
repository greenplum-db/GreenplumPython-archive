#GreenplumPython

##Usage

1. Connect to a database
```
connid_obj = GPDatabase.connect(dbIP, dbname, username, password, ...)
```

1. List all connect database
```
connection_ids_array = GPDdatabase.list()
```


1. Close a connection
```
GPDdatabase.close(connid_obj)
```

1. List all tables under this connection
```
table_names_array = GPDdatabase.tables(connid_obj)
```

1. Get a table details as a pandas data frame
```
table_data_frame = GPDdatabase.read_from_table(connid_obj, schema.table)
```

1. Check whether a table has existed or not
```
GPDdatabase.has_table(connid_obj, schema.table)
```

1. Get a table metadata info
```
table_object = GPDatabase.table(connid_obj, schema.table)
```

1. Create a gpdb data frame
```
gpdb_data_frame = GPDatabase.as.dataframe(table_object)
```

TBA
