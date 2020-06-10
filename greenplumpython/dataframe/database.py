from pandas.io.sql import SQLDatabase, SQLTable, _convert_params, _wrap_result
from .dataframe_wrapper import DataFrameWrapper
from greenplumpython.connection.gp import GPConnection

class GPDatabase(SQLDatabase):
    def __init__(self):
        self.max_conn_id = 0
        self.conns = {}

    def read_table(self,
        table_name,
        index_col=None,
        coerce_float=True,
        parse_dates=None,
        columns=None,
        schema=None,
        chunksize=None,
    ):
        table = DataFrameWrapper(table_name, self, index=index_col, schema=schema)
        return table.read(
            coerce_float=coerce_float,
            parse_dates=parse_dates,
            columns=columns,
            chunksize=chunksize,
        )
    def read_query(self,
        sql,
        index_col=None,
        coerce_float=True,
        parse_dates=None,
        params=None,
        chunksize=None,
    ):
        args = _convert_params(sql, params)

        result = self.execute(*args)
        columns = result.keys()
        data = dict()
        frame = _wrap_result(
            data,
            columns,
            index_col=index_col,
            coerce_float=coerce_float,
            parse_dates=parse_dates,
        )
        return frame
    def connect(self,
            dbip,
            dbname,
            username,
            password=None):
        conn = GPConnection(dbip, dbname, username, password).get_connection()
        self.max_conn_id += 1
        self.conns[self.max_conn_id] = conn
        return self.max_conn_id
