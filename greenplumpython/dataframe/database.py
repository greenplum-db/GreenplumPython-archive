from pandas.io.sql import SQLDatabase, SQLTable, _convert_params, _wrap_result, DataFrame
from .dataframe_wrapper import DataFrameWrapper
from greenplumpython.connection.gp import GPConnection

class GPDatabase(SQLDatabase):
    def __init__(self, engine, schema=None, meta=None):
        super(GPDatabase, self).__init__(engine, schema, meta)
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
            port,
            dbname,
            username,
            password=None):
        conn = GPConnection(dbip, port, dbname, username, password).get_connection()
        self.max_conn_id += 1
        self.conns[self.max_conn_id] = conn
        return self.max_conn_id

    def close(self, connid):
        if connid in self.conns:
            self.conns[connid].close()
            del self.conns[connid]

    def list(self):
        return sorted(self.conns.keys())

    def load_table_object(self, table_name, schema=None):
        if not schema:
            db_schema = "public"
        else:
            db_schema = schema

        db_sql = "select column_name, udt_name from information_schema.columns where table_schema='{schema}' AND table_name='{name}'".format(schema=db_schema, name=table_name)
        result = self.execute(db_sql)
        data = result.fetchall()
        pd_frame = DataFrame.from_records(data, columns=result.keys())
        table_object = dict()
        for i, row in pd_frame.iterrows():
            table_object[row['column_name']] = row['udt_name']
        return table_object

