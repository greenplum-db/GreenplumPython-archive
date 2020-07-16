from pandas.io.sql import SQLDatabase, SQLTable, DataFrame
from greenplumpython.core.dataframe_wrapper import DataFrameWrapper
from greenplumpython.core.gp_connection import GPConnection
from greenplumpython.core.gptable_metadata import GPTableMetadata

class GPDatabase(SQLDatabase):
    def __init__(self, conn, schema=None, meta=None):
        super(GPDatabase, self).__init__(conn, schema, meta)

    def execute_query(self, db_sql):
        result = self.execute(db_sql)
        data = result.fetchall()
        pd_frame = DataFrame.from_records(data, columns=result.keys())
        return pd_frame

    def execute_transaction_query(self, trans, db_sql):
        result = trans.execute(db_sql)
        data = result.fetchall()
        pd_frame = DataFrame.from_records(data, columns=result.keys())
        return pd_frame

    def get_table(self, table_name, schema=None):
        if not schema:
            db_schema = "public"
        else:
            db_schema = schema

        # TODO: need ordered column name/udt_name
        db_sql = "select column_name, udt_name from information_schema.columns where table_schema='{schema}' AND table_name='{name}'".format(schema=db_schema, name=table_name)
        pd_frame = self.execute_query(db_sql)
        
        columns_type = list()
        for i, row in pd_frame.iterrows():
            column_type = {row['column_name']:row['udt_name']}
            columns_type.append(column_type)
        # TODO: get distributed info
        table_meta = GPTableMetadata(table_name, columns_type, None)
        result = DataFrameWrapper(dataframe=pd_frame, table_metadata = table_meta)
        return result

    def check_table_if_exist(self, table_name, schema):
        return self.has_table(table_name, schema)

