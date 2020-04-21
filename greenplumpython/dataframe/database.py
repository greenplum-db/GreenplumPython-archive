from pandas.io.sql import SQLDatabase, SQLTable, _convert_params, _wrap_result
from .dataframe_wrapper import DataFrameWrapper
class GPDatabase(SQLDatabase):
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
