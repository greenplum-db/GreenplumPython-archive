
from pandas.io.sql import SQLTable, DataFrame
class DataFrameWrapper(SQLTable):
    def read(self, coerce_float=True, parse_dates=None, columns=None, chunksize=None):

        if columns is not None and len(columns) > 0:
            from sqlalchemy import select

            cols = [self.table.c[n] for n in columns]
            if self.index is not None:
                for idx in self.index[::-1]:
                    cols.insert(0, self.table.c[idx])
            sql_select = select(cols)
        else:
            sql_select = self.table.select()

        result = self.pd_sql.execute(sql_select)
        column_names = result.keys()

        if chunksize is not None:
            return self._query_iterator(
                result,
                chunksize,
                column_names,
                coerce_float=coerce_float,
                parse_dates=parse_dates,
            )
        else:
            data = dict()
            self.frame = DataFrame.from_records(
                data, columns=column_names, coerce_float=coerce_float
            )

            self._harmonize_columns(parse_dates=parse_dates)

            if self.index is not None:
                self.frame.set_index(self.index, inplace=True)

            return self.frame

