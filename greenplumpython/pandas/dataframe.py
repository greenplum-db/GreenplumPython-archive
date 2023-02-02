from typing import Dict, Iterable, List, Optional, Union, Any, Tuple, Literal

import greenplumpython.dataframe as dataframe
import greenplumpython.db as db
import greenplumpython.group as groupby


class DataFrame:
    def __init__(
        self,
        data: Union[List[Union[Tuple[Any, ...], Dict[str, Any]]], Dict[str, List[Any]]],
        columns: Optional[List[str]] = None,
        con: Optional[db.Database] = None,
    ) -> None:
        if isinstance(data, dataframe.DataFrame):
            self._proxy = data
        else:
            assert con is not None
            if isinstance(data, Dict):
                self._proxy = con.create_dataframe(columns=data)
            elif isinstance(data, List):
                self._proxy = con.create_dataframe(rows=data, column_names=columns)
            else:
                raise NotImplementedError

    def to_sql(self, name: str) -> "DataFrame":
        self._proxy = self._proxy.save_as(table_name=name)
        return self

    def read_sql(self, sql: str, con: db.Database):
        self._proxy = dataframe.DataFrame(query=sql, db=con)
        return self

    def sort_values(self, by: Union[str, List[str]], ascending: bool):
        self._proxy = self._proxy.order_by(column_name=by, ascending=ascending)
        return self

    def drop_duplicates(self, subset: Union[str, List[str]]):
        self._proxy = self._proxy.distinct_on(subset)
        return self

    def merge(self,
        right: "DataFrame",
        how: str = "inner",
        on: Optional[Union[str, Iterable[str]]] = None,
        left_on: Optional[Union[str, Iterable[str]]] = None,
        right_on: Optional[Union[str, Iterable[str]]] = None,
        sort: bool = False,
    ):
        pass

    def groupby(self, by: Union[str, List[str]]):
        return DataFrameGroupBy(self._proxy.group_by(by))

    def head(self, n: int):
        self._proxy = self._proxy[:n]
        return self

    def read_csv(
        self,
        filepath: str,
        sep: str,
        delimiter: str,
        names: List[str],
    ):
        pass

    def __repr__(self):
        return self._proxy.__repr__()

    def _repr_html_(self):
        return self._proxy._repr_html_()

    def __iter__(self):
        pass

    def apply(self):
        pass

    def agg(self):
        pass


class DataFrameGroupBy:
    def __init__(self, df_groupby: groupby.DataFrameGroupingSets):
        self._proxy = df_groupby

    def agg(self):
        pass

    def apply(self):
        pass

