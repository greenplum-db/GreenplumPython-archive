from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

from sqlalchemy import engine

import greenplumpython.dataframe as dataframe
import greenplumpython.db as db
import greenplumpython.group as groupby
import greenplumpython.order as orderby


class DataFrame:
    def __init__(
        self,
        data: Union[
            dataframe.DataFrame, List[Union[Tuple[Any, ...], Dict[str, Any]]], Dict[str, List[Any]]
        ],
        columns: Optional[List[str]] = None,
        con: Optional[db.Database] = None,
    ) -> None:
        if isinstance(data, dataframe.DataFrame):
            self._proxy = data
        else:
            assert con is not None
            if isinstance(data, Dict):
                self._proxy = con.create_dataframe(columns=data)
            else:
                self._proxy = con.create_dataframe(rows=data, column_names=columns)

    def to_sql(
        self,
        name: str,
        con: engine,
        schema: Union[str, None] = None,
        if_exists: Literal["fail", "replace", "append"] = "fail",
        index: bool = False,  # Not Used
        index_label: Union[str, List[str]] = None,  # Not Used
        chunksize: Union[int, None] = None,  # Not Used
        dtype: Union[Dict[str, type], None] = None,  # Not Used
        method: Literal[None, "multi", callable] = None,  # Not Used
    ) -> int:
        assert index is False, "DataFrame in GreenplumPython.pandas does not have an index column"
        assert if_exists in ["fail", "replace"], "Only support if_exists = 'fail' or 'replace'"
        table_name = name if schema is None else (schema + "." + name)
        with con.connect() as connection:
            if if_exists == "replace":
                connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            result = connection.execute(
                f"""
                    CREATE TABLE "{table_name}"
                    AS {self._proxy._build_full_query()}
                """
            )
            return result.rowcount

    def sort_values(
        self,
        by: Union[str, List[str]],
        axis: int = 0,  # Not Used
        ascending: Union[bool, list[bool], tuple[bool, ...]] = True,
        inplace: bool = False,
        kind: Literal["quicksort", "mergesort", "heapsort", "stable"] = "quicksort",  # Not Used
        na_position: Literal["first", "last"] = "last",
        ignore_index: bool = True,
        key: Optional[Callable] = None,  # Not Used
    ) -> "DataFrame":
        assert inplace is False, "Cannot perform operation in place"
        assert (
            ignore_index is True
        ), "DataFrame in GreenplumPython.pandas does not have an index column"
        nulls_first = True if na_position == "first" else False
        df = orderby.DataFrameOrdering(
                self._proxy,
                by if isinstance(by, list) else [by],
                ascending if isinstance(ascending, list) else [ascending],
                len(by)*[nulls_first] if isinstance(by, list) else [nulls_first],
                len(by)*[None]
            )[:]
        return DataFrame(df)

    def drop_duplicates(
        self,
        subset: Union[str, List[str], None] = None,
        keep: Literal["first", "last", False] = "first",
        inplace: bool = False,
        ignore_index: bool = True,
    ):
        assert keep == "first", "Can only keep first occurrence"
        assert inplace is False, "Cannot perform operation in place"
        assert (
                ignore_index is True
        ), "DataFrame in GreenplumPython.pandas does not have an index column"
        df = self._proxy.distinct_on(*subset)
        return DataFrame(df)

    def join(
        self,
        other: "DataFrame",
        on: Union[str, List[str]],
        how: Literal["left", "right", "outer", "inner", "cross"] = "left",
        lsuffix: str = "",
        rsuffix: str = "",
        sort: bool = False,
        validate: Optional[str] = None,  # Not Used
    ):
        how = "full" if how == "outer" else how
        assert (
            lsuffix == "" and rsuffix == ""
        ), "Can't support yet automatically handle overlapping column's suffice"
        assert (
            sort is False
        ), "Can't support yet order result DataFrame lexicographically by the join key"
        df = self._proxy.join(other._proxy, how=how, on=on)
        return DataFrame(df)

    def groupby(self, by: Union[str, List[str]]):
        return DataFrameGroupBy(self._proxy.group_by(by))

    def head(self, n: int):
        df = self._proxy[:n]
        return DataFrame(df)

    def __repr__(self):
        return self._proxy.__repr__()

    def _repr_html_(self):
        return self._proxy._repr_html_()

    def __iter__(self):
        return self._proxy.__iter__()

    def apply(self):
        pass

    def agg(self):
        pass


def read_sql(
    sql: str,
    con: db.Database,
    index_col: Union[str, list[str]] = None,
    coerce_float: bool = True,
    params=None,
    parse_dates=None,
    columns: Optional[list[str]] = None,
    chunksize: Optional[int] = None,
):
    try:
        con.execute(f'SELECT * FROM "{sql}"')
        return DataFrame(
            dataframe.DataFrame(f'TABLE "{sql}"', name=sql, db=con),
            con=con,
        )
    except:
        return DataFrame(dataframe.DataFrame(query=sql, db=con))



class DataFrameGroupBy:
    def __init__(self, df_groupby: groupby.DataFrameGroupingSets):
        self._proxy = df_groupby

    def agg(self):
        pass

    def apply(self):
        pass
