"""
This package supports pandas compatible API which involves `greenplumpython.pandas.DataFrame`.

This package contains classes and functions having same names and parameters as the equivalence in pandas to
provide a Data Scientist familiar syntax. And at the same time, its DataFrame has same specifications as
GreenplumPython DataFrame, which means: Data is located and manipulated on a remote database system.

N.B.: This package contains fewer functions than GreenplumPython DataFrame, but it is easy the conversion between
these two DataFrame.
"""
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

from pglast import parser  # type: ignore
from sqlalchemy import engine, text

import greenplumpython.dataframe as dataframe
import greenplumpython.db as db
import greenplumpython.order as orderby


class DataFrame:
    @classmethod
    def from_sql(cls, sql: str, conn: engine):
        c = super().__new__(cls)
        database = db.Database(url=str(conn.url))
        try:
            parser.parse_sql(sql)
            c._dataframe = dataframe.DataFrame(query=sql, db=database)
        except:
            df = database.create_dataframe(table_name=sql)
            c._dataframe = df
        return c

    def __init__(
        self,
        data: Union[
            dataframe.DataFrame, List[Union[Tuple[Any, ...], Dict[str, Any]]], Dict[str, List[Any]]
        ],
        columns: Optional[List[str]] = None,
        conn: Optional[db.Database] = None,
    ) -> None:
        if isinstance(data, dataframe.DataFrame):
            self._dataframe = data
        else:
            assert conn is not None
            if isinstance(data, Dict):
                self._dataframe = conn.create_dataframe(columns=data)
            else:
                self._dataframe = conn.create_dataframe(rows=data, column_names=columns)

    def to_sql(
        self,
        name: str,
        conn: engine,
        schema: Union[str, None] = None,
        if_exists: Literal["fail", "replace", "append"] = "fail",
        index: bool = False,  # Not Used
        index_label: Union[str, List[str]] = None,  # Not Used
        chunksize: Union[int, None] = None,  # Not Used
        dtype: Union[Dict[str, type], None] = None,  # Not Used
        method: Literal[None, "multi"] = None,  # Not Used  # type: ignore
    ) -> int:
        """
        Write records stored in a DataFrame to a SQL database.
        Tables in database can be newly created, appended to, or overwritten.

        This aligns with the function "pandas.DataFrame.to_sql()"
        <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html>`_.

        Returns:
            int: Number of rows affected by to_sql.

        Example:
            .. highlight:: python
            .. code-block::  python
                >>> import greenplumpython.pandas as pd
                >>> pd_df = pd.read_sql('SELECT unnest(ARRAY[1,2,3]) AS "a",unnest(ARRAY[1,2,3]) AS "b"', conn)
                >>> pd_df.to_sql(name="test_to_sql", conn=conn)
                3
                >>> pd.read_sql("test_to_sql", conn=conn)
                -------
                 a | b
                ---+---
                 1 | 1
                 2 | 2
                 3 | 3
                -------
                (3 rows)

        """
        assert index is False, "DataFrame in GreenplumPython.pandas does not have an index column"
        table_name = f'"{name}"' if schema is None else f"{schema}.{name}"
        with conn.connect() as connection:  # type: ignore
            query = self._dataframe._build_full_query()  # type: ignore
            if if_exists == "append":
                with connection.begin():
                    result = connection.execute(  # type: ignore
                        text(
                            f"""
                                INSERT INTO {table_name}
                                {query}
                            """
                        )
                    )
            else:
                if if_exists == "replace":
                    with connection.begin():
                        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                with connection.begin():
                    result = connection.execute(  # type: ignore
                        text(
                            f"""
                                CREATE TABLE {table_name}
                                AS {query}
                            """
                        )
                    )
            count: int = result.rowcount
            return count

    def to_gp_dataframe(self) -> dataframe.DataFrame:
        """
        Convert a GreenplumPython Pandas compatible DataFrame to a GreenplumPython DataFrame.

        Returns:
            a GreenplumPython :class:`~greenplumpython.dataframe.Dataframe`.
        """
        return self._dataframe

    def sort_values(
        self,
        by: Union[str, List[str]],
        axis: int = 0,  # Not Used
        ascending: Union[bool, list[bool], tuple[bool, ...]] = True,
        inplace: bool = False,
        kind: Literal["quicksort", "mergesort", "heapsort", "stable"] = "quicksort",  # Not Used
        na_position: Literal["first", "last"] = "last",
        ignore_index: bool = True,
        key: Optional[Callable[[Any], None]] = None,  # Not Used
    ) -> "DataFrame":
        """
        Sort by the values along columns.

        This aligns with the function "pandas.DataFrame.sort_values()"
        <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sort_values.html>`_.

        Returns:
            :class:`Dataframe`: class:`DataFrame` order by the given arguments.

        Example:
            .. highlight:: python
            .. code-block::  python
                >>> pd_df = pd.read_sql('SELECT unnest(ARRAY[3, 1, 2]) AS "id",unnest(ARRAY[1,2,3]) AS "b"', conn)
                >>> pd_df.sort_values(["id"])
                --------
                 id | b
                ----+---
                  1 | 2
                  2 | 3
                  3 | 1
                --------
                (3 rows)

        """
        assert inplace is False, "Cannot perform operation in place"
        assert (
            ignore_index is True
        ), "DataFrame in GreenplumPython.pandas does not have an index column"
        nulls_first = True if na_position == "first" else False
        df = orderby.DataFrameOrdering(
            self._dataframe,
            by if isinstance(by, list) else [by],
            ascending if isinstance(ascending, list) else [ascending],
            len(by) * [nulls_first] if isinstance(by, list) else [nulls_first],
            len(by) * [None],
        )[:]
        return DataFrame(df)

    def drop_duplicates(
        self,
        subset: Union[str, List[str], None] = None,
        keep: Literal["first", "last", False] = "first",
        inplace: bool = False,
        ignore_index: bool = True,
    ):
        """
        Return DataFrame with duplicate rows removed.

        This aligns with the function "pandas.DataFrame.drop_duplicates()"
        <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html>`_.

        Returns:
            :class:`Dataframe`: class:`DataFrame` with duplicates removed.

        Example:
            .. highlight:: python
            .. code-block::  python
                >>> import greenplumpython.pandas as pd
                >>> students = [("alice", 18), ("bob", 19), ("carol", 19)]
                >>> db.create_dataframe(rows=students, column_names=["name", "age"]).save_as("student", column_names=["name", "age"])
                >>> student = pd.read_sql("student", conn)
                >>> student.drop_duplicates(subset=["age"])
                -------------
                 name  | age
                -------+-----
                 alice |  18
                 bob   |  19
                -------------
                (2 rows)

        """
        assert keep == "first", "Can only keep first occurrence"
        assert inplace is False, "Cannot perform operation in place"
        assert (
            ignore_index is True
        ), "DataFrame in GreenplumPython.pandas does not have an index column"
        assert subset is not None  # FIXME: select distinct *
        df = self._dataframe.distinct_on(*subset)
        return DataFrame(df)

    def merge(
        self,
        right: "DataFrame",
        how: Literal["left", "right", "outer", "inner", "cross"] = "inner",
        on: Union[str, List[str]] = None,
        left_on: str = None,
        right_on: str = None,
        left_index: bool = False,  # Not Used
        right_index: bool = False,  # Not Used
        sort: bool = False,
        suffixes: str = "",
        copy: bool = True,  # Not Used
        indicator: bool = False,  # Not Used
        validate: Optional[str] = None,  # Not Used
    ):
        """
        Join the current :class:`DataFrame` with another using the given arguments.

        N.B: This function can't handle yet automatically suffixes when having the same column names on both sides.

        This aligns with the function "pandas.DataFrame.merge()"
        <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html>`_.

        Returns:
            :class:`Dataframe`: class:`DataFrame` of the two merged class:`DataFrame`.

        Example:
            .. highlight:: python
            .. code-block::  python
                >>> import greenplumpython.pandas as pd
                >>> students = [("alice", 18), ("bob", 19), ("carol", 19)]
                >>> db.create_dataframe(rows=students, column_names=["name", "age"]).save_as("student", column_names=["name_1", "age_1"])
                >>> db.create_dataframe(rows=students, column_names=["name", "age"]).save_as("student_2", column_names=["name_2", "age_2"])
                >>> student = pd.read_sql("student", conn)
                >>> student_2 = pd.read_sql("student_2", conn)
                >>> student.merge(
                        student_2,
                        how="inner",
                        left_on="age_1",
                        right_on="age_2",
                    )
                    ---------------------------------
                     name_1 | age_1 | name_2 | age_2
                    --------+-------+--------+-------
                     alice  |    18 | alice  |    18
                     bob    |    19 | carol  |    19
                     bob    |    19 | bob    |    19
                     carol  |    19 | carol  |    19
                     carol  |    19 | bob    |    19
                    ---------------------------------
                    (5 rows)

        """
        how = "full" if how == "outer" else how
        assert (
            suffixes == ""
        ), "Can't support yet automatically handle overlapping column's suffixes"
        assert (
            sort is False
        ), "Can't support yet order result DataFrame lexicographically by the join key"
        assert (
            left_index is False and right_index is False
        ), "DataFrame in GreenplumPython.pandas does not have an index column"
        assert on is None, "Can't support duplicate columns name in both DataFrame"
        df = self._dataframe.join(
            right._dataframe, how=how, cond=lambda s, o: s[left_on] == o[right_on]
        )
        return DataFrame(df)

    def head(self, n: int) -> "DataFrame":
        """
        Return the first n unordered rows.

        Returns:
            :class:`Dataframe`: The first n unordered rows of class:`DataFrame`.

        """
        df = self._dataframe[:n]
        return DataFrame(df)

    def __repr__(self):
        return self._dataframe.__repr__()

    def _repr_html_(self):
        return self._dataframe._repr_html_()  # type: ignore

    def __iter__(self):
        return self._dataframe.__iter__()

    def apply(self):
        pass

    def agg(self):
        pass


def read_sql(
    sql: str,
    conn: engine,
    index_col: Union[str, list[str]] = None,
    coerce_float: bool = True,
    params: Optional[List[str]] = None,
    parse_dates: Optional[List[str]] = None,
    columns: Optional[list[str]] = None,
    chunksize: Optional[int] = None,
):
    """
    Read SQL query or database table into a :class:`DataFrame`.

        This aligns with the function "pandas.read_sql()"
        <https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html?highlight=read_sql>`_.

        Returns:
            :class:`Dataframe`.

        Example:
            .. highlight:: python
            .. code-block::  python

            >>> columns = {"a": [1, 2, 3], "b": [1, 2, 3]}
            >>> db.create_dataframe(columns=columns).save_as("test_read_sql", column_names=["a", "b"])
            >>> pd.read_sql("test_read_sql", conn)
            -------
             a | b
            ---+---
             2 | 2
             3 | 3
             1 | 1
            -------
            (3 rows)
            >>> pd.read_sql('SELECT unnest(ARRAY[1, 2, 3]) AS "a",unnest(ARRAY[1,2,3]) AS "b"', conn)
            -------
             a | b
            ---+---
             1 | 1
             2 | 2
             3 | 3
            -------
            (3 rows)

    """
    return DataFrame.from_sql(sql, conn)
