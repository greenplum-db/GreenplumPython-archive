"""
This package supports pandas compatible API which involves :class:`~pandas.dataframe.Dataframe`.

This package contains classes and functions having same names and parameters as the equivalence in pandas to
provide a Data Scientist familiar syntax. And at the same time, its DataFrame has same specifications as
GreenplumPython DataFrame, which means: Data is located and manipulated on a remote database system.

N.B.: This package contains fewer functions than GreenplumPython DataFrame, it is easy to convert to it.
"""
from typing import Any, Callable, Dict, List, Literal, Optional, Union

import greenplumpython.dataframe as dataframe
import greenplumpython.db as db
import greenplumpython.order as orderby


class DataFrame:
    """Representation of GreenplumPython Pandas Compatible DataFrame object."""

    @classmethod
    def _from_sql(cls, sql: str, con: str):
        """:meta private:."""
        c = super().__new__(cls)
        database = db.Database(url=con)
        c._dataframe = dataframe.DataFrame(query=sql, db=database)
        return c

    @classmethod
    def _from_native(cls, df: dataframe.DataFrame):
        """:meta private:."""
        c = super().__new__(cls)
        c._dataframe = df
        return c

    def __init__(self) -> None:
        """:meta private:."""
        self._dataframe: dataframe.DataFrame = None
        raise NotImplementedError

    def to_sql(
        self,
        name: str,
        con: str,
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

        This aligns with the function
        `pandas.DataFrame.to_sql() <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html>`_.

        Returns:
            int: Number of rows affected by this function.

        Example:
            .. highlight:: python
            .. code-block::  python

                >>> import greenplumpython.pandas as pd
                >>> pd_df = pd.read_sql('SELECT unnest(ARRAY[1,2,3]) AS "a",unnest(ARRAY[1,2,3]) AS "b"', con)
                >>> pd_df.to_sql(name="test_to_sql", con=con)
                3
                >>> pd.read_sql("test_to_sql", con=con)
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
        database = db.Database(url=con)
        query = self._dataframe._build_full_query()  # type: ignore
        if if_exists == "append":
            rowcount = database._execute(  # type: ignore
                f"""
                    INSERT INTO {table_name}
                    {query}
                """,
                has_results=False,
            )
        else:
            if if_exists == "replace":
                database._execute(f"DROP TABLE IF EXISTS {table_name}", has_results=False)  # type: ignore
            rowcount = database._execute(  # type: ignore
                f"""
                        CREATE TABLE {table_name}
                        AS {query}
                    """,
                has_results=False,
            )
        return rowcount

    def to_native(self) -> dataframe.DataFrame:
        """
        Convert a pandas-compatible :class:`DataFrame` to a native :class:`~dataframe.Dataframe`.

        Returns:
            a native:class:`~dataframe.Dataframe`.
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

        This aligns with the function
        `pandas.DataFrame.sort_values() <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sort_values.html>`_.

        Returns:
            :class:`~pandas.dataframe.Dataframe`: :class:`~pandas.dataframe.Dataframe`
            order by the given arguments.

        Example:
            .. highlight:: python
            .. code-block::  python

                >>> pd_df = pd.read_sql('SELECT unnest(ARRAY[3, 1, 2]) AS "id",unnest(ARRAY[1,2,3]) AS "b"', con)
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
        return DataFrame._from_native(df)

    def drop_duplicates(
        self,
        subset: Union[str, List[str], None] = None,
        keep: Literal["first", "last", False] = "first",
        inplace: bool = False,
        ignore_index: bool = True,
    ):
        """
        Return DataFrame with duplicate rows removed.

        This aligns with the function
        `pandas.DataFrame.drop_duplicates() <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html>`_.

        Returns:
            :class:`~pandas.dataframe.Dataframe`: :class:`~pandas.dataframe.Dataframe`
            with duplicates removed.

        Example:
            .. highlight:: python
            .. code-block::  python

                >>> import greenplumpython.pandas as pd
                >>> students = [("alice", 18), ("bob", 19), ("carol", 19)]
                >>> db.create_dataframe(rows=students, column_names=["name", "age"]).save_as("student", column_names=["name", "age"])
                >>> student = pd.read_sql("student", con)
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
        df: dataframe.DataFrame = self._dataframe.distinct_on(*subset)
        return DataFrame._from_native(df)

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
        Join the current :class:`~pandas.dataframe.DataFrame` with another using the given arguments.

        N.B: This function can't handle yet automatically suffixes when having the same column names on both sides.

        This aligns with the function
        `pandas.DataFrame.merge() <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html>`_.

            :class:`~pandas.dataframe.Dataframe`: :class:`~pandas.dataframe.DataFrame` of the two merged class:`DataFrame`.

        Example:
            .. highlight:: python
            .. code-block::  python

                >>> import greenplumpython.pandas as pd
                >>> students = [("alice", 18), ("bob", 19), ("carol", 19)]
                >>> db.create_dataframe(rows=students, column_names=["name", "age"]).save_as("student", column_names=["name_1", "age_1"])
                >>> db.create_dataframe(rows=students, column_names=["name", "age"]).save_as("student_2", column_names=["name_2", "age_2"])
                >>> student = pd.read_sql("student", con)
                >>> student_2 = pd.read_sql("student_2", con)
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
        df: dataframe.DataFrame = self._dataframe.join(
            right._dataframe, how=how, cond=lambda s, o: s[left_on] == o[right_on]
        )
        return DataFrame._from_native(df)

    def head(self, n: int) -> "DataFrame":
        """
        Return the first n unordered rows.

        Returns:
            :class:`~pandas.dataframe.Dataframe`: The first n unordered rows of class:`~pandas.dataframe.DataFrame`.

        """
        df: dataframe.DataFrame = self._dataframe[:n]
        return DataFrame._from_native(df)

    def __repr__(self) -> str:
        """:meta private:."""
        return self._dataframe.__repr__()

    def _repr_html_(self) -> str:
        return self._dataframe._repr_html_()  # type: ignore

    def __iter__(self):
        """:meta private:."""
        return self._dataframe.__iter__()


def read_sql(
    sql: str,
    con: str,
    index_col: Union[str, list[str]] = None,
    coerce_float: bool = True,
    params: Optional[List[str]] = None,
    parse_dates: Optional[List[str]] = None,
    columns: Optional[list[str]] = None,
    chunksize: Optional[int] = None,
):
    """
    Read SQL query or database table into a :class:`~pandas.dataframe.DataFrame`.

    This aligns with the function
    `pandas.read_sql() <https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html?highlight=read_sql>`_.

    Returns:
        :class:`~pandas.dataframe.Dataframe`.

    Example:
        .. highlight:: python
        .. code-block::  python

            >>> columns = {"a": [1, 2, 3], "b": [1, 2, 3]}
            >>> db.create_dataframe(columns=columns).save_as("test_read_sql", column_names=["a", "b"])
            >>> pd.read_sql("SELECT * FROM test_read_sql", con)
            -------
             a | b
            ---+---
             2 | 2
             3 | 3
             1 | 1
            -------
            (3 rows)
            >>> pd.read_sql('SELECT unnest(ARRAY[1, 2, 3]) AS "a",unnest(ARRAY[1,2,3]) AS "b"', con)
            -------
             a | b
            ---+---
             1 | 1
             2 | 2
             3 | 3
            -------
            (3 rows)

    """
    return DataFrame._from_sql(sql, con)  # type: ignore
