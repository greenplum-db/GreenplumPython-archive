"""
DataFrame is the core data structure in GreenplumPython.

Conceptually, a DataFrame is a two-dimensional structure containing data.

In the data science world, a DataFrame in GreenplumPython, referred to as :code:`gp.DataFrame`, is similar to a 
`DataFrame in pandas <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_,
except that:

- Data in a :code:`gp.DataFrame` is lazily evaluated rather than eagerly. That is, they are computed only when they are
  observed. This can improve efficiency in many cases.
- Data in a :code:`gp.DataFrame` is located and manipulated on a remote database system rather than locally. As a consequence,

    - Retrieving them from the database system can be expensive. Therefore, once the data of a
      :code:`gp.DataFrame` is fetched from the database system, it will be cached locally for later use.
    - They might be modified concurrently by other users of the database system. You might
      need to use :meth:`~dataframe.DataFrame.refresh()` to sync the updates if the data becomes stale.

In the database world, a :code:`gp.DataFrame` is similar to a **materialized view** in a database system
in that:

- They both result from a possibly complex query.
- They both hold data, as opposed to views.
- The data can become stale due to concurrent modification. And the :meth:`~dataframe.DataFrame.refresh()` method
  is similar to the :code:`REFRESH MATERIALIZED VIEW` `command in PostgreSQL
  <https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html>`_ for syncing updates.
"""
import json
from collections import abc
from functools import partialmethod, singledispatchmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
    overload,
)

if TYPE_CHECKING:
    from greenplumpython.func import FunctionExpr

from uuid import uuid4

from psycopg2.extras import RealDictRow

from greenplumpython.col import Column, Expr
from greenplumpython.db import Database
from greenplumpython.expr import _serialize
from greenplumpython.group import DataFrameGroupingSet
from greenplumpython.order import DataFrameOrdering
from greenplumpython.row import Row


class DataFrame:
    """Representation of GreenplumPython DataFrame object."""

    def __init__(
        self,
        query: str,
        parents: List["DataFrame"] = [],
        name: Optional[str] = None,
        schema: Optional[str] = None,
        db: Optional[Database] = None,
        columns: Optional[Iterable[Column]] = None,
    ) -> None:
        # FIXME: Add doc
        # noqa
        self._query = query
        self._parents = parents
        self._schema = schema
        self._name = "cte_" + uuid4().hex if name is None else name
        self._columns = columns
        self._contents: Optional[Iterable[RealDictRow]] = None
        if any(parents):
            self._db = next(iter(parents))._db
        else:
            self._db = db

    @singledispatchmethod
    def _getitem(self, _) -> "DataFrame":
        raise NotImplementedError()

    @_getitem.register(abc.Callable)  # type: ignore reportMissingTypeArgument
    def _(self, predicate: Callable[["DataFrame"], Expr]):
        return self.where(predicate)

    @_getitem.register(list)
    def _(self, column_names: List[str]) -> "DataFrame":
        targets_str = [_serialize(self[col]) for col in column_names]
        return DataFrame(
            f"""
                SELECT {','.join(targets_str)}
                FROM {self._name}
            """,
            parents=[self],
        )

    @_getitem.register(str)
    def _(self, column_name: str) -> "DataFrame":
        return Column(column_name, self)

    @_getitem.register(slice)
    def _(self, rows: slice) -> "DataFrame":
        if rows.step is not None:
            raise NotImplementedError()
        offset_clause = "" if rows.start is None else f"OFFSET {rows.start}"
        limit_clause = (
            ""
            if rows.stop is None
            else f"LIMIT {rows.stop if rows.start is None else rows.stop - rows.start}"
        )
        return DataFrame(
            f"SELECT * FROM {self._name} {limit_clause} {offset_clause}",
            parents=[self],
        )

    @overload
    def __getitem__(self, _) -> "DataFrame":
        # noqa
        ...

    @overload
    def __getitem__(self, column_names: List[str]) -> "DataFrame":
        # noqa
        ...

    @overload
    def __getitem__(self, predicate: Callable[["DataFrame"], Expr]) -> "DataFrame":
        # noqa
        ...

    @overload
    def __getitem__(self, column_name: str) -> Expr:
        # noqa
        ...

    @overload
    def __getitem__(self, rows: slice) -> "DataFrame":
        # noqa
        ...

    def __getitem__(self, _):
        """
        Select parts of the :class:`~dataframe.DataFrame`.

        Returns: :class:`~col.Column` or :class:`~dataframe.DataFrame`

            - Returns: a :class:`~col.Column` of the current :class:`~dataframe.DataFrame`

              When want to use :class:`~col.Column` for computation rather for observing data:

              Args: key: :class:`string`

            .. code-block::  python

               id_col = dataframe["id"]


            - Returns: a new :class:`~dataframe.DataFrame` from the current :class:`~dataframe.DataFrame` per the type of key:

                - When want to retrieve some columns of :class:`~dataframe.DataFrame`:

                  Args: key: :class:`list` of columns

                  Returns: :class:`~dataframe.DataFrame` with the subset of columns, a.k.a. targets

                .. code-block::  python

                   id_dataframe = dataframe[["id"]]


                - When want to filter :class:`~dataframe.DataFrame` on :class:`~col.Column` with conditions:

                  Args: key: :class:`~expr.Expr`

                  Returns: :class:`~dataframe.DataFrame` with subset of rows per the value of the Expr

                .. code-block::  python

                   id_cond_dataframe = dataframe[lambda t: t["id"] == 0]


                - When want to retrieve a portion of :class:`DataFrame`:

                  Args: key: :class:`slice`

                  Returns: :class:`~dataframe.DataFrame` with the portion of consecutive rows

                .. code-block::  python

                   slice_dataframe = tab[2:5]


        """
        return self._getitem(_)

    def __repr__(self) -> str:
        # noqa
        """
        :meta private:

        Return a string representation for a dataframe
        """
        contents = list(self)
        row_num_string = f"({len(contents)} row{'s' if len(contents) != 1 else ''})\n"
        if len(contents) == 0:  # DataFrame is empty
            return "----\n" "----\n" "----\n" + row_num_string

        # To align each column, we use a two-pass algorithm:
        # 1. Iterate over the DataFrame to find the max width for each column; and
        # 2. Convert the datum in each column to str within the width.
        first_row: Row = contents[0]
        widths = {col: len(col) for col in first_row} if len(first_row) > 0 else {None: 2}
        for row in contents:
            for name, val in row.items():
                widths[name] = max(widths[name], len(str(val)))

        # For Python >= 3.7, dict.items() and dict.values() will preserves the
        # input order.
        def line(sep: str) -> str:
            return (
                sep.join(["-{:{}}-".format("-" * width, width) for width in widths.values()]) + "\n"
            )

        df_string = line("-")
        df_string += (
            "|".join(
                [
                    " {:{}} ".format(col if col is not None else "", width)
                    for col, width in widths.items()
                ]
            )
            + "\n"
        )
        df_string += line("+")
        for row in contents:
            df_string += (
                "|".join(
                    [
                        (" {:{}} ").format(
                            "{}".format(
                                ""
                                if col_name is None
                                else row[col_name]
                                if isinstance(row[col_name], list)
                                else ("{:{}}").format(
                                    row[col_name] if row[col_name] is not None else "",
                                    widths[col_name],
                                )
                            ),
                            widths[col_name],
                        )
                        for col_name in widths
                    ]
                )
            ) + "\n"
        df_string += line("-")
        df_string += row_num_string
        return df_string

    def _repr_html_(self) -> str:
        # noqa
        """:meta private:"""
        repr_html_str = ""
        ret = list(self)
        if len(ret) != 0:
            repr_html_str = "<table>\n"
            repr_html_str += "\t<tr>\n"
            repr_html_str += ("\t\t<th>{:}</th>\n" * len(ret[0])).format(*((ret[0])))
            repr_html_str += "\t</tr>\n"
            for row in ret:
                content = [row[c] for c in row]
                repr_html_str += "\t<tr>\n"
                for c in content:
                    if isinstance(c, list):
                        repr_html_str += ("\t\t<td>{:}</td>\n").format("{}".format(c))  # type: ignore
                    else:
                        repr_html_str += ("\t\t<td>{:}</td>\n").format(c if c is not None else "")  # type: ignore
                repr_html_str += "\t</tr>\n"
            repr_html_str += "</table>"
        return repr_html_str

    # FIXME: Add test
    def where(self, predicate: Callable[["DataFrame"], "Expr"]) -> "DataFrame":
        """
        Filter the :class:`DataFrame` by applying the predicate.

        Return the :class:`~dataframe.DataFrame` filtered by :class:`~expr.Expr`.

        Args:
            predicate: :class:`~expr.Expr` : where condition statement.

        Returns:
            DataFrame: :class:`~dataframe.DataFrame` filtered according to :class:`~expr.Expr` that is
            passed in argument.

        Example:
            .. highlight:: python
            .. code-block::  python

                >>> rows = [(i,) for i in range(-10, 10)]
                >>> series = db.create_dataframe(rows=rows, column_names=["id"])
                >>> series.where(lambda df: df["id"] > 0)
                ----
                 id
                ----
                  1
                  2
                  3
                  4
                  5
                  6
                  7
                  8
                  9
                ----
                (9 rows)

        """
        v = predicate(self)
        assert v._dataframe == self, "Predicate must based on current dataframe"
        parents = [self]
        if v._other_dataframe is not None and self._name != v._other_dataframe._name:
            parents.append(v._other_dataframe)
        return DataFrame(f"SELECT * FROM {self._name} WHERE {v._serialize()}", parents=parents)

    def apply(
        self,
        func: Callable[["DataFrame"], "FunctionExpr"],
        expand: bool = False,
        column_name: Optional[str] = None,
    ) -> "DataFrame":
        """
        Apply a dataframe function to the self :class:`~dataframe.DataFrame`.

        Args:
            func: A Python function that

                - takes the self :class:`~dataframe.DataFrame` as the only parameter, and
                - returns the result of a dataframe function, which can be a\
                    :class:`~func.NormalFunction`, a :class:`~func.AggregateFunction` or a\
                    :class:`~func.ColumnFunction`

            expand: whether to expand the multi-valued result into columns of
                the resulting :class:`~dataframe.DataFrame`.
            column_name: name of the column of the return value in the
                resulting :class:`~dataframe.DataFrame`.

        Returns:
            A :class:`~dataframe.DataFrame` of returned values of the function.

        Example:

            To compute the absolute value of a serie of numbers:

            .. highlight:: python
            .. code-block::  python

                >>> rows = [(i,) for i in range(-10, 0)]
                >>> series = db.create_dataframe(rows=rows, column_names=["id"])
                >>> abs = gp.function("abs")
                >>> result = series.apply(lambda df: abs(df["id"]))
                >>> result
                -----
                 abs
                -----
                  10
                  9
                  8
                  7
                  6
                  5
                  4
                  3
                  2
                  1
                -----
                (10 rows)

            To transform colums into other types, see the following example. Suppose *label* function takes a `str` and a
            `int`, it joins them into a string and returns:

            .. highlight:: python
            .. code-block::  python

                >>> rows = [(i,) for i in range(10)]
                >>> series = db.create_dataframe(rows=rows, column_names=["id"])
                >>> @gp.create_function
                ... def label(prefix: str, id: int) -> str:
                ...     prefix = "id"
                ...     return f"{prefix}_{id}"

                >>> result = series.apply(lambda t: label("label", t["id"]),
                ...                       column_name = "label")
                >>> result
                -------
                 label
                -------
                 id_0
                 id_1
                 id_2
                 id_3
                 id_4
                 id_5
                 id_6
                 id_7
                 id_8
                 id_9
                -------
                (10 rows)
        """
        # We need to support calling functions with constant args or even no
        # arg. For example: SELECT count(*) FROM t; In that case, the
        # arguments do not contain information on any dataframe or any database.
        # As a result, the generated SQL cannot be executed.
        #
        # To fix this, we need to pass the dataframe to the resulting FunctionExpr
        # explicitly.
        return func(self).bind(dataframe=self).apply(expand=expand, column_name=column_name)

    def assign(self, **new_columns: Callable[["DataFrame"], Any]) -> "DataFrame":
        """
        Add new columns to the current :class:`~dataframe.DataFrame`. Existing columns cannot be reassigned.

        Args:
            new_columns: a `dict` whose keys are column names and values are :class:`Callable` which
                         returns column data when is applied to the current :class:`~dataframe.DataFrame`.

        Returns:
            DataFrame: a new :class:`~dataframe.DataFrame` including the new assigned columns.

        Example:
            .. highlight:: python
            .. code-block::  python

                >>> rows = [(i,) for i in range(-10, 0)]
                >>> series = db.create_dataframe(rows=rows, column_names=["id"])
                >>> abs = gp.function("abs")
                >>> results = series.assign(abs=lambda nums: abs(nums["id"]))
                >>> results
                -----------
                 id  | abs
                -----+-----
                 -10 |  10
                 -9  |   9
                 -8  |   8
                 -7  |   7
                 -6  |   6
                 -5  |   5
                 -4  |   4
                 -3  |   3
                 -2  |   2
                 -1  |   1
                -----------
                (10 rows)

        """
        if len(new_columns) == 0:
            return self
        targets: List[str] = []
        other_parents: Dict[str, DataFrame] = {}
        if len(new_columns):
            for k, f in new_columns.items():
                v: Any = f(self)
                if isinstance(v, Expr):
                    assert (
                        v._dataframe is None or v._dataframe == self
                    ), "Newly included columns must be based on the current dataframe"
                    if v._other_dataframe is not None and v._other_dataframe._name != self._name:
                        if v._other_dataframe._name not in other_parents:
                            other_parents[v._other_dataframe._name] = v._other_dataframe
                targets.append(f"{_serialize(v)} AS {k}")
            schema, df_name = self._qualified_name
            qualified_name = f'"{schema}"."{df_name}"' if schema is not None else f'"{df_name}"'
            return DataFrame(
                f"SELECT *, {','.join(targets)} FROM {qualified_name}",
                parents=[self] + list(other_parents.values()),
            )

    def order_by(
        self,
        column_name: str,
        ascending: Optional[bool] = None,
        nulls_first: Optional[bool] = None,
        operator: Optional[str] = None,
    ) -> DataFrameOrdering:
        """
        Sort :class:`DataFrame` based on the configuration.

        Args:
            column_name: name of column to order the dataframe by.
            ascending: Define ascending of order, True = ASC / False = DESC.
            nulls_first: Define if nulls will be ordered first or last, True = First / False = Last.
            operator: Define order by using operator. **Can't combine with ascending.**

        Returns:
            :class:`~order.DataFrameOrdering`: Specification on ordering of the
            current :class:`~dataframe.DataFrame`.

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> columns = {"id": [3, 1, 2], "b": [1, 2, 3]}
                >>> t = gp.DataFrame.from_columns(columns, db=db)
                >>> result = t.order_by("id")[:]
                >>> result
                --------
                 id | b
                ----+---
                  1 | 2
                  2 | 3
                  3 | 1
                --------
                (3 rows)
        """
        # State transition diagram:
        # DataFrame --order_by()-> DataFrameOrdering --head()-> DataFrame
        if ascending is not None and operator is not None:
            raise Exception(
                "Could not use 'ascending' and 'operator' together to order by one column"
            )
        return DataFrameOrdering(
            self,
            [column_name],
            [ascending],
            [nulls_first],
            [operator],
        )

    def join(
        self,
        other: "DataFrame",
        how: Literal["", "left", "right", "outer", "inner", "cross"] = "",
        cond: Optional[Callable[["DataFrame", "DataFrame"], Expr]] = None,
        on: Optional[Union[str, Iterable[str]]] = None,
        self_columns: Union[Dict[str, Optional[str]], Set[str]] = {"*"},
        other_columns: Union[Dict[str, Optional[str]], Set[str]] = {"*"},
    ) -> "DataFrame":
        """
        Join the current :class:`~dataframe.DataFrame` with another using the given arguments.

        Args:
            other: :class:`~dataframe.DataFrame` to join with
            how: How the two :class:`~dataframe.DataFrame` are joined. The value can be one of:

                - `"INNER"`: inner join,
                - `"LEFT"`: left outer join,
                - `"RIGHT"`: right outer join,
                - `"FULL"`: full outer join, or
                - `"CROSS"`: cross join, i.e. the Cartesian product

                The default value `""` is equivalent to "INNER".

            cond: :class:`Callable` lambda function as the join condition
            on: a list of column names that exists in both `DataFrames` to join on.
                :code:`cond` and :code:`on` cannot be used together.
            self_columns: A :class:`dict` whose keys are the column names of
                the current dataframe to be included in the resulting
                dataframe. The value, if not `None`, is used for renaming
                the corresponding key to avoid name conflicts. Asterisk :code:`"*"`
                can be used as a key to indicate all columns.
            other_columns: Same as `self_columns`, but for the **other** :class:`~dataframe.DataFrame`.

        Note:
            When using :code:`"*"` as key in `self_columns` or `other_columns`,
            please ensure that there will not be more than one column with the
            same name by applying proper renaming. Otherwise, there will be an
            error.

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> age_rows = [("alice", 18), ("bob", 19), ("carol", 19)]
                >>> student = gp.DataFrame.from_rows(
                ...     age_rows, column_names=["name", "age"], db=db)
                >>> result = student.join(
                ...     student,
                ...     on="age",
                ...     self_columns={"*"},
                ...     other_columns={"name": "name_2"})
                >>> result
                ----------------------
                 name  | age | name_2
                -------+-----+--------
                 alice |  18 | alice
                 bob   |  19 | carol
                 bob   |  19 | bob
                 carol |  19 | carol
                 carol |  19 | bob
                ----------------------
                (5 rows)
        """
        # FIXME : Raise Error if target columns don't exist
        assert how.upper() in [
            "",
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "CROSS",
        ], "Unsupported join type"
        assert cond is None or on is None, 'Cannot specify "cond" and "using" together'

        def bind(t: DataFrame, columns: Union[Dict[str, Optional[str]], Set[str]]) -> List[str]:
            target_list: List[str] = []
            for k in columns:
                col: Column = t[k]
                v = columns[k] if isinstance(columns, dict) else None
                target_list.append(col._serialize() + (f' AS "{v}"' if v is not None else ""))
            return target_list

        other_name = other._name if self._name != other._name else "cte_" + uuid4().hex
        other_clause = (
            other._name if self._name != other._name else self._name + " AS " + other_name
        )
        target_list = bind(self, self_columns) + bind(
            DataFrame(query="", name=other_name), other_columns
        )
        # ON clause in SQL uses argument `cond`.
        sql_on_clause = f"ON {cond(self, other)._serialize()}" if cond is not None else ""
        join_column_names = (
            (f'"{on}"' if isinstance(on, str) else ",".join([f'"{name}"' for name in on]))
            if on is not None
            else None
        )
        # USING clause in SQL uses argument `on`.
        sql_using_clause = f"USING ({join_column_names})" if join_column_names is not None else ""
        return DataFrame(
            f"""
                SELECT {",".join(target_list)}
                FROM {self._name} {how} JOIN {other_clause} {sql_on_clause} {sql_using_clause}
            """,
            parents=[self, other],
        )

    inner_join = partialmethod(join, how="INNER")
    """
    Inner joins the current :class:`~dataframe.DataFrame` with another :class:`~dataframe.DataFrame`.

    Equivalent to calling :meth:`~dataframe.DataFrame.join` with `how="INNER"`.
    """

    left_join = partialmethod(join, how="LEFT")
    """
    Left-outer joins the current :class:`~dataframe.DataFrame` with another :class:`~dataframe.DataFrame`.

    Equivalent to calling :meth:`~dataframe.DataFrame.join` with `how="LEFT"`.
    """

    right_join = partialmethod(join, how="RIGHT")
    """
    Right-outer joins the current :class:`~dataframe.DataFrame` with another :class:`~dataframe.DataFrame`.

    Equivalent to calling :meth:`~dataframe.DataFrame.join` with `how="RIGHT"`.
    """

    full_join = partialmethod(join, how="FULL")
    """
    Full-outer joins the current :class:`~dataframe.DataFrame` with another :class:`~dataframe.DataFrame`.

    Equivalent to calling :meth:`~dataframe.DataFrame.join` with argutment `how="FULL"`.
    """

    cross_join = partialmethod(join, how="CROSS", cond=None, on=None)
    """
    Cross joins the current :class:`~dataframe.DataFrame` with another :class:`~dataframe.DataFrame`,
    i.e. the Cartesian product.

    Equivalent to calling :meth:`~dataframe.DataFrame.join` with `how="CROSS"`.
    """

    @property
    def _qualified_name(self) -> Tuple[Optional[str], str]:
        """
        Return the schema name and name of :class:`~dataframe.DataFrame`.

        Returns:
            Tuple[str, str]: schema name and :class:`~dataframe.DataFrame`'s name.
        """
        return self._schema, self._name

    # @property
    # def columns(self) -> Optional[Iterable[Column]]:
    #     """
    #     Returns its :class:`~expr.Column` name of :class:`DataFrame`, has
    #     results only for selected dataframe and joined dataframe with targets.

    #     Returns:
    #         Optional[Iterable[str]]: None or List of its columns names of dataframe
    #     """
    #     return self._columns

    # This is used to filter out dataframes that are derived from other dataframes.
    #
    # Actually we cannot determine if a dataframe is recorded in the system catalogs
    # without querying the db.
    def _in_catalog(self) -> bool:
        # noqa
        """:meta private:"""
        return self._query.startswith("TABLE")

    def _list_lineage(self) -> List["DataFrame"]:
        # noqa
        """:meta private:"""
        lineage: List["DataFrame"] = [self]
        dataframes_visited: Set[str] = set()
        current = 0
        while current < len(lineage):
            if (
                lineage[current]._name not in dataframes_visited
                and not lineage[current]._in_catalog()
            ):
                self._depth_first_search(lineage[current], dataframes_visited, lineage)
            current += 1
        return lineage

    def _depth_first_search(self, t: "DataFrame", visited: Set[str], lineage: List["DataFrame"]):
        # noqa
        """:meta private:"""
        visited.add(t._name)
        for i in t._parents:
            if i._name not in visited and not i._in_catalog():
                self._depth_first_search(i, visited, lineage)
        lineage.append(t)

    def _build_full_query(self) -> str:
        # noqa
        """:meta private:"""
        lineage = self._list_lineage()
        cte_list: List[str] = []
        for dataframe in lineage:
            if dataframe._name != self._name:
                cte_list.append(f"{dataframe._name} AS ({dataframe._query})")
        if len(cte_list) == 0:
            return self._query
        return "WITH " + ",".join(cte_list) + self._query

    def __iter__(self) -> "DataFrame.Iterator":
        # noqa
        """:meta private:"""
        if self._contents is not None:
            return DataFrame.Iterator(self._contents)
        assert self._db is not None
        self._contents = self._fetch()
        assert self._contents is not None
        return DataFrame.Iterator(self._contents)

    class Iterator:
        # noqa
        """:meta private:"""

        def __init__(self, contents: Iterable[RealDictRow]) -> None:
            # noqa
            """:meta private:"""
            self._proxy_iter: Iterator[RealDictRow] = iter(contents)

        def __iter__(self):
            # noqa
            return self

        def __next__(self) -> Row:
            # noqa
            """:meta private:"""

            def tuple_to_dict(json_pairs: List[tuple[str, Any]]):
                json_dict = dict(json_pairs)
                if len(json_dict) != len(json_pairs):
                    raise Exception("Duplicate column name(s) found: {}".format(json_dict.keys()))
                return json_dict

            current_row = next(self._proxy_iter)
            for name in current_row.keys():
                # According our current _fetch(), name == "to_json" will be always True
                json_dict: Dict[str, Union[Any, List[Any]]] = json.loads(
                    current_row[name], object_pairs_hook=tuple_to_dict
                )
                assert isinstance(json_dict, dict), "Failed to fetch the entire row of dataframe."
                return Row(json_dict)

    def refresh(self) -> "DataFrame":
        """
        Refresh the local cache of :class:`DataFrame`.

        After displayed dataframe, its content has been cached in local. All modifications made
        between last cache and this refresh are not updated in local.

        The local cache if used to iterate the :class:`~dataframe.DataFrame` instance locally.

        Returns:
            self

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> cursor.execute("DROP TABLE IF EXISTS const_table;")
                >>> nums = db.create_dataframe(rows=[(i,) for i in range(5)], column_names=["num"])
                >>> df = nums.save_as("const_table", column_names=["num"], temp=False).order_by("num")[:]
                >>> df
                -----
                 num
                -----
                   0
                   1
                   2
                   3
                   4
                -----
                (5 rows)
                >>> cursor.execute("INSERT INTO const_table(num) VALUES (5);")
                >>> df
                -----
                 num
                -----
                   0
                   1
                   2
                   3
                   4
                -----
                (5 rows)
                >>> df.refresh()
                -----
                 num
                -----
                   0
                   1
                   2
                   3
                   4
                   5
                -----
                (6 rows)
                >>> cursor.execute("DROP TABLE const_table;")

        Note:
            `cursor` is a predefined `Psycopg Cursor <https://www.psycopg.org/docs/cursor.html>`_
            which connects to the same database in another session with
            `auto-commit <https://www.psycopg.org/docs/connection.html?highlight=autocommit#connection.autocommit>`_
            enabled.
        """
        assert self._db is not None
        self._contents = self._fetch()
        assert self._contents is not None
        return self

    def _fetch(self, is_all: bool = True) -> Iterable[Tuple[Any]]:
        """
        Fetch rows of this GreenplumPython :class:`~dataframe.DataFrame`.

        - if is_all is True, fetch all rows at once
        - otherwise, open a CURSOR and FETCH one row at a time

        Args:
            is_all: bool: Define if fetch all rows at once

        Returns:
            Iterable[Tuple[Any]]: results of query received from database
        """
        if not is_all:
            raise NotImplementedError()
        assert self._db is not None
        output_name = "cte_" + uuid4().hex
        schema, name = self._qualified_name
        qualified_name = f'"{name}"' if schema is None else f'"{schema}"."{name}"'
        to_json_dataframe = DataFrame(
            f"SELECT to_json({output_name})::TEXT FROM {qualified_name} AS {output_name}",
            parents=[self],
        )
        result = self._db._execute(to_json_dataframe._build_full_query())
        return result if result is not None else []

    def save_as(
        self,
        table_name: str,
        column_names: List[str] = [],
        temp: bool = False,
        storage_params: dict[str, Any] = {},
        schema: Optional[str] = None,
    ) -> "DataFrame":
        """
        Save the GreenplumPython :class:`~dataframe.Dataframe` as a *table* into the database.

        And return a new instance of :class:`~dataframe.DataFrame` that represents the newly saved *table*.

        After running this function, if `temp is False`, you can also use
        :func:`~db.Database.create_dataframe(table_name)` to create a new :class:`~dataframe.Dataframe` next time.

        Args:
            table_name: name of table in database, required to be unique in the schema.
            temp: whether table is temporary. Temp tables will be dropped after the database connection is closed.
            column_names: list of column names
            storage_params: storage_parameter of gpdb, reference
                https://docs.vmware.com/en/VMware-Tanzu-Greenplum/7/greenplum-database/GUID-ref_guide-sql_commands-CREATE_TABLE_AS.html
            schema: schema of the table for avoiding name conflicts.

        Returns:
            DataFrame : :class:`~dataframe.DataFrame` represents the newly saved table

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> nums = db.create_dataframe(rows=[(i,) for i in range(5)], column_names=["num"])
                >>> df = nums.save_as("const_table", column_names=["num"], temp=True)
                >>> df.order_by("num")[:]
                -----
                 num
                -----
                   0
                   1
                   2
                   3
                   4
                -----
                (5 rows)
                >>> const_table = db.create_dataframe(table_name="const_table")
                >>> const_table.order_by("num")[:]
                -----
                 num
                -----
                   0
                   1
                   2
                   3
                   4
                -----
                (5 rows)
        """
        assert self._db is not None
        # TODO: Remove assertion below after implementing schema inference.
        assert len(column_names) > 0, "Column names of new dataframe are unknown."

        # build string from parameter dict, such as from {'a': 1, 'b': 2} to
        # 'WITH (a=1, b=2)'
        storage_parameters = (
            f"WITH ({','.join([f'{key}={storage_params[key]}' for key in storage_params.keys()])})"
        )
        df_full_name = f'"{table_name}"' if schema is None else f'"{schema}"."{table_name}"'
        self._db._execute(
            f"""
            CREATE {'TEMP' if temp else ''} TABLE {df_full_name}
            ({','.join(column_names)})
            {storage_parameters if storage_params else ''}
            AS {self._build_full_query()}
            """,
            has_results=False,
        )
        return DataFrame.from_table(table_name, self._db)

    # TODO: Uncomment or remove this.
    #
    # def create_index(
    #     self,
    #     columns: Iterable[Union["Column", str]],
    #     method: str = "btree",
    #     name: Optional[str] = None,
    # ) -> None:
    #     if not self._in_catalog():
    #         raise Exception("Cannot create index on dataframes not in the system catalog.")
    #     index_name: str = name if name is not None else "idx_" + uuid4().hex
    #     indexed_cols = ",".join([str(col) for col in columns])
    #     assert self._db is not None
    #     self._db._execute(
    #         f"CREATE INDEX {index_name} ON {self.name} USING {method} ({indexed_cols})",
    #         has_results=False,
    #     )

    def _explain(self, format: str = "TEXT") -> Iterable[Tuple[str]]:
        """
        Explain the GreenplumPython :class:`~dataframe.DataFrame`'s execution plan.

        Args:
            format: str: the format of the explain result. It can be one of "TEXT"/"XML"/"JSON"/"YAML".

        Returns:
            Iterable[Tuple[str]]: The results of *EXPLAIN* query.
        """
        assert self._db is not None
        results = self._db._execute(f"EXPLAIN (FORMAT {format}) {self._build_full_query()}")
        assert results is not None
        return results

    def group_by(self, *column_names: str) -> DataFrameGroupingSet:
        """
        Group the current GreenplumPython :class:`~dataframe.DataFrame` by `column_names`.

        Args:
            column_names: one or more column names of the :class:`~dataframe.DataFrame`.

        Returns:
            :class:`~group.DataFrameGroupingSet`: a set of groups of the current
            :class:`~dataframe.DataFrame`. Each group is identified by a different
            set of values of the columns in the arguments.
        """
        #  State transition diagram:
        #  DataFrame --group_by()-> DataFrameGroupingSet --aggregate()-> FunctionExpr
        #    ^                                                             |
        #    |------------------------- assign() or apply() ---------------|
        return DataFrameGroupingSet(self, [column_names])

    def distinct_on(self, *column_names: str) -> "DataFrame":
        """
        Deduplicate the current :class:`DataFrame` with respect to the given columns.

        This function follows the `DISTINCT ON` syntax in PostgreSQL.

        Args:
            column_names: names of the current :class:`~dataframe.DataFrame`'s columns.

        Returns:
            :class:`~dataframe.DataFrame`: the :class:`~dataframe.DataFrame` containing only the
            distinct values of the given columns.

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> students = [("alice", 18), ("bob", 19), ("carol", 19)]
                >>> student = gp.DataFrame.from_rows(students, column_names=["name", "age"], db=db)
                >>> student.distinct_on("age")[['age']]
                -----
                 age
                -----
                  18
                  19
                -----
                (2 rows)

        Note:
            Since both "bob" and "carol" have the same age 19, student.distinct_on("age")
            will randomly pick one of them for the name column. Use "[['age']]" to make
            sure the result is stable.
        """
        cols = [Column(name, self)._serialize() for name in column_names]
        return DataFrame(
            f"SELECT DISTINCT ON ({','.join(cols)}) * FROM {self._name}", parents=[self]
        )

    # dataframe_name can be table/view name
    @classmethod
    def from_table(cls, table_name: str, db: Database, schema: Optional[str] = None) -> "DataFrame":
        """
        Return a :class:`~dataframe.DataFrame` which represents the given table in the :class:`~db.Database`.

        Args:
            table_name: table name
            db: database of the table
            schema: schema of table in database

        .. code-block::  python

            df = gp.DataFrame.from_table("pg_class", db=db)

        """
        return DataFrame(
            f'TABLE "{schema}"."{table_name}"' if schema is not None else f'TABLE "{table_name}"',
            name=table_name,
            schema=schema,
            db=db,
        )

    @classmethod
    def from_rows(
        cls,
        rows: Iterable[Union[Tuple[Any], Dict[str, Any]]],
        db: Database,
        column_names: Optional[List[str]] = None,
    ) -> "DataFrame":
        """
        Return a :class:`~dataframe.DataFrame` using a given list of values.

        Args:
            rows:
                - Iterable[Tuple[Any]]: a list of row values.
                - Iterable[Dict[str, Any]]: a list of key value pairs to determine the columns and rows. The column
                  names are decided by the keys of the first dictionary element if the *column_names* is not specified.
            db: :class:`~db.Database`: database which will be associated with the :class:`~dataframe.DataFrame`.
            column_names: Iterable[str]: list of given column names.

        Returns:
            :class:`~dataframe.DataFrame`: :class:`~dataframe.DataFrame` generated with given values.

        .. highlight:: python
        .. code-block::  python

           >>> rows = [(1,), (2,), (3,)]
           >>> df = gp.DataFrame.from_rows(rows, db=db, column_names=["id"])
           >>> df
            ----
             id
            ----
             1
             2
             3
            ----
            (3 rows)

           >>> dict_list = [{"id": 1, "val": "11"}, {"id": 2, "val": "22"}]
           >>> df = gp.DataFrame.from_rows(dict_list, db=db)
           >>> df
            ----------
             id | val
            ----+-----
              1 | 11
              2 | 22
            ----------
            (2 rows)
        """
        row_tuples = [row.values() if isinstance(row, dict) else row for row in rows]
        if column_names is None:
            first_row = next(iter(rows))
            if isinstance(first_row, dict):
                column_names = first_row.keys()
        assert column_names is not None, "Column names of the DataFrame is unknown."
        rows_string = ",".join(
            [f"({','.join(_serialize(datum) for datum in row)})" for row in row_tuples]
        )
        column_names = [f'"{name}"' for name in column_names]
        columns_string = f"({','.join(column_names)})"
        table_name = "cte_" + uuid4().hex
        return DataFrame(
            f"SELECT * FROM (VALUES {rows_string}) AS {table_name} {columns_string}", db=db
        )

    @classmethod
    def from_columns(cls, columns: Dict[str, Iterable[Any]], db: Database) -> "DataFrame":
        """
        Return a :class:`~dataframe.DataFrame` using list of columns values given.

        Args:
            columns: Dict[str, List[Any]]: List of column values.
            db: :class:`~db.Database`: database which will be associated with the :class:`~dataframe.DataFrame`.

        Returns:
            :class:`~dataframe.DataFrame`: the :class:`~dataframe.DataFrame` generated with given values.

        Example:
            .. highlight:: python
            .. code-block::  python

                >>> columns = {"a": [1, 2, 3], "b": [1, 2, 3]}
                >>> t = gp.DataFrame.from_columns(columns, db=db)
                >>> t
                -------
                 a | b
                ---+---
                 1 | 1
                 2 | 2
                 3 | 3
                -------
                (3 rows)
        """
        columns_string = ",".join(
            [f'unnest({_serialize(list(v))}) AS "{k}"' for k, v in columns.items()]
        )
        return DataFrame(f"SELECT {columns_string}", db=db)
