"""Manage connection to Greenplum/PostgreSQL database."""

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

from greenplumpython import config

if TYPE_CHECKING:
    from greenplumpython.dataframe import DataFrame
    from greenplumpython.func import FunctionExpr

import psycopg2
import psycopg2.extras


class Database:
    """
    Representation of a database in which data is located and computation is performed.

    Each :class:`~db.Database` object is tied to a connection to the remote database system.
    """

    def __init__(self, uri: Optional[str] = None, params: Dict[str, Optional[str]] = {}) -> None:
        # noqa
        """:meta private:"""
        if uri is not None:
            assert len(params) == 0
            dsn = uri
        else:
            assert len(params) > 0
            dsn = " ".join([f"{k}={v}" for k, v in params.items() if v is not None])
        self._conn = psycopg2.connect(
            dsn,
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        self._conn.set_session(autocommit=True)

    def _execute(self, query: str, has_results: bool = True) -> Union[Iterable[Tuple[Any]], int]:
        # noqa: D400 D202
        """
        :meta private:

        Return the result of SQL query executed in :class:`~db.Database`

        Args:
            query: str : SQL query
            has_results: bool : whether return None or results

        Returns:
            Optional[Iterable]: rowcount or result of SQL query
        """

        with self._conn.cursor() as cursor:
            if config.print_sql:
                print(query)
            cursor.execute(query)
            return cursor.fetchall() if has_results else cursor.rowcount

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()

    def create_dataframe(
        self,
        table_name: Optional[str] = None,
        schema: Optional[str] = None,
        rows: Optional[List[Union[Tuple[Any, ...], Dict[str, Any]]]] = None,
        columns: Optional[Dict[str, Iterable[Any]]] = None,
        column_names: Optional[Iterable[str]] = None,
    ):
        """
        Create a :class:`~dataframe.DataFrame` from a database table, or a set of data.

        Args:
            table_name: str: name of table in Database
            schema: str: name of schema in Database
            rows: List[Union[Tuple[Any, ...], Dict[str, Any]]]: a List of rows
            columns: Dict[str, List[Any]]: a dict of columns
            column_names: Iterable[str]: List of given column names

        Example:
            To create :class:`~dataframe.DataFrame` from a database table:

            .. highlight:: python
            .. code-block::  python

                >>> cursor.execute("DROP TABLE IF EXISTS one_column_table")
                >>> cursor.execute(
                ...     "CREATE TABLE one_column_table AS SELECT 42 as id;")
                >>> df_from_table = db.create_dataframe(table_name="one_column_table")
                >>> df_from_table
                ----
                 id
                ----
                 42
                ----
                (1 row)


            To create :class:`~dataframe.DataFrame` from a predefined set of data:

            .. highlight:: python
            .. code-block::  python

                >>> rows = [(1,), (2,), (3,)]
                >>> df_from_rows = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df_from_rows
                ----
                 id
                ----
                  1
                  2
                  3
                ----
                (3 rows)
                >>> columns = {"a": [1, 2, 3], "b": [1, 2, 3]}
                >>> t_from_columns = db.create_dataframe(columns=columns)
                >>> t_from_columns
                -------
                 a | b
                ---+---
                 1 | 1
                 2 | 2
                 3 | 3
                -------
                (3 rows)

        """
        from greenplumpython.dataframe import DataFrame

        if table_name is not None:
            assert isinstance(table_name, str), "Table name is expected to be a str."
            assert (
                rows is None and columns is None
            ), "Provisioning data is not allowed when opening existing table."
            return DataFrame.from_table(table_name=table_name, schema=schema, db=self)
        assert rows is None or columns is None, "Only one data format is allowed."
        if rows is not None:
            return DataFrame.from_rows(rows=rows, db=self, column_names=column_names)
        return DataFrame.from_columns(columns=columns, db=self)

    def apply(
        self,
        func: Callable[[], "FunctionExpr"],
        expand: bool = False,
        column_name: Optional[str] = None,
    ) -> "DataFrame":
        """
        Apply a function in database without depending on a :class:`~dataframe.DataFrame`.

        This is primarily for applying functions on adaptable Python objects
        as constants in database.

        The arguments and return type is similar to :meth:`~dataframe.DataFrame.apply`
        except that parameter :code:`func` takes no argument.

        Example:
            .. highlight:: python
            .. code-block::  python

                >>> @gp.create_function
                ... def add(a: int, b: int) -> int:
                ...     return a + b
                >>> db.apply(lambda: add(1, 2), column_name="sum")
                -----
                 sum
                -----
                   3
                -----
                (1 row)
        """
        return func().bind(db=self).apply(expand=expand, column_name=column_name)

    def assign(self, **new_columns: Callable[[], Any]) -> "DataFrame":
        """
        Assign new columns by calling functions in database.

        Args:
            new_columns: a :class:`dict` whose keys are column names and values
                are :class:`Callable` returning column data when applied to
                constant value in database.

        Returns:
            DataFrame: GreenplumPython DataFrame resulted with assigned columns

        Example:
            .. highlight:: python
            .. code-block::  python

                >>> abs = gp.function("abs")
                >>> db.assign(abs=lambda: abs(-42))
                -----
                 abs
                -----
                  42
                -----
                (1 row)
        """
        from greenplumpython.dataframe import DataFrame
        from greenplumpython.expr import Expr, _serialize
        from greenplumpython.func import FunctionExpr

        targets: List[str] = []
        for k, f in new_columns.items():
            v: Any = f()
            if isinstance(v, Expr):
                assert v._dataframe is None, "New column should not depend on any dataframe."
            if isinstance(v, FunctionExpr):
                v = v.bind(db=self)
            targets.append(f"{_serialize(v)} AS {k}")
        return DataFrame(f"SELECT {','.join(targets)}", db=self)


def database(uri: Optional[str] = None, params: Dict[str, Optional[str]] = {}) -> Database:
    """
    Open a connection to database with connection URI or parameters.

    Args:
        uri: connection URI to the database. Please refer to the libpq documentation on `connection
            URI <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING/>`_
            for detailed usage.
        params: connection parameters to the database. Please refer to the libpq documentation on
            `parameter keywords
            <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS/>`_ for
            detailed usage. The parameter will be ignored and will **not** be passed to the remote
            database server if its value is :code:`None`.

    """
    return Database(uri=uri, params=params)
