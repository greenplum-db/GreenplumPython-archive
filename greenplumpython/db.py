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
    """Representation of Greenplum/PostgreSQL Database.

    Each Database object has an instance **conn** which establishes a connection using psycopg2.
    """

    def __init__(self, params: Optional[Dict[str, str]] = None, url: Optional[str] = None) -> None:
        # noqa D107
        assert (params is not None and url is None) or (params is None and url is not None)
        if url is not None:
            con_str = url
        else:
            con_str = " ".join([f"{k}={v}" for k, v in params.items()])  # type: ignore
        self._conn = psycopg2.connect(
            con_str,
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
        rows: Optional[List[Union[Tuple[Any, ...], Dict[str, Any]]]] = None,
        columns: Optional[Dict[str, Iterable[Any]]] = None,
        column_names: Optional[Iterable[str]] = None,
    ):
        """
        Create a :class:`~dataframe.DataFrame` from a database table, or a set of data.

        Args:
            table_name: str: name of table in Database
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
            return DataFrame.from_table(table_name=table_name, db=self)
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
                assert v.dataframe is None, "New column should not depend on any dataframe."
            if isinstance(v, FunctionExpr):
                v = v.bind(db=self)
            targets.append(f"{_serialize(v)} AS {k}")
        return DataFrame(f"SELECT {','.join(targets)}", db=self)


def database(
    host: str = "localhost",
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
) -> Database:
    """
    Create a connection using psycopg2 with given arguments.

    Args:
        host: str : default value = "localhost"
        dbname: str : default value = None
        user: str : Optional
        password: str : Optional
        port: int : Optional

    There are different ways to passing database information:

    .. highlight:: python
    .. code-block::  python

       db = database(host="localhost", dbname=dbname)

    `password` can be ommitted if it is empty. For `user` and `port`, the default values will be
    used.

    Or, a connection can be established by passing more detailed information, in this case,
    password is needed for connection:

    .. highlight:: python
    .. code-block::  python

        >>> my_db = database(
        ...       host=db_host,
        ...       dbname=db_name,
        ...       user=db_user,
        ...       password=db_password,
        ...       port=db_port)
        >>> my_db.close()

    """
    params = {"host": host}
    if dbname is not None:
        params["dbname"] = dbname
    if port is not None:
        params["port"] = str(port)
    if user is not None:
        params["user"] = user
    if password is not None:
        params["password"] = password
    return Database(params=params)
