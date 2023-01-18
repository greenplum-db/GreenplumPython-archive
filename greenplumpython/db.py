"""
This  module can create a connection to a Greenplum database
"""
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
    Representation of Greenplum Database.
    Each Database object has an instance **conn** which establishes a connection using psycopg2.
    """

    def __init__(self, params: Dict[str, str]) -> None:
        self._conn = psycopg2.connect(  # type: ignore
            " ".join([f"{k}={v}" for k, v in params.items()]),
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        self._conn.set_session(autocommit=True)

    def execute(self, query: str, has_results: bool = True) -> Optional[Iterable[Tuple[Any]]]:
        """
        Return the result of SQL query executed in :class:`Database`

        Args:
            query: str : SQL query
            has_results: bool : whether return None or results

        Returns:
            Optional[Iterable]: None or result of SQL query

        Example:
            .. code-block::  Python

                result = db.execute("SELECT version()")

        """

        with self._conn.cursor() as cursor:
            if config.print_sql:
                print(query)
            cursor.execute(query)
            return cursor.fetchall() if has_results else None

    def close(self) -> None:
        """
        Close the self database connection
        """
        self._conn.close()

    def create_dataframe(
        self,
        table_name: Optional[str] = None,
        rows: Optional[List[Union[Tuple[Any, ...], Dict[str, Any]]]] = None,
        columns: Optional[Dict[str, List[Any]]] = None,
        column_names: Optional[Iterable[str]] = None,
    ):
        """
        Returns a :class:`~dataframe.DataFrame` using Table name, list of values given by rows or columns

        Args:
            table_name: str: name of table in Database
            rows: List[Union[Tuple[Any, ...], Dict[str, Any]]]: a List of rows
            columns: Dict[str, List[Any]]: a dict of columns
            column_names: Iterable[str]: List of given column names

        Returns:
        .. highlight:: python
        .. code-block::  python

            >>> t_from_table = db.create_dataframe(table_name="pg_class")
            >>> rows = [(1,), (2,), (3,)]
            >>> t_from_rows = db.create_dataframe(rows=rows, column_names=["id"])
            >>> t_from_rows
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
        as_name: Optional[str] = None,
    ) -> "DataFrame":
        """
        Apply a function in database.

        Args:
            func: An aggregate function to be applied to
            expand: bool: expand field of composite returning type
            as_name: str: rename returning column

        Returns:
            DataFrame: resulted GreenplumPython DataFrame

        Example:
            .. code-block::  python

                db.apply(lambda: add(1, 2))
        """
        return func().bind(db=self).apply(expand=expand, as_name=as_name)

    def assign(self, **new_columns: Callable[[], Any]) -> "DataFrame":
        """
        Assign new columns by calling functions in database

        Args:
            new_columns: a :class:`dict` whose keys are column names and values
                are :class:`Callable` returning column data when applied to
                constant value in database.

        Returns:
            DataFrame: GreenplumPython DataFrame resulted with assigned columns

        Example:
            .. highlight:: python
            .. code-block::  python

                >>> version = gp.function("version")
                >>> db.assign(version=lambda: version())
                -----------------------------------------------------------------------------------------------------------------------------
                version
                -----------------------------------------------------------------------------------------------------------------------------
                PostgreSQL 12.9 (Debian 12.9-1.pgdg110+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 10.2.1-6) 10.2.1 20210110, 64-bit
                -----------------------------------------------------------------------------------------------------------------------------
                (1 row)
        """
        from greenplumpython.dataframe import DataFrame
        from greenplumpython.expr import Expr, serialize
        from greenplumpython.func import FunctionExpr

        targets: List[str] = []
        for k, f in new_columns.items():
            v: Any = f()
            if isinstance(v, Expr):
                assert v.dataframe is None, "New column should not depend on any dataframe."
            if isinstance(v, FunctionExpr):
                v = v.bind(db=self)
            targets.append(f"{serialize(v)} AS {k}")
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

    .. code-block::  python

       db = database(host="localhost", dbname=dbname)

    If it is a connection to localhost, password can be ignored.

    Or, a connection can be established by passing more detailed information, in this case,
    password is needed for connexion:

    .. code-block::  python

        db = database(
                host=dbIP,
                dbname=dbname,
                user=username,
                password=password,
                port=dbPort
            )

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
    return Database(params)
