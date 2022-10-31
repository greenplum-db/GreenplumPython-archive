"""
This  module can create a connection to a Greenplum database
"""
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Tuple

if TYPE_CHECKING:
    from greenplumpython.table import Table
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
            cursor.execute(query)
            return cursor.fetchall() if has_results else None

    def close(self) -> None:
        """
        Close the self database connection
        """
        self._conn.close()

    # FIXME: How to get other "global" variables, e.g. CURRENT_ROLE, CURRENT_TIMETAMP, etc.?
    def set_config(self, key: str, value: Any):
        """
        Set Database parameters

        Args:
            key: str : database parameter name
            value: undefined : value to set up

        Returns:
            void
        """
        assert isinstance(key, str)
        self.execute(f"SET {key} TO {value}", has_results=False)

    def table(self, name: str):
        """
        Returns a :class:`~table.Table` using table name and self

        Args:
            name: str : Table name

        Returns:
            Table: Table in database named **name**
        """
        from greenplumpython.table import table

        return table(name, self)

    def apply(
        self,
        func: Callable[[], "FunctionExpr"],
        expand: bool = False,
        as_name: Optional[str] = None,
    ) -> "Table":
        """
        Apply a function in database.

        Args:
            func: An aggregate function to be applied to
            expand: bool: expand field of composite returning type
            as_name: str: rename returning column

        Returns:
            Table: resulted Table

        Example:
            .. code-block::  python

                db.apply(lambda: add(1, 2))
        """
        return func().bind(db=self).apply(expand=expand, as_name=as_name)

    def assign(self, **new_columns: Callable[[], Any]) -> "Table":
        """
        Assign new columns by calling functions in database

        Args:
            new_columns: a :class:`dict` whose keys are column names and values
                are :class:`Callable` returning column data when applied to
                constant value in database.

        Returns:
            Table: Table resulted with assigned columns

        Example:
            .. code-block::  python

                version = gp.function("version")
                db.assign(version=lambda: version())

        """
        from greenplumpython.expr import Expr
        from greenplumpython.func import FunctionExpr
        from greenplumpython.table import Table
        from greenplumpython.type import to_pg_const

        targets: List[str] = []
        for k, f in new_columns.items():
            v: Any = f()
            if isinstance(v, Expr):
                assert v.table is None, "New column should not depend on any table."
            if isinstance(v, FunctionExpr):
                v = v.bind(db=self)
            targets.append(f"{v.serialize() if isinstance(v, Expr) else to_pg_const(v)} AS {k}")
        return Table(f"SELECT {','.join(targets)}", db=self)


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
