"""
This  module can create a connection to a Greenplum database
"""
from typing import Any, Iterable, Optional, Tuple

import psycopg2
import psycopg2.extras


class Database:
    """
    Representation of Greenplum Database.
    Each Database object has an instance **conn** which establishes a connection using psycopg2.
    """

    def __init__(self, params: "dict[str, str]") -> None:
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

    def get_table(self, name: str):
        """
        Returns a :class:`~table.Table` using table name and self

        Args:
            name: str : Table name

        Returns:
            Table: Table in database named **name**
        """
        from greenplumpython.table import table

        return table(name, self)


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
