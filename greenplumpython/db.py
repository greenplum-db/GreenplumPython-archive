"""
This  module can create a connection to a Greenplum database
"""
from typing import Iterable, List, Optional

import psycopg2
import psycopg2.extras


class Database:
    """
    Representation of Greenplum Database.
    Each Database object has an instance **conn**
    """

    def __init__(self, params) -> None:
        self._conn = psycopg2.connect(
            " ".join([f"{k}={v}" for k, v in params.items()]),
            cursor_factory=psycopg2.extras.RealDictCursor,
        )

    def execute(self, query: str, args: List = [], has_results: bool = True) -> Optional[Iterable]:
        """
        Return the result of SQL query executed in Database

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
            cursor.execute(query, args)
            return cursor.fetchall() if has_results else None

    def close(self) -> None:
        """
        Close the self database connection
        """
        self._conn.close()

    # FIXME: How to get other "global" variables, e.g. CURRENT_ROLE, CURRENT_TIMETAMP, etc.?
    def set_config(self, key: str, value):
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


def database(host="localhost",
            dbname="postgres",
            user="",
            password="",
            port=7000,
            **conn_strings) -> Database:
    """
    Create a connection using psycopg2.

    The default database name is "postgres" and default port number is 7000

    There are different ways to passing database information:

    .. code-block::  python

       db = database(host="localhost", dbname=dbname)

    If it is a connection to localhost.

    Or, a connection can be established by passing more detailed information:

    .. code-block::  python

        db = database(
                host=dbIP,
                dbname=dbname,
                user=username,
                password=password,
                port=dbPort
            )

    """
    params = {"host":host, "dbname":dbname, "port":port}
    if user is not "":
        params["user"] = user
    if password is not "":
        params["password"] = password
    return Database({**params, **conn_strings})
