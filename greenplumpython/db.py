from typing import Iterable, List, Optional

import psycopg2
import psycopg2.extras


class Database:
    def __init__(self, **params) -> None:
        self._conn = psycopg2.connect(
            " ".join([f"{k}={v}" for k, v in params.items()]),
            cursor_factory=psycopg2.extras.RealDictCursor,
        )

    def execute(self, query: str, args: List = [], has_results: bool = True) -> Optional[Iterable]:
        with self._conn.cursor() as cursor:
            cursor.execute(query, args)
            return cursor.fetchall() if has_results else None

    def close(self) -> None:
        self._conn.close()

    # FIXME: Should we use this to set GUCs?
    #
    # How to get other "global" variables, e.g. CURRENT_ROLE, CURRENT_TIMETAMP, etc.?
    def __setitem__(self, key, value):
        if not isinstance(key, str):
            raise NotImplementedError()
        self.execute(f"SET {key} TO {value}", has_results=False)


def database(**conn_strings) -> Database:
    return Database(**conn_strings)
