from typing import Iterable, List, Literal, Optional

import psycopg2
import psycopg2.extras


class Database:
    def __init__(self, **params) -> None:
        self._conn = psycopg2.connect(
            " ".join([f"{k}={v}" for k, v in params.items()]),
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        query = f"""
                SELECT n.nspname as "Schema",
                  pg_catalog.format_type(t.oid, NULL) AS "Name",
                  pg_catalog.obj_description(t.oid, 'pg_type') as "Description"
                FROM pg_catalog.pg_type t
                     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
                WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
                  AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
                      AND n.nspname <> 'pg_catalog'
                      AND n.nspname <> 'information_schema'
                  AND pg_catalog.pg_type_is_visible(t.oid)
                ORDER BY 1, 2;
            """
        assert self._conn is not None
        with self._conn.cursor() as cursor:
            cursor.execute(query)
            res = cursor.fetchall()
        assert res is not None
        ret = list(res)
        # udt_list: [Literal["Name"]]
        assert list(ret) is not None
        if len(ret) > 0:
            udt_list = [row["Name"] for row in ret]
        else:
            udt_list = []
        self._udt_list = udt_list

    def execute(self, query: str, args: List = [], has_results: bool = True) -> Optional[Iterable]:
        with self._conn.cursor() as cursor:
            cursor.execute(query, args)
            return cursor.fetchall() if has_results else None

    def get_udt_list(self):
        return self._udt_list

    def add_udt(self, new_udt):
        self._udt_list.append(new_udt)

    def remove_udt(self, udt):
        self._udt_list.remove(udt)

    def close(self) -> None:
        self._conn.close()

    # FIXME: How to get other "global" variables, e.g. CURRENT_ROLE, CURRENT_TIMETAMP, etc.?
    def set_config(self, key: str, value):
        assert isinstance(key, str)
        self.execute(f"SET {key} TO {value}", has_results=False)


def database(**conn_strings) -> Database:
    return Database(**conn_strings)
