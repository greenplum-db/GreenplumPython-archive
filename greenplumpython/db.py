from typing import Iterable, List, Optional

import psycopg2
import psycopg2.extras


class Database:
    def __init__(self, **params) -> None:
        self._conn = psycopg2.connect(
            " ".join([f"{k}={v}" for k, v in params.items()]),
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        ret = list(
            self.execute(
                f"""
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
            )
        )
        self._udt = []
        assert list(ret) is not None
        for row in ret:
            self._udt.append(row["Name"])

    def execute(self, query: str, args: List = [], has_results: bool = True) -> Optional[Iterable]:
        with self._conn.cursor() as cursor:
            cursor.execute(query, args)
            return cursor.fetchall() if has_results else None

    def get_udt_list(self):
        return self._udt

    def add_udt(self, new_udt):
        self._udt.append(new_udt)

    def remove_udt(self, udt):
        self._udt.remove(udt)

    def close(self) -> None:
        self._conn.close()


def database(**conn_strings) -> Database:
    return Database(**conn_strings)
