from typing import Iterable, Optional, Tuple
from uuid import uuid4

from . import db, expr


class Table:
    def __init__(
        self,
        query: str,
        parents: Iterable["Table"] = [],
        name: Optional[str] = None,
        db: Optional[db.Database] = None,
    ) -> None:
        self._query = query
        self._parents = parents
        self._name = "cte_" + uuid4().hex if name is None else name
        if any(parents):
            self._db = next(iter(parents))._db
        else:
            self._db = db

    def __getitem__(self, key):
        """
        Returns
        - a Column of the current Table if key is string, or
        - a new Table from the current Table per the type of key:
            - if key is a list, then SELECT a subset of columns, a.k.a. targets;
            - if key is an Expr, then SELECT a subset of rows per the value of the Expr;
            - if key is a slice, then SELECT a portion of consecutive rows
        """
        if isinstance(key, str):
            return expr.Column(key, self)
        if isinstance(key, list):
            return self.select(key)
        if isinstance(key, expr.Expr):
            return self.filter(key)
        if isinstance(key, slice):
            raise NotImplementedError()

    # FIXME: Add test
    def filter(self, expr: expr.Expr) -> "Table":
        return Table(f"SELECT * FROM {self._name} WHERE {str(expr)}", parents=[self])

    # FIXME: Add test
    def select(self, target_list: Iterable) -> "Table":
        return Table(
            f"""
                SELECT {','.join([str(target) for target in target_list])} 
                FROM {self._name}
            """,
            parents=[self],
        )

    def column_names(self) -> "Table":
        if any(self._parents):
            raise NotImplementedError()
        return Table(
            f"""
                SELECT column_name
                FROM information_schema.columns 
                WHERE table_name = quote_ident('{self._name}')
            """,
            db=self._db,
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def db(self) -> db.Database:
        return self._db

    def _list_lineage(self) -> Iterable["Table"]:
        lineage = [self]
        tables_visited = set()
        current = 0
        while current < len(lineage):
            for table in lineage[current]._parents:
                if table._name not in tables_visited:
                    lineage.append(table)
                    tables_visited.add(table._name)
            current += 1
        return lineage

    def _build_full_query(self) -> str:
        if not any(self._parents):
            return self._query
        lineage = self._list_lineage()
        cte_list = []
        for table in reversed(lineage):
            if table._name != self._name:
                cte_list.append(f"{table._name} AS ({table._query})")
        return "WITH " + ",".join(cte_list) + self._query

    def fetch(self, all: bool = True) -> Iterable:
        """
        Fetch rows of this table.
        - if all is True, fetch all rows at once
        - otherwise, open a CURSOR and FETCH one row at a time
        """
        if not all:
            raise NotImplementedError()
        return self._db.execute(self._build_full_query())

    def save_as(
        self, table_name: str, temp: bool = True, column_names: Iterable[str] = []
    ) -> "Table":
        self._db.execute(
            f"""
            CREATE {'TEMP' if temp else ''} TABLE {table_name} ({','.join(column_names)}) 
            AS {self._build_full_query()}
            """,
            has_results=False,
        )
        return table(table_name, self._db)


# table_name can be table/view name
def table(name: str, db: db.Database) -> Table:
    return Table(f"TABLE {name}", name=name, db=db)


def values(rows: Iterable[Tuple], db: db.Database) -> Table:
    return Table(
        f"VALUES {','.join(['(' + ','.join(str(datum) for datum in row) + ')' for row in rows])}",
        db=db,
    )
