from typing import Iterable, List, Optional, Tuple
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

    def as_name(self, name_as: str) -> "Table":
        return Table(f"SELECT * FROM {self.name}", parents=[self], name=name_as, db=self._db)

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

    def _join(
        self,
        other: "Table",
        targets1: List = ["*"],
        targets2: List = ["*"],
        how: Optional[str] = "NATURAL JOIN",
        on_str: Optional[str] = None,
    ) -> "Table":
        targets_str1 = ",".join([self.name + ".{}".format(target) for target in targets1])
        targets_str2 = ",".join([other.name + ".{}".format(target) for target in targets2])
        targets = ",".join([targets_str1, targets_str2])
        select_str = "*" if not targets else targets
        return Table(
            f"""
                SELECT {select_str} 
                FROM {self.name} {how} {other.name}
                {str(on_str)}  
            """,
            parents=[self, other],
        )

    def inner_join(
        self,
        other: "Table",
        cond: Optional[expr.Expr] = None,
        targets1: List = ["*"],
        targets2: List = ["*"],
    ):
        on_str = " ".join(["ON", str(cond)])
        return self._join(other, targets1, targets2, "INNER JOIN", on_str)

    def left_join(
        self,
        other: "Table",
        cond: Optional[expr.Expr] = None,
        targets1: List = ["*"],
        targets2: List = ["*"],
    ):
        on_str = " ".join(["ON", str(cond)])
        return self._join(other, targets1, targets2, "LEFT JOIN", on_str)

    def right_join(
        self,
        other: "Table",
        cond: Optional[expr.Expr] = None,
        targets1: List = ["*"],
        targets2: List = ["*"],
    ):
        on_str = " ".join(["ON", str(cond)])
        return self._join(other, targets1, targets2, "RIGHT JOIN", on_str)

    def full_outer_join(
        self,
        other: "Table",
        cond: Optional[expr.Expr] = None,
        targets1: List = ["*"],
        targets2: List = ["*"],
    ):
        on_str = " ".join(["ON", str(cond)])
        return self._join(other, targets1, targets2, "FULL JOIN", on_str)

    def natural_join(
        self,
        other: "Table",
        targets1: List = ["*"],
        targets2: List = ["*"],
    ):
        on_str = ""
        return self._join(other, targets1, targets2, "FULL JOIN", on_str)

    def cross_join(
        self,
        other: "Table",
        targets1: List = ["*"],
        targets2: List = ["*"],
    ):
        on_str = ""
        return self._join(other, targets1, targets2, "CROSS JOIN", on_str)

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
    def db(self) -> Optional[db.Database]:
        return self._db

    def _list_lineage(self) -> List["Table"]:
        lineage = []
        lineage.append(self)
        tables_visited = set()
        current = 0
        while current < len(lineage):
            for table_ in lineage[current]._parents:
                if table_.name not in tables_visited:
                    lineage.append(table_)
                    tables_visited.add(table_.name)
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
        assert self._db is not None
        result = self._db.execute(self._build_full_query())
        return result if result is not None else []

    def save_as(self, table_name: str, temp: bool = False, column_names: List[str] = []) -> "Table":
        assert self._db is not None
        # When no column_names is not explicitly passed
        # TODO : USE SLICE 1 ROW TO MANIPULATE LESS DATA
        #        OR USING column_names() FUNCTION WITH RESULT ORDERED
        if len(column_names) == 0:
            ret = self.fetch()
            column_names = list(list(ret)[0].keys())
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


def values(rows: Iterable[Tuple], db: db.Database, column_names: Iterable[str] = []) -> Table:
    rows_string = ",".join(["(" + ",".join(str(datum) for datum in row) + ")" for row in rows])
    columns_string = f"({','.join(column_names)})" if any(column_names) else ""
    return Table(f"SELECT * FROM (VALUES {rows_string}) AS vals {columns_string}", db=db)
