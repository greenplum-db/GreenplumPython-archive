from typing import TYPE_CHECKING, Iterable, List, Optional, Tuple, Union
from uuid import uuid4

from . import db

if TYPE_CHECKING:
    from .expr import Column, Expr


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
        from .expr import Column, Expr

        if isinstance(key, str):
            return Column(key, self)
        if isinstance(key, list):
            return self.select(key)
        if isinstance(key, Expr):
            return self.filter(key)
        if isinstance(key, slice):
            raise NotImplementedError()

    def as_name(self, name_as: str) -> "Table":
        return Table(f"SELECT * FROM {self.name}", parents=[self], name=name_as, db=self._db)

    # FIXME: Add test
    def filter(self, expr: "Expr") -> "Table":
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
        targets: List,
        how: str,
        on_str: str,
    ) -> "Table":
        # FIXME : Raise Error if target columns don't exist
        # FIXME : Same column name in both table
        select_str = ",".join([str(target) for target in targets]) if targets != [] else "*"
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
        cond: "Expr",
        targets: List = [],
    ):
        on_str = " ".join(["ON", str(cond)])
        return self._join(other, targets, "INNER JOIN", on_str)

    def left_join(
        self,
        other: "Table",
        cond: "Expr",
        targets: List = [],
    ):
        on_str = " ".join(["ON", str(cond)])
        return self._join(other, targets, "LEFT JOIN", on_str)

    def right_join(
        self,
        other: "Table",
        cond: "Expr",
        targets: List = [],
    ):
        on_str = " ".join(["ON", str(cond)])
        return self._join(other, targets, "RIGHT JOIN", on_str)

    def full_outer_join(
        self,
        other: "Table",
        cond: "Expr",
        targets: List = [],
    ):
        on_str = " ".join(["ON", str(cond)])
        return self._join(other, targets, "FULL JOIN", on_str)

    def natural_join(
        self,
        other: "Table",
        targets: List = [],
    ):
        on_str = ""
        return self._join(other, targets, "NATURAL JOIN", on_str)

    def cross_join(
        self,
        other: "Table",
        targets: List = [],
    ):
        on_str = ""
        return self._join(other, targets, "CROSS JOIN", on_str)

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

    # This is used to filter out tables that are derived from other tables.
    #
    # Actually we cannot determine if a table is recorded in the system catalogs
    # without querying the db.
    def _in_catalog(self) -> bool:
        return self._query.startswith("TABLE")

    def _list_lineage(self) -> List["Table"]:
        lineage = []
        lineage.append(self)
        tables_visited = set()
        current = 0
        while current < len(lineage):
            for table_ in lineage[current]._parents:
                if table_.name not in tables_visited and not table_._in_catalog():
                    lineage.append(table_)
                    tables_visited.add(table_.name)
            current += 1
        return lineage

    def _build_full_query(self) -> str:
        lineage = self._list_lineage()
        cte_list = []
        for table in reversed(lineage):
            if table._name != self._name:
                cte_list.append(f"{table._name} AS ({table._query})")
        if len(cte_list) == 0:
            return self._query
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

    # TODO: Uncomment or remove this.
    #
    # def create_index(
    #     self,
    #     columns: Iterable[Union["Column", str]],
    #     method: str = "btree",
    #     name: Optional[str] = None,
    # ) -> None:
    #     if not self._in_catalog():
    #         raise Exception("Cannot create index on tables not in the system catalog.")
    #     index_name: str = name if name is not None else "idx_" + uuid4().hex
    #     indexed_cols = ",".join([str(col) for col in columns])
    #     assert self._db is not None
    #     self._db.execute(
    #         f"CREATE INDEX {index_name} ON {self.name} USING {method} ({indexed_cols})",
    #         has_results=False,
    #     )

    # FIXME: Should we choose JSON as the default format?
    def explain(self, format: str = "TEXT") -> Iterable:
        assert self._db is not None
        results = self._db.execute(f"EXPLAIN (FORMAT {format}) {self._build_full_query()}")
        assert results is not None
        return results


# table_name can be table/view name
def table(name: str, db: db.Database) -> Table:
    return Table(f"TABLE {name}", name=name, db=db)


def values(rows: Iterable[Tuple], db: db.Database, column_names: Iterable[str] = []) -> Table:
    rows_string = ",".join(["(" + ",".join(str(datum) for datum in row) + ")" for row in rows])
    columns_string = f"({','.join(column_names)})" if any(column_names) else ""
    return Table(f"SELECT * FROM (VALUES {rows_string}) AS vals {columns_string}", db=db)
