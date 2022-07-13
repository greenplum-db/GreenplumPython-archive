"""
This module creates a Python object Table which keep in memory all the user modifications
on a table, in order to proceed SQL query. It concatenates different pieces of queries
together using CTEs.

Table sends the aggregated SQL query to the database and return the final result only when
user calling `fetch()` function.

All modifications made by users are only saved to database when calling `save_as()` function.
"""
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Tuple, Union
from uuid import uuid4

from . import db

if TYPE_CHECKING:
    from .expr import Expr


class Table:
    """
    Representation of Table object.
    """

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
            - a Column of the current Table if key is string

            .. code-block::  python

               id_col = tab["id"]

            - a new Table from the current Table per the type of key:

                - if key is a list, then SELECT a subset of columns, a.k.a. targets;

                .. code-block::  python

                   id_table = tab[["id"]]

                - if key is an Expr, then SELECT a subset of rows per the value of the Expr;

                .. code-block::  python

                   id_cond_table = tab[tab["id"] == 0]

                - if key is a slice, then SELECT a portion of consecutive rows

                .. code-block::  python

                   slice_table = tab[2:5]

        """
        from .expr import Column, Expr

        if isinstance(key, str):
            return Column(key, self)
        if isinstance(key, list):
            return self.select(key)
        if isinstance(key, Expr):
            return self.filter(key)
        if isinstance(key, slice):
            if key.step is not None:
                raise NotImplementedError()
            offset_clause = "" if key.start is None else f"OFFSET {key.start}"
            limit_clause = (
                ""
                if key.stop is None
                else f"LIMIT {key.stop if key.start is None else key.stop - key.start}"
            )
            return Table(
                f"SELECT * FROM {self.name} {limit_clause} {offset_clause}",
                parents=[self],
            )

    def as_name(self, name_as: str) -> "Table":
        """
        Returns the table with a new name.
        """
        return Table(f"SELECT * FROM {self.name}", parents=[self], name=name_as, db=self._db)

    # FIXME: Add test
    def filter(self, expr: "Expr") -> "Table":
        """
        Returns the table filtered by Expression.

        Args:
            expr: Expr : where condition statement

        Returns:
            Table : Table filtered according expr passed in argument
        """
        return Table(f"SELECT * FROM {self._name} WHERE {str(expr)}", parents=[self])

    # FIXME: Add test
    def select(self, target_list: Iterable[Union[str, "Expr"]]) -> "Table":
        """
        Returns table with targeted columns

        Args:
            target_list: Iterable : list of targeted columns

        Returns:
            Table : Table selected only with targeted columns
        """
        return Table(
            f"""
                SELECT {','.join([str(target) for target in target_list])} 
                FROM {self._name}
            """,
            parents=[self],
        )

    @staticmethod
    def _order_by_str(order_by: Union[Iterable[str], Dict[str, str]]) -> str:
        """
        Private method returns ORDER BY statement according to the list of targets

        Args:
            order_by: Iterable : List of columns used for order by

        Returns:
            str : order by statement
        """
        order_by_clause = (
            f"""
                {','.join([' '.join([col, order]) for col, order in order_by.items()])}
            """
            if isinstance(order_by, dict)
            else f"""
                    {','.join([order_index for order_index in order_by])}
                """
        )
        return order_by_clause

    def top(
        self, count: int, order_by: Union[Iterable[str], Dict[str, str]], skip: int = 0
    ) -> "Table":
        """
        Returns top k rows of tables skipping n rows wth order

        Args:
            count: int : number of top consecutive rows will be selected
            order_by: Iterable : list of columns used for order by
            skip: int : number of top consecutive rows to be skipped to proceed select

        Returns:
             Table : table with top k consecutive rows by skipping n rows
        """
        order_by_clause = self._order_by_str(order_by)
        return Table(
            f"""
                SELECT * FROM {self.name}
                ORDER BY {order_by_clause}
                LIMIT {count}
                OFFSET {skip}
            """,
            parents=[self],
        )

    def union(
        self,
        other: "Table",
        is_all: bool = False,
    ):
        return Table(
            f"""
                SELECT *
                FROM {self.name} 
                UNION {"ALL" if is_all else ""}
                SELECT *
                FROM {other.name}
            """,
            parents=[self, other],
        )

    def _join(
        self,
        other: "Table",
        targets: List["Expr"],
        how: str,
        on_str: str,
    ) -> "Table":
        """
        Private function returns table results by joining two tables
        """
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
        targets: List["Expr"] = [],
    ):
        """
        Returns inner join of self and another Table using condition, and only select targeted columns

        Args:
            other: Table : table to use to do the join
            cond: Expr : join on condition
            targets : List : list of targeted columns for joined table

        Returns
            Table : inner joined table

        The result table can select all columns of both tables, or a selection of columns. User can
        also rename column_name to resolve conflicts

        .. code-block::  python

           ret = zoo_1.inner_join(zoo_2,
                                  zoo_1["animal1"] == zoo_2["animal2"],
                                  targets=[
                                    zoo_1["animal1"].rename("zoo1_animal"),
                                    zoo_2["animal2"].rename("zoo2_animal"),
                                  ],
            )

        """
        on_str = " ".join(["ON", str(cond)])
        return self._join(other, targets, "INNER JOIN", on_str)

    def left_join(
        self,
        other: "Table",
        cond: "Expr",
        targets: List["Expr"] = [],
    ):
        """
        Returns left join of self and another Table using condition, and only select targeted columns

        Args:
            other: Table : table to use to do the join
            cond: Expr : join on condition
            targets : List : list of targeted columns for joined table

        Returns
            Table : left joined table

        The result table can select all columns of both tables, or a selection of columns. User can
        also rename column_name to resolve conflicts

        .. code-block::  python

           ret = zoo_1.left_join(zoo_2, zoo_1["animal1"] == zoo_2["animal2"])

        """
        on_str = " ".join(["ON", str(cond)])
        return self._join(other, targets, "LEFT JOIN", on_str)

    def right_join(
        self,
        other: "Table",
        cond: "Expr",
        targets: List["Expr"] = [],
    ):
        """
        Returns right join of self and another Table using condition, and only select targeted columns

        Args:
            other: Table : table to use to do the join
            cond: Expr : join on condition
            targets : List : list of targeted columns for joined table

        Returns
            Table : right joined table

        The result table can select all columns of both tables, or a selection of columns. User can
        also rename column_name to resolve conflicts

        .. code-block::  python

           ret = zoo_1.right_join(zoo_2, zoo_1["animal1"] == zoo_2["animal2"])

        """
        on_str = " ".join(["ON", str(cond)])
        return self._join(other, targets, "RIGHT JOIN", on_str)

    def full_outer_join(
        self,
        other: "Table",
        cond: "Expr",
        targets: List["Expr"] = [],
    ):
        """
        Returns full outer join of self and another Table using condition, and only select targeted columns

        Args:
            other: Table : table to use to do the join
            cond: Expr : join on condition
            targets : List : list of targeted columns for joined table

        Returns
            Table : full outer joined table

        The result table can select all columns of both tables, or a selection of columns. User can
        also rename column_name to resolve conflicts

        .. code-block::  python

           ret = zoo_1.full_join(zoo_2, zoo_1["animal1"] == zoo_2["animal2"])

        """
        on_str = " ".join(["ON", str(cond)])
        return self._join(other, targets, "FULL JOIN", on_str)

    def natural_join(
        self,
        other: "Table",
        targets: List["Expr"] = [],
    ):
        """
        Returns natural join of self and another Table, and only select targeted columns

        Args:
            other: Table : table to use to do the join
            targets : List : list of targeted columns for joined table

        Returns
            Table : natural joined table

        The result table is an implicit join based on the same column names in the joined tables

        .. code-block::  python

           ret = categories.natural_join(products)

        """
        on_str = ""
        return self._join(other, targets, "NATURAL JOIN", on_str)

    def cross_join(
        self,
        other: "Table",
        targets: List["Expr"] = [],
    ):
        """
        Returns cross join of self and another Table, and only select targeted columns

        Args:
            other: Table : table to use to do the join
            targets : List : list of targeted columns for joined table

        Returns
            Table : natural joined table

        The result table can select all columns of both tables, or a selection of columns. User can
        also rename column_name to resolve conflicts

        .. code-block::  python

           ret = zoo_1.cross_join(zoo_2)

        """
        on_str = ""
        return self._join(other, targets, "CROSS JOIN", on_str)

    def column_names(self) -> "Table":
        """
        Returns list of column names of self
        """
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
        """
        Returns name of Table
        """
        return self._name

    @property
    def db(self) -> Optional[db.Database]:
        """
        Returns database associated with Table
        """
        return self._db

    # This is used to filter out tables that are derived from other tables.
    #
    # Actually we cannot determine if a table is recorded in the system catalogs
    # without querying the db.
    def _in_catalog(self) -> bool:
        return self._query.startswith("TABLE")

    def _list_lineage(self) -> List["Table"]:
        lineage: List["Table"] = []
        lineage.append(self)
        tables_visited: set[str] = set()
        current = 0
        while current < len(lineage):
            if lineage[current].name not in tables_visited and not lineage[current]._in_catalog():
                self._depth_first_search(lineage[current], tables_visited, lineage)
            current += 1
        return lineage

    def _depth_first_search(self, t, visited, lineage):
        visited.add(t.name)
        for i in t._parents:
            if i.name not in visited and not i._in_catalog():
                self._depth_first_search(i, visited, lineage)
        lineage.append(t)

    def _build_full_query(self) -> str:
        lineage = self._list_lineage()
        cte_list: List[str] = []
        for table in reversed(lineage):
            if table._name != self._name:
                cte_list.append(f"{table._name} AS ({table._query})")
        if len(cte_list) == 0:
            return self._query
        return "WITH " + ",".join(cte_list) + self._query

    def fetch(self, is_all: bool = True) -> Iterable:
        """
        Fetch rows of this table.
        - if is_all is True, fetch all rows at once
        - otherwise, open a CURSOR and FETCH one row at a time
        """
        if not is_all:
            raise NotImplementedError()
        assert self._db is not None
        result = self._db.execute(self._build_full_query())
        return result if result is not None else []

    def save_as(self, table_name: str, temp: bool = False, column_names: List[str] = []) -> "Table":
        """
        Save the Table to database as a real Greenplum Table

        Args:
            table_name : str
            temp : bool : if table is temporary
            column_names : List : list of column names

        Returns:
            Table : table saved in database
        """
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
        """
        Explaind the table's query
        """
        assert self._db is not None
        results = self._db.execute(f"EXPLAIN (FORMAT {format}) {self._build_full_query()}")
        assert results is not None
        return results


# table_name can be table/view name
def table(name: str, db: db.Database) -> Table:
    """
    Returns a Table object using table name and associated database
    """
    return Table(f"TABLE {name}", name=name, db=db)


def values(rows: Iterable[Tuple], db: db.Database, column_names: Iterable[str] = []) -> Table:
    """
    Returns a Table using list of values given

    .. code-block::  python

       rows = [(1,), (2,), (3,)]
        t = gp.values(rows, db=db)
        t = t.save_as("const_table", column_names=["id"], temp=True)

    """
    rows_string = ",".join(["(" + ",".join(str(datum) for datum in row) + ")" for row in rows])
    columns_string = f"({','.join(column_names)})" if any(column_names) else ""
    return Table(f"SELECT * FROM (VALUES {rows_string}) AS vals {columns_string}", db=db)
