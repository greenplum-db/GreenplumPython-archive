"""
This module creates a Python object :class:`Table` which keeps in memory all the user modifications
on a table, in order to proceed with SQL query. It concatenates different pieces of queries
together using CTEs.

Table sends the aggregated SQL query to the database and returns the final result only when
user calling `fetch()` function.

All modifications made by users are only saved to the database when calling the `save_as()`
function.
"""
from functools import singledispatchmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    overload,
)
from uuid import uuid4

from greenplumpython import db
from greenplumpython.group import TableRowGroup

if TYPE_CHECKING:
    from greenplumpython.func import FunctionExpr

from greenplumpython.expr import Column, Expr
from greenplumpython.order import OrderedTable
from greenplumpython.type import to_pg_const


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
        columns: Optional[Iterable[str]] = None,
    ) -> None:
        self._query = query
        self._parents = parents
        self._name = "cte_" + uuid4().hex if name is None else name
        self._columns = columns
        if any(parents):
            self._db = next(iter(parents))._db
        else:
            self._db = db

    @singledispatchmethod
    def _getitem(self, key):  # type: ignore
        raise NotImplementedError()

    @_getitem.register(Expr)
    def _(self, key: Expr):
        return self.filter(key)

    @_getitem.register(list)
    def _(self, key: List[Union[str, Expr]]) -> "Table":
        return self.select(key)

    @_getitem.register
    def _(self, key: str):
        return Column(key, self)

    @_getitem.register
    def _(self, key: slice):
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

    @overload
    def __getitem__(self, key) -> "Table":  # type: ignore
        ...

    @overload
    def __getitem__(self, key: List[Union[str, Expr]]) -> "Table":
        ...

    @overload
    def __getitem__(self, key: Expr) -> "Table":
        ...

    @overload
    def __getitem__(self, key: str) -> Expr:
        ...

    @overload
    def __getitem__(self, key: slice) -> Optional["Table"]:
        ...

    def __getitem__(self, *args, **kwargs):  # type: ignore
        """
        Returns
            - a :class:`~expr.Column` of the current Table if key is string

            .. code-block::  python

               id_col = tab["id"]

            - a new :class:`Table` from the current Table per the type of key:

                - if key is a list, then SELECT a subset of columns, a.k.a. targets;

                .. code-block::  python

                   id_table = tab[["id"]]

                - if key is an :class:`~expr.Expr`, then SELECT a subset of rows per the value of the Expr;

                .. code-block::  python

                   id_cond_table = tab[tab["id"] == 0]

                - if key is a slice, then SELECT a portion of consecutive rows

                .. code-block::  python

                   slice_table = tab[2:5]

        """
        return self._getitem(*args, **kwargs)  # type: ignore

    def __repr__(self):
        """
        Return a string representation for a table
        """
        # FIXME : adjust columns width depending on the number of characters
        repr_string = ""
        ret = list(self.fetch())
        repr_string += (("| {:10} |" * len(ret[0])).format(*ret[0])) + "\n"
        repr_string += ("=" * 14 * len(ret[0])) + "\n"
        for row in ret:
            content = [row[c] for c in row]
            s = ("| {:10} |" * len(row)).format(*content)
            repr_string += s + "\n"
        return repr_string

    def _repr_html_(self):
        ret = list(self.fetch())
        repr_html_str = "<table>\n"
        repr_html_str += "\t<tr>\n"
        repr_html_str += ("\t\t<th>{:}</th>\n" * len(ret[0])).format(*ret[0])
        repr_html_str += "\t</tr>\n"
        for row in ret:
            repr_html_str += "\t<tr>\n"
            content = [row[c] for c in row]
            repr_html_str += ("\t\t<td>{:}</td>\n" * len(row)).format(*content)
            repr_html_str += "\t</tr>\n"
        repr_html_str += "</table>"
        return repr_html_str

    def as_name(self, name_as: str) -> "Table":
        """
        Returns a copy of the :class:`Table` with a new name.

        Args:
            name_as: str : Table's new name

        Returns:
            Table: a new Object Table named **name_as** with same content
        """
        return Table(f"SELECT * FROM {self.name}", parents=[self], name=name_as, db=self._db)

    # FIXME: Add test
    def filter(self, expr: "Expr") -> "Table":
        """
        Returns the :class:`Table` filtered by Expression.

        Args:
            expr: :class:`~expr.Expr` : where condition statement

        Returns:
            Table : Table filtered according **expr** passed in argument
        """
        return Table(f"SELECT * FROM {self._name} WHERE {str(expr)}", parents=[self])

    # FIXME: Add test
    def select(self, target_list: Iterable[Union[str, "Expr"]]) -> "Table":
        """
        Returns :class:`Table` with list of targeted :class:`~expr.Column`

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
            columns=target_list,
        )

    def order_by(
        self,
        order_col: Expr,
        ascending: Optional[bool] = None,
        nulls_first: Optional[bool] = None,
        operator: Optional[str] = None,
    ):
        """
        Returns :class:`Table` order by the given arguments.

        Args:
            order_col: :class:`~expr.Expr` : Column which used to order by the table
            ascending: Optional[Bool]: Define ascending of order, True = ASC / False = DESC
            nulls_first: Optional[bool]: Define if nulls will be ordered first or last, True = First / False = Last
            operator: Optional[str]: Define order by using operator. **Can't combine with ascending.**

        Returns:
            OrderedTable : Table ordered by the given arguments

        Example:
            .. code-block::  Python

                t.order_by(t["id"])
        """
        # State transition diagram:
        # Table --order_by()-> OrderedTable --head()-> Table
        if ascending is not None and operator is not None:
            raise Exception(
                "Could not use 'ascending' and 'operator' at the same time to order by one column"
            )
        return OrderedTable(
            self,
            [order_col],
            [ascending],
            [nulls_first],
            [operator],
        )

    def union(
        self,
        other: "Table",
        is_all: bool = False,
    ):
        """
        Returns self union other table.

        Args:
            other: :class:`Table`: table to use to do the union
            is_all: bool: Define if it is a UNION ALL

        Returns:
            Table: self union other
        """
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
        targets: List["Column"],
        how: str,
        on_str: str,
    ) -> "Table":
        """
        Private function returns table results by joining two :class:`Table`
        """
        # FIXME : Raise Error if target columns don't exist
        # FIXME : Same column name in both table
        target_str_list = [str(target) for target in targets]
        select_str = ",".join(target_str_list) if targets != [] else "*"
        return Table(
            f"""
                SELECT {select_str} 
                FROM {self.name} {how} {other.name}
                {str(on_str)}  
            """,
            parents=[self, other],
            columns=[
                target.as_name if target.as_name is not None else target.name for target in targets
            ]
            if targets != []
            and str(self["*"]) not in target_str_list
            and str(other["*"]) not in target_str_list
            # FIXME : Add analyze for other cases
            # FIXME : For example when select * and both table has attribute "columns"
            else None,
        )

    def inner_join(
        self,
        other: "Table",
        cond: "Expr",
        targets: List["Column"] = [],
    ):
        """
        Returns inner join of self and another :class:`Table` using condition, and only select targeted
        columns

        Args:
            other: :class:`Table` : table to use to do the join
            cond: :class:`~expr.Expr` : join on condition
            targets : List : list of targeted columns for joined table

        Returns:
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
        targets: List["Column"] = [],
    ):
        """
        Returns left join of self and another :class:`Table` using condition, and only select targeted
        columns

        Args:
            other: :class:`Table` : table to use to do the join
            cond: :class:`~expr.Expr` : join on condition
            targets : List : list of targeted columns for joined table

        Returns:
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
        targets: List["Column"] = [],
    ):
        """
        Returns right join of self and another :class:`Table` using condition, and only select targeted
        columns

        Args:
            other: :class:`Table` : table to use to do the join
            cond: :class:`~expr.Expr` : join on condition
            targets : List : list of targeted columns for joined table

        Returns:
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
        targets: List["Column"] = [],
    ):
        """
        Returns full outer join of self and another :class:`Table` using condition, and only select targeted
        columns

        Args:
            other: :class:`Table` : table to use to do the join
            cond: :class:`~expr.Expr` : join on condition
            targets : List : list of targeted columns for joined table

        Returns:
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
        targets: List["Column"] = [],
    ):
        """
        Returns natural join of self and another :class:`Table`, and only select targeted columns

        Args:
            other: :class:`Table` : table to use to do the join
            targets : List : list of targeted columns for joined table

        Returns:
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
        targets: List["Column"] = [],
    ):
        """
        Returns cross join of self and another :class:`Table`, and only select targeted columns

        Args:
            other: :class:`Table` : table to use to do the join
            targets : List : list of targeted columns for joined table

        Returns:
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
        Returns :class:`Table` contained column names of self. Need to do a fetch afterwards to get results.

        Returns:
            Table: table contained list of columns name of self
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
        Returns name of :class:`Table`

        Returns:
            str: Table name
        """
        return self._name

    @property
    def db(self) -> Optional[db.Database]:
        """
        Returns :class:`~db.Database` associated with :class:`Table`

        Returns:
            Optional[Database]: database associated with table
        """
        return self._db

    @property
    def columns(self) -> Optional[Iterable[str]]:
        """
        Returns its :class:`~expr.Column` name of :class:`Table`, has results only for selected table and joined table with targets.

        Returns:
            Optional[Iterable[str]]: None or List of its columns names of table
        """
        return self._columns

    # This is used to filter out tables that are derived from other tables.
    #
    # Actually we cannot determine if a table is recorded in the system catalogs
    # without querying the db.
    def _in_catalog(self) -> bool:
        return self._query.startswith("TABLE")

    def _list_lineage(self) -> List["Table"]:
        lineage: List["Table"] = [self]
        tables_visited: Set[str] = set()
        current = 0
        while current < len(lineage):
            if lineage[current].name not in tables_visited and not lineage[current]._in_catalog():
                self._depth_first_search(lineage[current], tables_visited, lineage)
            current += 1
        return lineage

    def _depth_first_search(self, t: "Table", visited: Set[str], lineage: List["Table"]):
        visited.add(t.name)
        for i in t._parents:
            if i.name not in visited and not i._in_catalog():
                self._depth_first_search(i, visited, lineage)
        lineage.append(t)

    def _build_full_query(self) -> str:
        lineage = self._list_lineage()
        cte_list: List[str] = []
        for table in lineage:
            if table._name != self._name:
                cte_list.append(f"{table._name} AS ({table._query})")
        if len(cte_list) == 0:
            return self._query
        return "WITH " + ",".join(cte_list) + self._query

    def fetch(self, is_all: bool = True) -> Iterable[Tuple[Any]]:
        """
        Fetch rows of this :class:`Table`.
        - if is_all is True, fetch all rows at once
        - otherwise, open a CURSOR and FETCH one row at a time

        Args:
            is_all: bool: Define if fetch all rows at once

        Returns:
            Iterable[Tuple[Any]]: results of query received from database
        """
        if not is_all:
            raise NotImplementedError()
        assert self._db is not None
        result = self._db.execute(self._build_full_query())
        return result if result is not None else []

    def save_as(self, table_name: str, temp: bool = False, column_names: List[str] = []) -> "Table":
        """
        Save the table to database as a real Greenplum Table

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
            column_names = list(list(ret)[0].keys())  # type: ignore
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
    def explain(self, format: str = "TEXT") -> Iterable[Tuple[str]]:
        """
        Explained the table's query

        Args:
            format: str: format of explain

        Returns:
            Iterable[Tuple[str]]: EXPLAIN query answer
        """
        assert self._db is not None
        results = self._db.execute(f"EXPLAIN (FORMAT {format}) {self._build_full_query()}")
        assert results is not None
        return results

    def group_by(self, *group_by: "Expr") -> TableRowGroup:
        """
        Returns self group by the given list.

        Args:
            *group_by: :class:`~expr.Expr` : Set of columns which used to group by the table

        Returns:
            TableRowGroup : :class:`Table` grouped by the given list of :class:`~expr.Column`
        """
        #  State transition diagram:
        #  Table --group_by()-> TableRowGroup --aggregate()-> FunctionExpr
        #    ^                                                    |
        #    |------------------------- to_table() ---------------|
        return TableRowGroup(self, [list(group_by)])

    # FIXME : Add more tests
    def apply(self, func: Callable[["Table"], "FunctionExpr"]) -> "FunctionExpr":
        """
        Apply a function to the :class:`Table`

        Args:
            func: Callable[[:class:`Table`], :class:`~func.FunctionExpr`]: a lambda function of a FunctionExpr

        Returns:
            FunctionExpr: a callable

        Example:
            .. code-block::  python

                rows = [(i,) for i in range(-10, 0)]
                series = gp.values(rows, db=db, column_names=["id"])
                abs = gp.function("abs", db=db)
                result = series.apply(lambda t: abs(t["id"])).to_table().fetch()

            If we want to give constant as attribute, it is also easy to use. Suppose *label* function
            takes a str and a int:

            .. code-block::  python

                result = series.apply(lambda t: label("label", t["id"])).to_table().fetch()

        """
        return func(self)


# table_name can be table/view name
def table(name: str, db: db.Database) -> Table:
    """
    Returns a :class:`Table` using table name and associated :class:`~db.Database`

    Args:
        name: str: Table name
        db: :class:`~db.Database`: database which contains the table
    """
    return Table(f"TABLE {name}", name=name, db=db)


def values(rows: Iterable[Tuple[Any]], db: db.Database, column_names: Iterable[str] = []) -> Table:
    """
    Returns a :class:`Table` using list of values given

    Args:
        rows: Iterable[Tuple[Any]]: List of values
        db: :class:`~db.Database`: database which will be associated with table
        column_names: Iterable[str]: List of given column names

    Returns:
        Table: table generated with given values

    .. code-block::  python

       rows = [(1,), (2,), (3,)]
        t = gp.values(rows, db=db)

    """
    rows_string = ",".join(
        ["(" + ",".join(to_pg_const(datum) for datum in row) + ")" for row in rows]
    )
    columns_string = f"({','.join(column_names)})" if any(column_names) else ""
    return Table(f"SELECT * FROM (VALUES {rows_string}) AS vals {columns_string}", db=db)
