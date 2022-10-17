"""
This module creates a Python object :class:`Table` which keeps in memory all the user modifications
on a table, in order to proceed with SQL query. It concatenates different pieces of queries
together using CTEs.

Table sends the aggregated SQL query to the database and returns the final result only when
user calling `fetch()` function.

All modifications made by users are only saved to the database when calling the `save_as()`
function.
"""
from functools import partialmethod, singledispatchmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
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
        columns: Optional[Iterable[Column]] = None,
    ) -> None:
        self._query = query
        self._parents = parents
        self._name = "cte_" + uuid4().hex if name is None else name
        self._columns = columns
        self._ndim = None
        self._contents = None
        if any(parents):
            self._db = next(iter(parents))._db
        else:
            self._db = db

    @singledispatchmethod
    def _getitem(self, key):  # type: ignore
        raise NotImplementedError()

    @_getitem.register(Expr)
    def _(self, key: Expr):
        return self.where(key)

    @_getitem.register(list)
    def _(self, key: List[str]) -> "Table":
        return self.select(*(self[col_name] for col_name in key))

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
    def __getitem__(self, key: List[str]) -> "Table":
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
        repr_string = ""
        ret = list(self._fetch())  # type: ignore
        if len(ret) != 0:
            # Iterate over the given table to calculate the column width for its ASCII representation.
            width = [0] * len(ret[0])
            for row in ret:
                for col_idx, col in enumerate(row):
                    width[col_idx] = max(width[col_idx], len(col), len(str(row[col])))

            # Table header.
            repr_string += (
                "".join(["| {:{}} |".format(col, width[idx]) for idx, col in enumerate(ret[0])])
                + "\n"
            )
            # Dividing line below table header.
            repr_string += ("=" * (sum(width) + 4 * len(width))) + "\n"
            # Table contents.
            for row in ret:
                content = [row[c] for c in row]
                for idx, c in enumerate(content):
                    if isinstance(c, list):
                        repr_string += ("| {:{}} |").format("{}".format(c), width[idx])  # type: ignore
                    else:
                        repr_string += ("| {:{}} |").format(c, width[idx])
                repr_string += "\n"
        return repr_string

    def _repr_html_(self):
        ret = list(self._fetch())
        repr_html_str = ""
        if len(ret) != 0:
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

    def rename(self, name: str) -> "Table":
        """
        Returns a copy of the :class:`Table` with a new name.

        Args:
            name_as: str : Table's new name

        Returns:
            Table: a new Object Table named **name_as** with same content
        """
        return Table(f"SELECT * FROM {self.name}", parents=[self], name=name, db=self._db)

    # FIXME: Add test
    def where(self, expr: "Expr") -> "Table":
        """
        Returns the :class:`Table` filtered by Expression.

        Args:
            expr: :class:`~expr.Expr` : where condition statement

        Returns:
            Table : Table filtered according **expr** passed in argument
        """
        return Table(f"SELECT * FROM {self._name} WHERE {str(expr)}", parents=[self])

    # FIXME: Add test
    def select(self, *targets: Iterable[Any]) -> "Table":
        """
        Returns :class:`Table` with list of targeted :class:`~expr.Column`

        Args:
            target_list: Iterable : list of targeted columns

        Returns:
            Table : Table selected only with targeted columns
        """
        targets_str = [
            str(target) if isinstance(target, Expr) else to_pg_const(target) for target in targets
        ]
        return Table(
            f"""
                SELECT {','.join(targets_str)} 
                FROM {self._name}
            """,
            parents=[self],
        )

    def extend(self, name: str, value: Any) -> "Table":
        """
        Extends the current :class:`Table` by including an extra value as a
        new :class:`Column`.

        Args:
            name: Name of the new column in the resulting :class:`Table`.
            value: Value of the new column, can be either an :class:`Expr`,
                or any other type that can be adapted to a SQL type.

        Returns:
            Table: Table after extension

        Warning:
            Currently, value of type :class:`Expr` whose result has more than
            one column is **not supported** even though no exception will
            be thrown in that case. Please **don't rely on this behavior** as
            this will be fixed soon.

            Examples of this case include functions returning composite type
            objects, etc.

            This is because GreenplumPython have no knowledge on what columns
            are in a :class:`Table` or the result of an :class:`Expr`. This
            issue will be fixed as soon as we implement column inference for
            GreenplumPython.
        """
        if isinstance(value, Expr) and not (value.table is None or value.table == self):
            raise Exception("Current table and included expression must be based on the same table")
        target = value.serialize() if isinstance(value, Expr) else to_pg_const(value)
        return Table(f"SELECT *, {target} AS {name} FROM {self.name}", parents=[self])

    def order_by(
        self,
        order_col: Expr,
        ascending: Optional[bool] = None,
        nulls_first: Optional[bool] = None,
        operator: Optional[str] = None,
    ) -> OrderedTable:
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

    def join(
        self,
        other: "Table",
        how: str = "",
        cond: Optional[Expr] = None,
        using: Optional[Iterable[str]] = None,
        self_columns: Union[Dict[str, Optional[str]], Set[str]] = {},
        other_columns: Union[Dict[str, Optional[str]], Set[str]] = {},
    ) -> "Table":
        """
        Joins the current :class:`Table` with another :class:`Table`.

        Args:
            other: :class:`Table` to join with
            how: How the two tables are joined. The value can be one of
                - `"INNER"`: inner join,
                - `"LEFT"`: left outer join,
                - `"LEFT"`: right outer join,
                - `"FULL"`: full outer join, or
                - `"CROSS"`: cross join, i.e. the Cartesian product
                The default value `""` is equivalent to "INNER".

            cond: :class:`Expr` as the join condition
            using: a list of column names that exist in both tables to join on.
                `cond` and `using` cannot be used together.
            self_columns: A :class:`dict` whose keys are the column names of
                the current table to be included in the resulting
                table. The value, if not `None`, is used for renaming
                the corresponding key to avoid name conflicts. Asterisk `"*"`
                can be used as a key to indicate all columns.
            other_columns: Same as `self_columns`, but for the `other`
                table.

        Note:
            When using `"*"` as key in `self_columns` or `other_columns`,
            please ensure that there will not be more than one column with the
            same name by applying proper renaming. Otherwise there will be an
            error.
        """
        # FIXME : Raise Error if target columns don't exist
        assert how.upper() in [
            "",
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "CROSS",
        ], "Unsupported join type"
        assert cond is None or using is None, 'Cannot specify "cond" and "using" together'

        def to_target_list(
            t: Table, columns: Union[Dict[str, Optional[str]], Set[str]]
        ) -> List[str]:
            target_list: List[str] = []
            for k in columns:
                col: Column = t[k]
                v = columns[k] if isinstance(columns, dict) else None
                target_list.append(col.serialize() + (f" AS {v}" if v is not None else ""))
            return target_list

        target_list = to_target_list(self, self_columns) + to_target_list(other, other_columns)
        on_clause = f"ON {cond.serialize()}" if cond is not None else ""
        using_clause = f"USING ({','.join(using)})" if using is not None else ""
        return Table(
            f"""
                SELECT {",".join(target_list)}
                FROM {self.name} {how} JOIN {other.name} {on_clause} {using_clause}
            """,
            parents=[self, other],
        )

    inner_join = partialmethod(join, how="INNER")
    """
    Inner joins the current :class:`Table` with another :class:`Table`.

    Equivalent to calling :meth:`Table.join` with `how="INNER"`.
    """

    left_join = partialmethod(join, how="LEFT")
    """
    Left-outer joins the current :class:`Table` with another :class:`Table`.

    Equivalent to calling :meth:`Table.join` with `how="LEFT"`.
    """

    right_join = partialmethod(join, how="RIGHT")
    """
    Right-outer joins the current :class:`Table` with another :class:`Table`.

    Equivalent to calling :meth:`Table.join` with `how="RIGHT"`.
    """

    full_join = partialmethod(join, how="FULL")
    """
    Full-outer joins the current :class:`Table` with another :class:`Table`.

    Equivalent to calling :meth:`Table.join` with argutment `how="FULL"`.
    """

    cross_join = partialmethod(join, how="CROSS", cond=None, using=None)
    """
    Cross joins the current :class:`Table` with another :class:`Table`,
    i.e. the Cartesian product.

    Equivalent to calling :meth:`Table.join` with `how="CROSS"`.
    """

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
    def ndim(self) -> int:
        """
        Returns number of rows of Table

        Returns:
            int: number of rows of table

        """
        if self._ndim is None:
            t: Table = iter(self)
            assert t._ndim is not None
            return t._ndim
        assert self._ndim is not None
        return self._ndim

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
    def columns(self) -> Optional[Iterable[Column]]:
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

    def __iter__(self):
        if self._contents is not None and self._n < len(self._contents):
            return self
        assert self._db is not None
        result = self._fetch()
        assert result is not None
        self._contents = list(result)
        self._n = 0
        self._ndim = len(self._contents)
        return self

    def __next__(self):
        if self._n < len(self._contents):
            row_contents: Dict[str, str] = {}  # type ignore
            assert self._contents is not None
            for name in list(self._contents[0]):  # type ignore
                row_contents[name] = self._contents[self._n][name]  # type ignore
            self._n += 1
            return row_contents
        raise StopIteration("StopIteration: Reached last row of table!")

    def _fetch(self, is_all: bool = True) -> Iterable[Tuple[Any]]:
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
            ret = self._fetch()
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
                result = series.apply(lambda t: abs(t["id"])).to_table()_fetch()

            If we want to give constant as attribute, it is also easy to use. Suppose *label* function
            takes a str and a int:

            .. code-block::  python

                result = series.apply(lambda t: label("label", t["id"])).to_table()._fetch()

        """
        # We need to support calling functions with constant args or even no
        # arg. For example: SELECT count(*) FROM t; In that case, the
        # arguments do not conain information on any table or any database.
        # As a result, the generated SQL cannot be executed.
        #
        # To fix this, we need to pass the table to the resulting FunctionExpr
        # explicitly.
        return func(self)(table=self)


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
