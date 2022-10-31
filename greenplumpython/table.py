"""
This module creates a Python object :class:`Table` which keeps in memory all the user modifications
on a table, in order to proceed with SQL query. It concatenates different pieces of queries
together using CTEs.

Iterating over rows of a Table can be expensive, so is printing it, or converting it to other data structures
like :class:`list`, :class:`tuple`, as well as :class:`pandas.DataFrame`. This is because the content need to
be computed and fetched from a remote database system. That's why :class:`Table` sends the aggregated SQL query
to the database and returns the final result only when `_fetch()` function is called.

Once the content of a :class:`Table` is fetched from the database system, it will be cached locally for later use.
Therefore, re-iterating the same table again will be fast.

Since the content is cached locally, it will become stale once the Table gets modified by someone else on the database system.
Therefore, you might want to use refresh() to sync the latest update.
**Note that refresh() is also expensive.**


N.B: _fetch() function will be called when user wants to iterate through table contents for the first time.

All modifications made by users are only saved to the database when calling the `save_as()`
function.
"""
import collections
import json
from collections import abc
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

if TYPE_CHECKING:
    from greenplumpython.func import FunctionExpr

from uuid import uuid4

from psycopg2.extras import RealDictRow

from greenplumpython import db
from greenplumpython.col import Column, Expr
from greenplumpython.group import TableGroupingSets
from greenplumpython.order import OrderedTable
from greenplumpython.row import Row
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
        self._contents = None
        if any(parents):
            self._db = next(iter(parents))._db
        else:
            self._db = db

    @singledispatchmethod
    def _getitem(self, _) -> "Table":
        raise NotImplementedError()

    @_getitem.register(abc.Callable)  # type: ignore reportMissingTypeArgument
    def _(self, predicate: Callable[["Table"], Expr]):
        return self.where(predicate)

    @_getitem.register(list)
    def _(self, column_names: List[str]) -> "Table":
        return self._select(lambda t: [t[col_name] for col_name in column_names])

    @_getitem.register(str)
    def _(self, column_name: str) -> "Table":
        return Column(column_name, self)

    @_getitem.register(slice)
    def _(self, rows: slice) -> "Table":
        if rows.step is not None:
            raise NotImplementedError()
        offset_clause = "" if rows.start is None else f"OFFSET {rows.start}"
        limit_clause = (
            ""
            if rows.stop is None
            else f"LIMIT {rows.stop if rows.start is None else rows.stop - rows.start}"
        )
        return Table(
            f"SELECT * FROM {self.name} {limit_clause} {offset_clause}",
            parents=[self],
        )

    @overload
    def __getitem__(self, _) -> "Table":
        ...

    @overload
    def __getitem__(self, column_names: List[str]) -> "Table":
        ...

    @overload
    def __getitem__(self, predicate: Callable[["Table"], Expr]) -> "Table":
        ...

    @overload
    def __getitem__(self, column_name: str) -> Expr:
        ...

    @overload
    def __getitem__(self, rows: slice) -> "Table":
        ...

    def __getitem__(self, _):
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

                   id_cond_table = tab[lambda t: t["id"] == 0]

                - if key is a slice, then SELECT a portion of consecutive rows

                .. code-block::  python

                   slice_table = tab[2:5]

        """
        return self._getitem(_)

    def __repr__(self):
        """
        :meta private:

        Return a string representation for a table
        """
        repr_string: str = ""
        if len(list(self)) != 0:
            # Iterate over the given table to calculate the column width for its ASCII representation.
            width = [0] * len(next(iter(self)).column_names())
            for row in self:
                for col_idx, col in enumerate(row):
                    width[col_idx] = max(width[col_idx], len(col), len(str(row[col])))

            # Table header.
            repr_string += (
                "".join(
                    [
                        "| {:{}} |".format(col, width[idx])
                        for idx, col in enumerate(next(iter(self)))
                    ]
                )
                + "\n"
            )
            # Dividing line below table header.
            repr_string += ("=" * (sum(width) + 4 * len(width))) + "\n"
            # Table contents.
            for row in self:
                content = [row[c] for c in row]
                for idx, c in enumerate(content):
                    if isinstance(c, list):
                        repr_string += ("| {:{}} |").format("{}".format(c), width[idx])  # type: ignore
                    else:
                        repr_string += ("| {:{}} |").format(c, width[idx])
                repr_string += "\n"
        return repr_string

    def _repr_html_(self):
        """:meta private:"""
        repr_html_str = ""
        if len(list(self)) != 0:
            repr_html_str = "<table>\n"
            repr_html_str += "\t<tr>\n"
            repr_html_str += ("\t\t<th>{:}</th>\n" * len(list(next(iter(self))))).format(
                *((next(iter(self))))
            )
            repr_html_str += "\t</tr>\n"
            for row in self:
                repr_html_str += "\t<tr>\n"
                content = [row[c] for c in row]
                repr_html_str += ("\t\t<td>{:}</td>\n" * len(list(row))).format(*content)
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
    def where(self, predicate: Callable[["Table"], "Expr"]) -> "Table":
        """
        Returns the :class:`Table` filtered by Expression.

        Args:
            predicate: :class:`~expr.Expr` : where condition statement

        Returns:
            Table : Table filtered according **expr** passed in argument
        """
        return Table(f"SELECT * FROM {self._name} WHERE {str(predicate(self))}", parents=[self])

    def _select(self, targets: Callable[["Table"], Union[Any, Iterable[Any]]]) -> "Table":
        """
        :meta private:

        Returns :class:`Table` with list of targeted :class:`~expr.Column`

        Args:
            target_list: Iterable : list of targeted columns

        Returns:
            Table : Table selected only with targeted columns
        """
        targets_str = [
            str(target) if isinstance(target, Expr) else to_pg_const(target)
            for target in targets(self)
        ]
        return Table(
            f"""
                SELECT {','.join(targets_str)} 
                FROM {self._name}
            """,
            parents=[self],
        )

    def apply(
        self,
        func: Callable[["Table"], "FunctionExpr"],
        expand: bool = False,
        as_name: Optional[str] = None,
    ) -> "Table":
        """
        Apply a function to the :class:`Table`
        Args:
            func: Callable[[:class:`Table`], :class:`~func.FunctionExpr`]: a lambda function of a FunctionExpr
            expand: bool: expand field of composite returning type
            as_name: str: rename returning column
        Returns:
            Table: resulted Table
        Example:
            .. code-block::  python

                rows = [(i,) for i in range(-10, 0)]
                series = gp.values(rows, db=db, column_names=["id"])
                abs = gp.function("abs", db=db)
                result = series.apply(lambda t: abs(t["id"]))

            If we want to give constant as attribute, it is also easy to use. Suppose *label* function takes a str and a int:

            .. code-block::  python

                result = series.apply(lambda t: label("label", t["id"]))
        """
        # We need to support calling functions with constant args or even no
        # arg. For example: SELECT count(*) FROM t; In that case, the
        # arguments do not conain information on any table or any database.
        # As a result, the generated SQL cannot be executed.
        #
        # To fix this, we need to pass the table to the resulting FunctionExpr
        # explicitly.
        return func(self).bind(table=self).apply(expand=expand, as_name=as_name)

    def assign(self, **new_columns: Callable[["Table"], Any]) -> "Table":
        """
        Assigns new columns to the current :class:`Table`. Existing columns
        cannot be reassigned.

        Args:
            new_columns: a :class:`dict` whose keys are column names and values
                are :class:`Callable`s returning column data when applied to the
                current :class:`Table`.

        Returns:
            Table: New table including the new assigned columns

        Example:
            .. code-block::  python

                rows = [(i,) for i in range(-10, 0)]
                series = gp.to_table(rows, db=db, column_names=["id"])
                abs = gp.function("abs")
                results = series.assign(abs=lambda nums: abs(nums["id"]))

        """

        if len(new_columns) == 0:
            return self
        targets: List[str] = []
        if len(new_columns):
            for k, f in new_columns.items():
                v: Any = f(self)
                if isinstance(v, Expr) and not (v.table is None or v.table == self):
                    raise Exception("Newly included columns must be based on the current table")
                targets.append(f"{v.serialize() if isinstance(v, Expr) else to_pg_const(v)} AS {k}")
            return Table(f"SELECT *, {','.join(targets)} FROM {self.name}", parents=[self])

    def order_by(
        self,
        column_name: str,
        ascending: Optional[bool] = None,
        nulls_first: Optional[bool] = None,
        operator: Optional[str] = None,
    ) -> OrderedTable:
        """
        Returns :class:`Table` order by the given arguments.

        Args:
            column_name: name of column to order the table by
            ascending: Optional[Bool]: Define ascending of order, True = ASC / False = DESC
            nulls_first: Optional[bool]: Define if nulls will be ordered first or last, True = First / False = Last
            operator: Optional[str]: Define order by using operator. **Can't combine with ascending.**

        Returns:
            OrderedTable : Table ordered by the given arguments

        Example:
            .. code-block::  Python

                t.order_by("id")
        """
        # State transition diagram:
        # Table --order_by()-> OrderedTable --head()-> Table
        if ascending is not None and operator is not None:
            raise Exception(
                "Could not use 'ascending' and 'operator' at the same time to order by one column"
            )
        return OrderedTable(
            self,
            [column_name],
            [ascending],
            [nulls_first],
            [operator],
        )

    def join(
        self,
        other: "Table",
        how: str = "",
        cond: Optional[Callable[["Table", "Table"], Expr]] = None,
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

            cond: :class:`Callable` lambda function as the join condition
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

        def qualify(t: Table, columns: Union[Dict[str, Optional[str]], Set[str]]) -> List[str]:
            target_list: List[str] = []
            for k in columns:
                col: Column = t[k]
                v = columns[k] if isinstance(columns, dict) else None
                target_list.append(col.serialize() + (f" AS {v}" if v is not None else ""))
            return target_list

        target_list = qualify(self, self_columns) + qualify(other, other_columns)
        on_clause = f"ON {cond(self, other).serialize()}" if cond is not None else ""
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
        """:meta private:"""
        return self._query.startswith("TABLE")

    def _list_lineage(self) -> List["Table"]:
        """:meta private:"""
        lineage: List["Table"] = [self]
        tables_visited: Set[str] = set()
        current = 0
        while current < len(lineage):
            if lineage[current].name not in tables_visited and not lineage[current]._in_catalog():
                self._depth_first_search(lineage[current], tables_visited, lineage)
            current += 1
        return lineage

    def _depth_first_search(self, t: "Table", visited: Set[str], lineage: List["Table"]):
        """:meta private:"""
        visited.add(t.name)
        for i in t._parents:
            if i.name not in visited and not i._in_catalog():
                self._depth_first_search(i, visited, lineage)
        lineage.append(t)

    def _build_full_query(self) -> str:
        """:meta private:"""
        lineage = self._list_lineage()
        cte_list: List[str] = []
        for table in lineage:
            if table._name != self._name:
                cte_list.append(f"{table._name} AS ({table._query})")
        if len(cte_list) == 0:
            return self._query
        return "WITH " + ",".join(cte_list) + self._query

    def __iter__(self) -> "Table":
        """:meta private:"""
        if self._contents is not None:
            self._n = 0
            return self
        assert self._db is not None
        result = self._fetch()
        assert result is not None
        self._contents: List[RealDictRow] = list(result)
        self._n = 0
        return self

    def __next__(self):
        """:meta private:"""

        def detect_duplicate_keys(json_pairs: List[tuple[str, Any]]):
            key_count = collections.Counter(k for k, _ in json_pairs)
            duplicate_keys = ", ".join(k for k, v in key_count.items() if v > 1)

            if len(duplicate_keys) > 0:
                raise Exception("Duplicate column_name(s) found: {}".format(duplicate_keys))

        def validate_data(json_pairs: List[tuple[str, Any]]):
            detect_duplicate_keys(json_pairs)
            return dict(json_pairs)

        if self._n < len(self._contents):
            row_contents: Dict[str, Union[str, List[str]]] = {}
            assert self._contents is not None
            for name in self._contents[0].keys():
                # if name == "to_json":
                to_json_dict = json.loads(
                    self._contents[self._n][name], object_pairs_hook=validate_data
                )
                for sub_name in to_json_dict:
                    row_contents[sub_name] = to_json_dict[sub_name]
                # else:
                #     # According our current _fetch(), name=="to_json" will be always true right?
                #     row_contents[name] = self._contents[self._n][name]
            self._n += 1
            return Row(row_contents)
        raise StopIteration("StopIteration: Reached last row of table!")

    def refresh(self) -> "Table":
        """
        Refresh self._contents

        Returns:
            self
        """

        assert self._db is not None
        result = self._fetch()
        assert result is not None
        self._contents = list(result)
        self._n = 0
        return self

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
        to_json_table = Table(
            f"SELECT to_json({self.name})::TEXT FROM {self.name}",
            parents=[self],
        )
        result = self._db.execute(to_json_table._build_full_query())
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
            column_names = next(iter(self)).column_names()  # type: ignore
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

    def group_by(self, *column_names: str) -> TableGroupingSets:
        """
        Group the current :class:`~table.Table` by columns specified by
        `column_names`.

        Args:
            column_names: one or more column names of the table

        Returns:
            TableGroupingSets: a list of grouping sets. Each group is identified
            by a different set of values of the columns in the arguments.
        """
        #  State transition diagram:
        #  Table --group_by()-> TableRowGroup --aggregate()-> FunctionExpr
        #    ^                                                    |
        #    |------------------------- to_table() ---------------|
        return TableGroupingSets(self, [column_names])

    def distinct_on(self, *column_names: str) -> "Table":
        """
        Deduplicate the current :class:`Table` with respect to the given columns.

        Args:
            column_names: name of column of the current :class:`Table`.

        Returns:
            :class:`Table`: Table containing only the distinct values of the
                            given columns.
        """
        cols = [Column(name, self).serialize() for name in column_names]
        return Table(f"SELECT DISTINCT ON ({','.join(cols)}) * FROM {self.name}", parents=[self])


# table_name can be table/view name
def table(name: str, db: db.Database) -> Table:
    """
    Returns a :class:`Table` using table name and associated :class:`~db.Database`

    Args:
        name: str: Table name
        db: :class:`~db.Database`: database which contains the table
    """
    return Table(f"TABLE {name}", name=name, db=db)


def to_table(
    rows: Iterable[Tuple[Any]], db: db.Database, column_names: Iterable[str] = []
) -> Table:
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
        t = gp.to_table(rows, db=db)

    """
    rows_string = ",".join(
        ["(" + ",".join(to_pg_const(datum) for datum in row) + ")" for row in rows]
    )
    columns_string = f"({','.join(column_names)})" if any(column_names) else ""
    return Table(f"SELECT * FROM (VALUES {rows_string}) AS vals {columns_string}", db=db)
