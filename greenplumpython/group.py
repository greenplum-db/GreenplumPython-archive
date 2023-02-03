"""Definitions for the result of grouping :class:`~dataframe.DataFrame`."""

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    List,
    MutableSet,
    Optional,
    Set,
)

from greenplumpython.expr import Expr, serialize

if TYPE_CHECKING:
    from greenplumpython.dataframe import DataFrame
    from greenplumpython.func import FunctionExpr


class DataFrameGroupingSets:
    """Represent a group of rows in a :class:`~dataframe.DataFrame`."""

    def __init__(self, dataframe: "DataFrame", grouping_sets: List[Iterable["Expr"]]) -> None:
        """Construct an instance of this class."""
        self._dataframe = dataframe
        self._grouping_sets = grouping_sets

    def apply(
        self,
        func: Callable[["DataFrame"], "FunctionExpr"],
        expand: bool = False,
        as_name: Optional[str] = None,
    ) -> "DataFrame":
        """
        Apply a function to the grouping set.

        Args:
            func: an aggregate function to apply.
            expand: expand fields if `func` returns composite type.
            as_name: rename returning column.

        Returns:
            DataFrame: resulted :class:`~dataframe:DataFrame`.

        Example:
            .. highlight::  python
            .. code-block::  python

                >>> rows = [(i, i % 2 == 0) for i in range(10)]
                >>> numbers = db.create_dataframe(rows=rows, column_names=["val", "is_even"])
                >>> count = gp.aggregate_function("count")

                >>> results = numbers.group_by("is_even").apply(lambda _: count())
                >>> results
                -----------------
                 count | is_even
                -------+---------
                     5 |       1
                     5 |       0
                -----------------
                (2 rows)

                >>> results = numbers.group_by("is_even").apply(lambda _: count(), as_name='cnt')
                >>> results
                ---------------
                 cnt | is_even
                -----+---------
                   5 |       1
                   5 |       0
                ---------------
                (2 rows)

                >>> class array_sum:
                ...     sum: int
                ...     count: int
                ...
                >>> @gp.create_array_function
                ... def my_count_sum(val_list: List[int]) -> array_sum:
                ...     return {"sum": sum(val_list), "count": len(val_list)}
                ...
                >>> results = numbers.group_by("is_even").apply(lambda t: my_count_sum(t["val"]), expand=True)
                >>> results
                -----------------------
                 is_even | sum | count
                ---------+-----+-------
                       0 |  25 |     5
                       1 |  20 |     5
                -----------------------
                (2 rows)
        """
        return func(self._dataframe).bind(group_by=self).apply(expand=expand, as_name=as_name)

    def assign(self, **new_columns: Callable[["DataFrame"], Any]) -> "DataFrame":
        """
        Assign new columns to the current grouping sets.

        **NOTE:** Existing columns cannot be reassigned.

        Args:
            new_columns: a :class:`dict` whose keys are column names and values are
                         :class:`Callable`. The :class:`Callable` will be applied to the current
                         :class:`DataFrameGroupingSets` and return the column data.

        Returns:
            :class:`~dataframe.DataFrame` with the new columns.


        Example:
            .. highlight:: python
            .. code-block::  python

                >>> rows = [(1,) for _ in range(10)]
                >>> numbers = db.create_dataframe(rows=rows, column_names=["val"])
                >>> count = gp.aggregate_function("count")
                >>> sum = gp.aggregate_function("sum")
                >>> results = numbers.group_by("is_even").assign(
                        count=lambda t: count(t["val"]),
                        sum=lambda t: sum(t["val"]))
                >>> results
                -----------------------
                 is_even | count | sum
                ---------+-------+-----
                       1 |     5 |  20
                       0 |     5 |  25
                -----------------------
        """
        from greenplumpython.dataframe import DataFrame

        targets: List[str] = list(self.flatten())
        for k, f in new_columns.items():
            v: Any = f(self.dataframe).bind(group_by=self)
            if isinstance(v, Expr) and not (v.dataframe is None or v.dataframe == self.dataframe):
                raise Exception("Newly included columns must be based on the current dataframe")
            targets.append(f"{serialize(v)} AS {k}")
        return DataFrame(
            f"SELECT {','.join(targets)} FROM {self.dataframe.name} {self.clause()}",
            parents=[self.dataframe],
        )

    def union(
        self, other: Callable[["DataFrame"], "DataFrameGroupingSets"]
    ) -> "DataFrameGroupingSets":
        """
        Union with another :class:`DataFrameGroupingSets`.

        So that when applying an aggregate function to the list, the function will be applied to
        each grouping set individually.

        Args:
            other: a :class:`Callable` returning the result of
                   :meth:`~dataframe.DataFrame.group_by()` when applied to the current
                   :class:`~dataframe.DataFrame`.

        Returns:
            a new instance of :class:`DataFrameGroupingSets`.

        Example:
            .. highlight:: python
            .. code-block::  python

                >>> rows = [(i, i % 2 == 0, i % 3 == 0) for i in range(6)]  # 0, 1, 2, 3, 4, 5
                >>> numbers = db.create_dataframe(rows=rows, column_names=["val", "is_even", "is_multiple_of_3"])
                >>> count = gp.aggregate_function("count")
                >>>
                >>> results = (
                ...     numbers.group_by("is_even")
                ...     .union(lambda t: t.group_by("is_multiple_of_3"))
                ...     .assign(count=lambda t: count(t["val"]))
                ... )
                >>>
                >>> results
                ------------------------------------
                 is_even | is_multiple_of_3 | count
                ---------+------------------+-------
                         |                1 |     2
                         |                0 |     4
                       0 |                  |     3
                       1 |                  |     3
                ------------------------------------
                (4 rows)
        """
        return DataFrameGroupingSets(
            self._dataframe,
            self._grouping_sets + other(self._dataframe)._grouping_sets,
        )

    def flatten(self) -> Set[str]:
        # noqa: D400
        """:meta private:"""
        item_set: MutableSet[Expr] = set()
        for grouping_set in self._grouping_sets:
            for item in grouping_set:
                assert isinstance(item, str), f"Grouping item {item} is not a column name."
                item_set.add(item)
        return item_set

    @property
    def dataframe(self) -> "DataFrame":
        """
        Return the source :class:`~dataframe.DataFrame` associated with grouping set.

        Returns:
            GreenplumPython DataFrame
        """
        return self._dataframe

    # FIXME: Make this function package-private
    def clause(self) -> str:
        # noqa: D400
        """:meta private:"""
        grouping_sets_str = [
            f"({','.join([item for item in grouping_set])})" for grouping_set in self._grouping_sets
        ]
        return "GROUP BY GROUPING SETS " + f"({','.join(grouping_sets_str)})"
