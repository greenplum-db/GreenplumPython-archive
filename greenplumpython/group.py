"""Definitions for the result of grouping :class:`~dataframe.DataFrame`."""

from typing import TYPE_CHECKING, Any, Callable, List, Optional

from greenplumpython.expr import Expr, _serialize_to_expr

if TYPE_CHECKING:
    from greenplumpython.dataframe import DataFrame
    from greenplumpython.func import FunctionExpr


class DataFrameGroupingSet:
    """
    Represent a set of groups of a :class:`~dataframe.DataFrame`.

    It can be created from:

    - :meth:`~dataframe.DataFrame.group_by`, or
    - :meth:`DataFrameGroupingSet.union` of multiple :meth:`~dataframe.DataFrame.group_by`.

    An :class:`~func.AggregateFunction` can be applied to each of the groups in
    :class:`DataFrameGroupingSet` to obtain a summary.
    """

    def __init__(self, dataframe: "DataFrame", grouping_sets: List[List["Expr"]]) -> None:
        # noqa: D400
        """:meta private:"""
        self._dataframe = dataframe
        # _grouping_sets should be an ordered set to maintain stable column display order
        self._grouping_sets = grouping_sets

    def apply(
        self,
        func: Callable[["DataFrame"], "FunctionExpr"],
        expand: bool = False,
        column_name: Optional[str] = None,
    ) -> "DataFrame":
        """
        Apply a dataframe function to each group of the :code:`self` grouping set.

        The arguemnts and the return type is the same as
        :meth:`~dataframe.DataFrame.apply`.

        The differences between them are

        - :meth:`~dataframe.DataFrame.apply` operates on the entire :class:`~dataframe.DataFrame`, while\
            this method operate on only one group.
        - For :meth:`~dataframe.DataFrame.apply`, the resulting :class:`~dataframe.DataFrame` will only\
            contain the return value of the function, while for this method, the\
            resulting :class:`~dataframe.DataFrame` will contain the grouping attributes as\
            columns.

        Warning:
            An exception will be raised when the data of the resulting
            :class:`~dataframe.DataFrame` is observed if there is name conflict, possibly
            due to

            - The assigned column name in :code:`column_name` or
            - The names of members in the composite type if :code:`expend` is :code:`True`

            conflict with the name of the grouping attributes.

        Example:
            .. highlight::  python
            .. code-block::  python

                >>> rows = [(i, i % 2 == 0) for i in range(10)]
                >>> numbers = db.create_dataframe(rows=rows, column_names=["val", "is_even"])
                >>> count = gp.aggregate_function("count")
                >>> results = numbers.group_by("is_even").apply(lambda row: count(row["*"]))
                >>> results.order_by("is_even")[:]
                -----------------
                 count | is_even
                -------+---------
                     5 |       0
                     5 |       1
                -----------------
                (2 rows)

                >>> results = numbers.group_by("is_even").apply(lambda row: count(row["*"]), column_name='cnt')
                >>> results.order_by("is_even")[:]
                ---------------
                 cnt | is_even
                -----+---------
                   5 |       0
                   5 |       1
                ---------------
                (2 rows)

                >>> class array_sum:
                ...     sum: int
                ...     count: int
                ...
                >>> @gp.create_column_function
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
        from greenplumpython.func import FunctionExpr

        v: FunctionExpr = func(self._dataframe)
        assert isinstance(v, FunctionExpr), "Can only apply functions."
        return v._bind(group_by=self).apply(
            expand=expand, column_name=column_name, db=self._dataframe._db
        )

    def assign(self, **new_columns: Callable[["DataFrame"], Any]) -> "DataFrame":
        """
        Assign new columns to the current grouping set.

        **NOTE:** Existing columns cannot be reassigned.

        Args:
            new_columns: a :class:`dict` whose keys are column names and values
                are :class:`Callable`'s returning column data when applied to the
                current :class:`~group.DataFrameGroupingSet`.

        Returns:
            :class:`~dataframe.DataFrame` with the new columns.


        Example:
            .. highlight:: python
            .. code-block::  python

                >>> rows = [(i, i % 2 == 0) for i in range(10)]
                >>> numbers = db.create_dataframe(rows=rows, column_names=["val", "is_even"])
                >>> count = gp.aggregate_function("count")
                >>> sum = gp.aggregate_function("sum")
                >>> results = numbers.group_by("is_even").assign(
                ...     count=lambda t: count(t["val"]),
                ...     sum=lambda t: sum(t["val"]))
                >>> results.order_by("is_even")[:]
                -----------------------
                 is_even | count | sum
                ---------+-------+-----
                       0 |     5 |  25
                       1 |     5 |  20
                -----------------------
                (2 rows)
        """
        from greenplumpython.dataframe import DataFrame

        targets: List[str] = self._flatten()
        for k, f in new_columns.items():
            v: Any = f(self._dataframe)._bind(group_by=self)
            if isinstance(v, Expr):
                assert (
                    v._dataframe is None or v._dataframe == self._dataframe
                ), "Newly included columns must be based on the current dataframe"
            targets.append(f"{_serialize_to_expr(v, db=self._dataframe._db)} AS {k}")
        return DataFrame(
            f"SELECT {','.join(targets)} FROM {self._dataframe._name} {self._clause()}",
            parents=[self._dataframe],
        )

    def union(
        self, other: Callable[["DataFrame"], "DataFrameGroupingSet"]
    ) -> "DataFrameGroupingSet":
        """
        Union with another :class:`DataFrameGroupingSet`.

        When applying an aggregate function to the list, the function will be applied to
        each group in the grouping set individually.

        Args:
            other: a :class:`Callable` returning the result of
                   :meth:`~dataframe.DataFrame.group_by()` when applied to the current
                   :class:`~dataframe.DataFrame`.

        Returns:
            a new instance of :class:`DataFrameGroupingSet`.

        Example:
            .. highlight:: python
            .. code-block::  python

                >>> rows = [(i, i % 2 == 0, i % 3 == 0) for i in range(6)]  # 0, 1, 2, 3, 4, 5
                >>> numbers = db.create_dataframe(rows=rows, column_names=["val", "is_even", "is_multiple_of_3"])
                >>> count = gp.aggregate_function("count")
                >>> results = (
                ...     numbers.group_by("is_even")
                ...     .union(lambda t: t.group_by("is_multiple_of_3"))
                ...     .assign(count=lambda t: count(t["val"]))
                ... )
                >>> results.order_by("is_even")[:].order_by("count")[:]
                ------------------------------------
                 is_even | is_multiple_of_3 | count
                ---------+------------------+-------
                         |                1 |     2
                       0 |                  |     3
                       1 |                  |     3
                         |                0 |     4
                ------------------------------------
                (4 rows)
        """
        return DataFrameGroupingSet(
            self._dataframe,
            self._grouping_sets + other(self._dataframe)._grouping_sets,
        )

    def _flatten(self) -> List[str]:
        # noqa: D400
        """:meta private:"""
        item_list: List[str] = list()
        # Remove the duplicates and keep the input order
        for grouping_set in self._grouping_sets:
            for item in grouping_set:
                assert isinstance(item, str), f"Grouping item {item} is not a column name."
                if item not in item_list:
                    item_list.append(item)
        return item_list

    def _clause(self) -> str:
        # noqa: D400
        """:meta private:"""
        grouping_sets_str = [
            f"({','.join([item for item in grouping_set])})" for grouping_set in self._grouping_sets
        ]
        return "GROUP BY GROUPING SETS " + f"({','.join(grouping_sets_str)})"
