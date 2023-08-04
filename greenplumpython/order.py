# noqa: D100
import sys
from typing import TYPE_CHECKING, List, Optional

from greenplumpython.col import Column

if TYPE_CHECKING:
    from greenplumpython.dataframe import DataFrame


class DataFrameOrdering:
    """Ordering specification of a :class:`~dataframe.DataFrame`."""

    def __init__(
        self,
        dataframe: "DataFrame",
        column_name_list: List[str],
        ascending_list: List[Optional[bool]],
        nulls_first_list: List[Optional[bool]],
        operator_list: List[Optional[str]],
    ) -> None:
        # noqa D107
        self._dataframe = dataframe
        self._column_name_list = column_name_list
        self._ascending_list = ascending_list
        self._nulls_first_list = nulls_first_list
        self._operator_list = operator_list

    def order_by(
        self,
        column_name: str,
        ascending: Optional[bool] = None,
        nulls_first: Optional[bool] = None,
        operator: Optional[str] = None,
    ) -> "DataFrameOrdering":
        """
        Refine the order by adding another order definition to break the tie.

        Args:
            column_name: name of the column to order the GreenplumPython DataFrame by
            ascending: Optional[Bool]: Define ascending of order, True = ASC / False = DESC
            nulls_first: Optional[bool]: Define if nulls will be ordered first or last, True = First / False = Last
            operator: Optional[str]: Define the order with the operator. **Can't be combined with ascending.**

        Returns:
            DataFrameOrdering : :class:`~dataframe.DataFrame` ordered by the given arguments

        Example:
            .. code-block::  Python

                >>> rows = [(1, 2), (1, 3), (2, 2), (3, 1), (3, 4)]
                >>> t = db.create_dataframe(rows=rows, column_names=["id", "num"])
                >>> ret = t.order_by("id").order_by("num", ascending=False)[:5]
                >>> t.order_by("id").order_by("num", ascending=False)[:]
                ----------
                 id | num
                ----+-----
                  1 |   3
                  1 |   2
                  2 |   2
                  3 |   4
                  3 |   1
                ----------
                (5 rows)
        """
        if ascending is not None and operator is not None:
            raise Exception(
                "Could not use 'ascending' and 'operator' together to order by one column"
            )
        return DataFrameOrdering(
            self._dataframe,
            self._column_name_list + [column_name],
            self._ascending_list + [ascending],
            self._nulls_first_list + [nulls_first],
            self._operator_list + [operator],
        )

    def __getitem__(self, rows: slice) -> "DataFrame":
        """
        Return a slice of :class:`~dataframe.DataFrame` in the specified order.

        Args:
            rows: slice: number of first rows

        Returns:
            :class:`~dataframe.DataFrame` with the rows in order

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(4,), (3,), (2,), (1,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df.order_by("id")[:]
                ----
                id
                ----
                1
                2
                3
                4
                ----
                (4 rows)
        """
        from greenplumpython.dataframe import DataFrame

        if rows.step is not None:
            raise NotImplementedError()
        offset_clause = "" if rows.start is None else f"OFFSET {rows.start}"
        limit = (
            sys.maxsize
            if rows.stop is None
            else rows.stop
            if rows.start is None
            else rows.stop - rows.start
        )
        return DataFrame(
            f"SELECT * FROM {self._dataframe._name} {self._clause()} LIMIT {limit} {offset_clause}",
            parents=[self._dataframe],
        )

    def _clause(self) -> str:
        # noqa: D400 D202
        """:meta private:"""
        order_by_str: str = ",".join(
            [
                " ".join(
                    [
                        Column(self._column_name_list[i], self._dataframe)._serialize(db=None),
                        ""
                        if self._ascending_list[i] is None
                        else "ASC"
                        if self._ascending_list[i]
                        else "DESC",
                        ""
                        if self._operator_list[i] is None
                        else ("USING " + self._operator_list[i]),
                        ""
                        if self._nulls_first_list[i] is None
                        else "NULLS FIRST"
                        if self._nulls_first_list[i]
                        else "NULLS LAST",
                    ]
                )
                for i in range(len(self._column_name_list))
            ]
        )
        return "ORDER BY " + order_by_str
