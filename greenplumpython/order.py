from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from .expr import Expr
    from .table import Table


class OrderedTable:
    def __init__(
        self,
        table: "Table",
        ordering_sets: List["Expr"],
        ascending_sets: Dict["Expr", bool],
        nulls_first_sets: Dict["Expr", bool],
        operator_sets: Dict["Expr", str],
    ) -> None:
        self._table = table
        self._ordering_sets = ordering_sets
        self._ascending_sets = ascending_sets
        self._nulls_first_sets = nulls_first_sets
        self._operator_sets = operator_sets

    def order_by(
        self,
        index: "Expr",
        ascending: Optional[bool] = None,
        nulls_first: Optional[bool] = None,
        operator: Optional[str] = None,
    ) -> "OrderedTable":
        return OrderedTable(
            self._table,
            self._ordering_sets + [index],
            {**self._ascending_sets, **{str(index): ascending}},
            {**self._nulls_first_sets, **{str(index): nulls_first}},
            {**self._operator_sets, **{str(index): operator}},
        )

    # FIXME : Not sure about return type
    def head(self, num: int) -> "Table":
        """
        Returns a Table
        """
        from .table import Table

        return Table(
            f"""
                SELECT * FROM {self._table.name}
                {self.make_order_by_clause()}
                LIMIT {num}
            """,
            parents=[self._table],
        )

    @property
    def table(self) -> "Table":
        return self._table

    def make_order_by_clause(self) -> str:
        # FIXME : If user define ascending and operator, will get syntax error
        order_by_str = ",".join(
            [
                (
                    f"""
            {order_index} {"" if self._ascending_sets[str(order_index)] is None else "ASC" if self._ascending_sets[str(order_index)] else "DESC"}
            {"" if self._operator_sets[str(order_index)] is None else ("USING " + self._operator_sets[str(order_index)])}
            {"" if self._nulls_first_sets[str(order_index)] is None else "NULLs FIRST" if self._nulls_first_sets[str(order_index)] else "NULLs LAST"}
            """
                )
                for order_index in self._ordering_sets
            ]
        )
        return "ORDER BY " + order_by_str
