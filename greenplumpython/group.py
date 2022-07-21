from typing import TYPE_CHECKING, Callable, Iterable

if TYPE_CHECKING:
    from .table import Table
    from .expr import Expr
    from .func import FunctionExpr


class TableRowGroup:
    def __init__(self, table: "Table", group_by: Iterable["Expr"]) -> None:
        self._table = table
        self._group_by = [self._table[e] if isinstance(e, str) else e for e in group_by]

    def aggregate(self, func: Callable, *args: "Expr") -> "FunctionExpr":
        qualified_args = [self._table[e] if isinstance(e, str) else e for e in args]
        return func(*qualified_args, group_by=self._group_by)
