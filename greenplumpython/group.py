from typing import TYPE_CHECKING, Callable, Iterable, List, MutableSet

if TYPE_CHECKING:
    from .expr import Expr
    from .func import FunctionExpr
    from .table import Table


class TableRowGroup:
    def __init__(self, table: "Table", grouping_sets: List[List["Expr"]]) -> None:
        self._table = table
        self._grouping_sets = grouping_sets

    # TODO: provide apply() instead of aggregate() for consistency. That is:
    # `t.group_by(["is_even"])[["val"]].apply(count)`.
    # But how about `t[[*]].apply(count)` ?
    def aggregate(self, func: Callable, *args: "Expr") -> "FunctionExpr":
        qualified_args = [self._table[e] if isinstance(e, str) else e for e in args]
        return func(*qualified_args, group_by=self)

    def union(self, other: "TableRowGroup") -> "TableRowGroup":
        assert self._table == other._table
        return TableRowGroup(self._table, self._grouping_sets + other._grouping_sets)

    def get_targets(self) -> Iterable["Expr"]:
        item_set: MutableSet[Expr] = set()
        for grouping_set in self._grouping_sets:
            for group_by_item in grouping_set:
                item_set.add(group_by_item)
        return item_set

    @property
    def table(self) -> "Table":
        return self._table

    def make_group_by_clause(self) -> str:
        grouping_sets_str = [
            f"({','.join([str(item) for item in grouping_set])})"
            for grouping_set in self._grouping_sets
        ]
        return "GROUP BY GROUPING SETS " + f"({','.join(grouping_sets_str)})"
