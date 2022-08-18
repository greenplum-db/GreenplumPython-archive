from typing import Any, Callable

from greenplumpython.db import Database
from greenplumpython.expr import BinaryExpr


def operator(name: str, db: Database) -> Callable[[Any, Any], BinaryExpr]:
    """
    Returns a wrap correspond to an operator in Greenplum

    Args:
        name: str: str of operator
        db: :class:`~db.Database`: database where stored operator

    Returns:
        Callable


    Example:
        .. code-block::  python

            regex_match = gp.operator("~", db)
            result = list(regex_match("hello", "h.*o").rename("is_matched").to_table().fetch())
    """

    def make_operator_expr(left: Any, right: Any) -> BinaryExpr:
        return BinaryExpr(name, left, right, db=db)

    return make_operator_expr
