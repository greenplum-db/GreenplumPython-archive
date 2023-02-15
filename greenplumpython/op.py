# noqa: D100
from typing import Any, Callable

from greenplumpython.db import Database
from greenplumpython.expr import BinaryExpr


def operator(name: str, db: Database) -> Callable[[Any, Any], BinaryExpr]:
    """
    Get access to the operator in database.

    Args:
        name: `str`: the string that represents the operator
        db: :class:`~db.Database`: the database where the operator is stored

    Returns:
        :class:`Callable`


    Example:
        .. highlight:: python
        .. code-block::  python

            >>> regex_match = gp.operator("~", db)
            >>> result = db.assign(is_matched=lambda: regex_match("hello", "h.*o"))
            >>> result
            ------------
             is_matched
            ------------
                      1
            ------------
            (1 row)
    """

    def make_operator_expr(left: Any, right: Any) -> BinaryExpr:
        return BinaryExpr(name, left=left, right=right, db=db)

    return make_operator_expr
