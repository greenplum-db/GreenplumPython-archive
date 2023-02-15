from typing import Any, Callable, Optional

from greenplumpython.db import Database
from greenplumpython.expr import BinaryExpr


def operator(
    name: str, db: Database, schema: Optional[str] = None
) -> Callable[[Any, Any], BinaryExpr]:
    """
    Returns a wrap correspond to an operator in Greenplum

    Args:
        name: str: str of operator
        db: :class:`~db.Database`: database where stored operator

    Returns:
        :class:`Callable`


    Example:
        .. highlight:: python
        .. code-block::  python

            >>> regex_match = gp.operator("~", db)
            >>> result = db.assign(is_matched=lambda: regex_match("hello", "h.*o")
            >>> result
            ------------
             is_matched
            ------------
                      1
            ------------
            (1 row)
    """

    def make_operator_expr(left: Any, right: Any) -> BinaryExpr:
        return BinaryExpr(name, left=left, right=right, db=db, schema=schema)

    return make_operator_expr
