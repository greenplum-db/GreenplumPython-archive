from typing import Any, Callable

from greenplumpython.db import Database
from greenplumpython.expr import BinaryExpr


def operator(name: str, db: Database) -> Callable[[Any, Any], BinaryExpr]:
    """
    Returns a wrap correspond to an operator in Greenplum
    """

    def make_operator_expr(left: Any, right: Any) -> BinaryExpr:
        return BinaryExpr(name, left, right, db=db)

    return make_operator_expr
