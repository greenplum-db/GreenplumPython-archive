from typing import Any, Callable

from .db import Database
from .expr import BinaryExpr


def operator(name: str, db: Database) -> Callable[[Any, Any], BinaryExpr]:
    def make_operator_expr(left: Any, right: Any) -> BinaryExpr:
        return BinaryExpr(name, left, right, db=db)

    return make_operator_expr
