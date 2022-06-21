from typing import Callable

from .db import Database
from .expr import BinaryExpr, Expr


def operator(name: str, db: Database) -> Callable[[Expr, Expr], BinaryExpr]:
    def make_operator_expr(left: Expr, right: Expr) -> BinaryExpr:
        return BinaryExpr(name, left, right, db=db)

    return make_operator_expr
