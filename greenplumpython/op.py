from typing import Callable

from .db import Database
from .expr import BinaryExpr, Expr, UnaryExpr


def binaryOperator(name: str, db: Database) -> Callable[[Expr, Expr], BinaryExpr]:
    def make_operator_expr(left: Expr, right: Expr) -> BinaryExpr:
        return BinaryExpr(name, left, right, db=db)

    return make_operator_expr


def unaryOperator(name: str, db: Database) -> Callable[[Expr], UnaryExpr]:
    def make_operator_expr(right: Expr) -> UnaryExpr:
        return UnaryExpr(name, right, db=db)

    return make_operator_expr
