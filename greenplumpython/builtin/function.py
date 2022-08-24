from typing import Any, Iterable, Optional, Union

from greenplumpython.db import Database
from greenplumpython.expr import Expr
from greenplumpython.func import FunctionExpr
from greenplumpython.group import TableRowGroup


def count(
    arg: Optional[Any] = None,
    group_by: Optional[TableRowGroup] = None,
    as_name: Optional[str] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    if arg is None:
        return FunctionExpr("count", ["*"], group_by=group_by, as_name=as_name, db=db)
    return FunctionExpr("count", [arg], group_by=group_by, as_name=as_name, db=db)


def min(
    arg: Any,
    group_by: Optional[TableRowGroup] = None,
    as_name: Optional[str] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    return FunctionExpr("min", [arg], group_by=group_by, as_name=as_name, db=db)


def max(
    arg: Any,
    group_by: Optional[TableRowGroup] = None,
    as_name: Optional[str] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    return FunctionExpr("max", [arg], group_by=group_by, as_name=as_name, db=db)


def avg(
    arg: Any,
    group_by: Optional[TableRowGroup] = None,
    as_name: Optional[str] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    return FunctionExpr("avg", [arg], group_by=group_by, as_name=as_name, db=db)


def sum(
    arg: Any,
    group_by: Optional[TableRowGroup] = None,
    as_name: Optional[str] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    return FunctionExpr("sum", [arg], group_by=group_by, as_name=as_name, db=db)
