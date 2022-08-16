from typing import Any, Iterable, Optional, Union

from greenplumpython.db import Database
from greenplumpython.expr import Expr
from greenplumpython.func import FunctionCall, function


def count(
    arg: Optional[Any] = None,
    group_by: Optional[Iterable[Union[Expr, str]]] = None,
    as_name: Optional[str] = None,
    db: Optional[Database] = None,
) -> FunctionCall:
    if arg is None:
        return function("count")(group_by=group_by, as_name=as_name, db=db)
    return function("count")(arg, group_by=group_by, as_name=as_name, db=db)


def min(
    arg: Any,
    group_by: Optional[Iterable[Union[Expr, str]]] = None,
    as_name: Optional[str] = None,
    db: Optional[Database] = None,
) -> FunctionCall:
    return function("min")(arg, group_by=group_by, as_name=as_name, db=db)


def max(
    arg: Any,
    group_by: Optional[Iterable[Union[Expr, str]]] = None,
    as_name: Optional[str] = None,
    db: Optional[Database] = None,
) -> FunctionCall:
    return function("max")(arg, group_by=group_by, as_name=as_name, db=db)


def avg(
    arg: Any,
    group_by: Optional[Iterable[Union[Expr, str]]] = None,
    as_name: Optional[str] = None,
    db: Optional[Database] = None,
) -> FunctionCall:
    return function("avg")(arg, group_by=group_by, as_name=as_name, db=db)


def sum(
    arg: Any,
    group_by: Optional[Iterable[Union[Expr, str]]] = None,
    as_name: Optional[str] = None,
    db: Optional[Database] = None,
) -> FunctionCall:
    return function("sum")(arg, group_by=group_by, as_name=as_name, db=db)
