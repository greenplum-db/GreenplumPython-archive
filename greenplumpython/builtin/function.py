from typing import Any, Optional

from greenplumpython.db import Database
from greenplumpython.func import FunctionExpr, aggregate_function, function
from greenplumpython.group import DataFrameGroupingSets


def count(
    arg: Optional[Any] = None,
    group_by: Optional[DataFrameGroupingSets] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    if arg is None:
        return FunctionExpr(aggregate_function(name="count"), ("*",), group_by=group_by, db=db)
    return FunctionExpr(aggregate_function(name="count"), (arg,), group_by=group_by, db=db)


def min(
    arg: Any,
    group_by: Optional[DataFrameGroupingSets] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    return FunctionExpr(aggregate_function(name="min"), (arg,), group_by=group_by, db=db)


def max(
    arg: Any,
    group_by: Optional[DataFrameGroupingSets] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    return FunctionExpr(aggregate_function(name="max"), (arg,), group_by=group_by, db=db)


def avg(
    arg: Any,
    group_by: Optional[DataFrameGroupingSets] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    return FunctionExpr(aggregate_function(name="avg"), (arg,), group_by=group_by, db=db)


def sum(
    arg: Any,
    group_by: Optional[DataFrameGroupingSets] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    return FunctionExpr(aggregate_function(name="sum"), (arg,), group_by=group_by, db=db)


def generate_series(
    start: Any, stop: Any, step: Optional[Any] = None, db: Optional[Database] = None
) -> FunctionExpr:
    return FunctionExpr(function(name="generate_series"), (start, stop, step), db=db)
