"""
This module contains a list of Functions methods that are currently available in GreenplumPython.
"""
from typing import Any, Optional

from greenplumpython.db import Database
from greenplumpython.func import FunctionExpr, aggregate_function, function
from greenplumpython.group import DataFrameGroupingSet


def count(
    arg: Optional[Any] = None,
    group_by: Optional[DataFrameGroupingSet] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    """
    The aggregate function in database that counts the number of rows or non-NULL values
    against a specifield column or an entire table.

    Example:
        .. highlight:: python
        .. code-block::  python

            >>> import greenplumpython.builtins.functions as F
            >>> rows = [(i,) for i in range(10)]
            >>> df = db.create_dataframe(rows=rows, column_names=["a"])
            >>> df.group_by().assign(count=lambda t: F.count())
            -------
             count
            -------
                10
            -------
            (1 row)

    """
    if arg is None:
        return FunctionExpr(aggregate_function(name="count"), ("*",), group_by=group_by, db=db)
    return FunctionExpr(aggregate_function(name="count"), (arg,), group_by=group_by, db=db)


def min(
    arg: Any,
    group_by: Optional[DataFrameGroupingSet] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    """
    The aggregate function in database that returns the minimum value in a set of values.

    Example:
        .. highlight:: python
        .. code-block::  python

            >>> import greenplumpython.builtins.functions as F
            >>> rows = [(i,) for i in range(10)]
            >>> df = db.create_dataframe(rows=rows, column_names=["a"])
            >>> df.group_by().assign(min=lambda t: F.min(t["a"]))
            -----
             min
            -----
               0
            -----
            (1 row)

    """
    return FunctionExpr(aggregate_function(name="min"), (arg,), group_by=group_by, db=db)


def max(
    arg: Any,
    group_by: Optional[DataFrameGroupingSet] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    """
    The aggregate function in database that returns the maximum value in a set of values.

    Example:
        .. highlight:: python
        .. code-block::  python

            >>> import greenplumpython.builtins.functions as F
            >>> rows = [(i,) for i in range(10)]
            >>> df = db.create_dataframe(rows=rows, column_names=["a"])
            >>> df.group_by().assign(max=lambda t: F.max(t["a"]))
            -----
             min
            -----
               9
            -----
            (1 row)

    """
    return FunctionExpr(aggregate_function(name="max"), (arg,), group_by=group_by, db=db)


def avg(
    arg: Any,
    group_by: Optional[DataFrameGroupingSet] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    """
    The aggregate function in database that computes the average value of a set.

    Example:
        .. highlight:: python
        .. code-block::  python

            >>> import greenplumpython.builtins.functions as F
            >>> rows = [(i,) for i in range(10)]
            >>> df = db.create_dataframe(rows=rows, column_names=["a"])
            >>> df.group_by().assign(avg=lambda t: F.avg(t["a"]))
            -----
             avg
            -----
             4.5
            -----
            (1 row)

    """
    return FunctionExpr(aggregate_function(name="avg"), (arg,), group_by=group_by, db=db)


def sum(
    arg: Any,
    group_by: Optional[DataFrameGroupingSet] = None,
    db: Optional[Database] = None,
) -> FunctionExpr:
    """
    The aggregate function in database that computes the sum of a set of values.

    Example:
        .. highlight:: python
        .. code-block::  python

            >>> import greenplumpython.builtins.functions as F
            >>> rows = [(i,) for i in range(10)]
            >>> df = db.create_dataframe(rows=rows, column_names=["a"])
            >>> df.group_by().assign(sum=lambda t: F.sum(t["a"]))
            -----
             avg
            -----
              45
            -----
            (1 row)

    """
    return FunctionExpr(aggregate_function(name="sum"), (arg,), group_by=group_by, db=db)


def generate_series(
    start: Any, stop: Any, step: Optional[Any] = None, db: Optional[Database] = None
) -> FunctionExpr:
    """
    The function in database that generates a series of values from :code:`start` to :code:`stop`,
    with a step size of :code:`step`.

    :code:`step` defaults to 1.

    Example:
        .. highlight:: python
        .. code-block::  python

            >>> import greenplumpython.builtins.functions as F
            >>> db.assign(id=lambda: F.generate_series(0, 9))
            ----
             id
            ----
              0
              1
              2
              3
              4
              5
              6
              7
              8
              9
            ----
            (10 rows)

    """
    return FunctionExpr(function(name="generate_series"), (start, stop, step), db=db)
