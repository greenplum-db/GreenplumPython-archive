"""This module contains a list of predefined dataframe functions in database."""
from typing import Any, Optional

from greenplumpython.db import Database
from greenplumpython.func import FunctionExpr, aggregate_function, function
from greenplumpython.group import DataFrameGroupingSet


def count(
    arg: Optional[Any] = None,
) -> FunctionExpr:
    """
    Count the number of rows or non-NULL values against a specified column or an entire table.

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
        no_arg: tuple[()] = tuple()
        return FunctionExpr(aggregate_function(name="count"), no_arg)
    return FunctionExpr(aggregate_function(name="count"), (arg,))


def min(
    arg: Any,
) -> FunctionExpr:
    """
    Return the minimum value in a set of values.

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
    return FunctionExpr(aggregate_function(name="min"), (arg,))


def max(
    arg: Any,
) -> FunctionExpr:
    """
    Return the maximum value in a set of values.

    Example:
        .. highlight:: python
        .. code-block::  python

            >>> import greenplumpython.builtins.functions as F
            >>> rows = [(i,) for i in range(10)]
            >>> df = db.create_dataframe(rows=rows, column_names=["a"])
            >>> df.group_by().assign(max=lambda t: F.max(t["a"]))
            -----
             max
            -----
               9
            -----
            (1 row)

    """
    return FunctionExpr(aggregate_function(name="max"), (arg,))


def avg(
    arg: Any,
) -> FunctionExpr:
    """
    Calculate the average value of a set.

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
    return FunctionExpr(aggregate_function(name="avg"), (arg,))


def sum(
    arg: Any,
) -> FunctionExpr:
    """
    Calculate the sum of a set of values.

    Example:
        .. highlight:: python
        .. code-block::  python

            >>> import greenplumpython.builtins.functions as F
            >>> rows = [(i,) for i in range(10)]
            >>> df = db.create_dataframe(rows=rows, column_names=["a"])
            >>> df.group_by().assign(sum=lambda t: F.sum(t["a"]))
            -----
             sum
            -----
              45
            -----
            (1 row)

    """
    return FunctionExpr(aggregate_function(name="sum"), (arg,))


def generate_series(start: Any, stop: Any, step: Optional[Any] = None) -> FunctionExpr:
    """
    Generate a series of values from :code:`start` to :code:`stop`, with a step size of :code:`step`.

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
    return FunctionExpr(
        function(name="generate_series"), (start, stop, step) if step is not None else (start, stop)
    )
