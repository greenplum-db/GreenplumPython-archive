"""This module contains classes for representing expressions."""
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any, List, Optional, Union, overload
from uuid import uuid4

from greenplumpython.db import Database

if TYPE_CHECKING:
    from greenplumpython.dataframe import DataFrame


class Expr:
    """Representation of an expression for :class:`~col.Column` transformation."""

    def __init__(
        self,
        dataframe: Optional["DataFrame"] = None,
        other_dataframe: Optional["DataFrame"] = None,
    ) -> None:
        # noqa: D107
        self._dataframe = dataframe
        self._other_dataframe = other_dataframe
        self._db = None
        # self._db = dataframe._db if dataframe is not None else None  # FIXME: set it to None

    def _bind(
        self,
        dataframe: Optional["DataFrame"] = None,
        db: Optional[Database] = None,
    ) -> "Expr":
        # noqa D102
        self._db = db
        self._dataframe = dataframe
        return self

    def __hash__(self) -> int:
        # noqa: D105
        return hash(self._serialize(db=None))

    def __and__(self, other: Any) -> "BinaryExpr":
        """
        Operator :code:`&` for logical :code:`AND`.

        Returns a Binary Expression AND between self and another :class:`~expr.Expr` or constant

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> import greenplumpython.builtins.functions as F
                >>> df = db.assign(id=lambda: F.generate_series(0, 9))
                >>> df[lambda t: (t["id"] >= 3) & (t["id"] < 8)].order_by("id")[:]
                ----
                 id
                ----
                  3
                  4
                  5
                  6
                  7
                ----
                (5 rows)

        """
        return BinaryExpr("AND", self, other)

    def __or__(self, other: Any) -> "BinaryExpr":
        """
        Operator :code:`|` for logical :code:`OR`.

        Returns a Binary Expression OR between self and another :class:`~expr.Expr` or constant

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (3,), (-2,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df[lambda t: (t["id"] >= 3) | (t["id"] < 0)].order_by("id")[:]
                ----
                 id
                ----
                 -2
                  3
                ----
                (2 rows)
        """
        return BinaryExpr("OR", self, other)

    def __eq__(self, other: Any) -> "BinaryExpr":
        """
        Operator :code:`==`.

        Returns a Binary Expression EQUAL between self and another :class:`~expr.Expr` or constant

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (3,), (2,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df[lambda t: t["id"] == 2].order_by("id")[:]
                ----
                 id
                ----
                  2
                  2
                ----
                (2 rows)
        """
        if other is None:
            return BinaryExpr("IS", self, other)
        return BinaryExpr("=", self, other)

    def __lt__(self, other: Any) -> "BinaryExpr":
        """
        Operator :code:`<`.

        Returns a Binary Expression LESS THAN between self and another :class:`~expr.Expr` or constant

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (3,), (4,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df[lambda t: t["id"] < 3].order_by("id")[:]
                ----
                 id
                ----
                  1
                  2
                ----
                (2 rows)
        """
        if other is None:
            return BinaryExpr("IS NOT", self, other)
        return BinaryExpr("<", self, other)

    def __le__(self, other: Any) -> "BinaryExpr":
        """
        Operator :code:`<=`.

        Returns a Binary Expression LESS EQUAL between self and another :class:`~expr.Expr` or constant

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (3,), (4,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df[lambda t: t["id"] <= 3].order_by("id")[:]
                ----
                 id
                ----
                  1
                  2
                  3
                ----
                (3 rows)
        """
        return BinaryExpr("<=", self, other)

    def __gt__(self, other: Any) -> "BinaryExpr":
        """
        Operator :code:`>`.

        Returns a Binary Expression GREATER THAN between self and another :class:`~expr.Expr` or constant

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (3,), (4,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df[lambda t: t["id"] > 3].order_by("id")[:]
                ----
                 id
                ----
                  4
                ----
                (1 row)
        """
        return BinaryExpr(">", self, other)

    def __ge__(self, other: Any) -> "BinaryExpr":
        """
        Operator :code:`>=`.

        Returns a Binary Expression GREATER EQUAL between self and another :class:`~expr.Expr` or constant

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (3,), (4,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df[lambda t: t["id"] >= 3].order_by("id")[:]
                ----
                 id
                ----
                  3
                  4
                ----
                (2 rows)
        """
        return BinaryExpr(">=", self, other)

    def __ne__(self, other: Any) -> "BinaryExpr":
        """
        Operator :code:`!=`.

        Returns a Binary Expression NOT EQUAL between self and another :class:`~expr.Expr` or constant

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (3,), (2,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df[lambda t: t["id"] != 2].order_by("id")[:]
                ----
                 id
                ----
                  1
                  3
                ----
                (2 rows)
        """
        if other is None:
            return BinaryExpr("IS NOT", self, other)
        return BinaryExpr("!=", self, other)

    def __mod__(self, other: Any) -> "BinaryExpr":
        """
        Operator :code:`%`.

        Returns a Binary Expression Modulo between an :class:`~expr.Expr` and another :class:`~expr.Expr` or constant

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (3,), (4,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df.assign(mod2=lambda t: t["id"] % 2).order_by("id")[:]
                -----------
                 id | mod2
                ----+------
                  1 |    1
                  2 |    0
                  3 |    1
                  4 |    0
                -----------
                (4 rows)
        """
        return BinaryExpr("%", self, other)

    def __add__(self, other: Any) -> "BinaryExpr":
        """
        Operator :code:`+`.

        Returns a Binary Expression Addition between an :class:`~expr.Expr` and another :class:`~expr.Expr` or constant

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (3,), (4,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df.assign(next=lambda t: t["id"] + 1).order_by("id")[:]
                -----------
                 id | next
                ----+------
                  1 |    2
                  2 |    3
                  3 |    4
                  4 |    5
                -----------
                (4 rows)
        """
        return BinaryExpr("+", self, other)

    def __sub__(self, other: Any) -> "BinaryExpr":
        """
        Operator :code:`-`.

        Returns a Binary Expression Subtraction between an :class:`~expr.Expr` and another :class:`~expr.Expr`
        or constant

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (3,), (4,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df.assign(prev=lambda t: t["id"] - 1).order_by("id")[:]
                -----------
                 id | prev
                ----+------
                  1 |    0
                  2 |    1
                  3 |    2
                  4 |    3
                -----------
                (4 rows)
        """
        return BinaryExpr("-", self, other)

    def __mul__(self, other: Any) -> "BinaryExpr":
        """
        Operator :code:`*`.

        Returns a Binary Expression Multiplication between an :class:`~expr.Expr` and another :class:`~expr.Expr`
        or constant

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (3,), (4,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df.assign(double=lambda t: t["id"] * 2).order_by("id")[:]
                -------------
                 id | double
                ----+--------
                  1 |      2
                  2 |      4
                  3 |      6
                  4 |      8
                -------------
                (4 rows)
        """
        return BinaryExpr("*", self, other)

    def __truediv__(self, other: Any) -> "BinaryExpr":
        """
        Operator :code:`/`.

        Returns a Binary Expression Division between an :class:`~expr.Expr` and another :class:`~expr.Expr` or constant.
        It results integer division between two integers, and true division if one of the arguments is a float.

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (3,), (4,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df.assign(true_div=lambda t: t["id"] / 2).order_by("id")[:]
                ---------------
                 id | true_div
                ----+----------
                  1 |        0
                  2 |        1
                  3 |        1
                  4 |        2
                ---------------
                (4 rows)
        """
        return BinaryExpr("/", self, other)

    def __pos__(self) -> "UnaryExpr":
        """
        Operator :code:`+`.

        Returns a Unary Expression POSITIVE of self

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (-3,), (-2,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df.assign(pos=lambda t: +t["id"]).order_by("id")[:]
                ----------
                 id | pos
                ----+-----
                 -3 |  -3
                 -2 |  -2
                  1 |   1
                  2 |   2
                ----------
                (4 rows)

        """
        return UnaryExpr("+", self)

    def __neg__(self) -> "UnaryExpr":
        """
        Operator :code:`-`.

        Returns a Unary Expression NEGATIVE of self

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (-3,), (-2,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df.assign(neg=lambda t: -t["id"]).order_by("id")[:]
                ----------
                 id | neg
                ----+-----
                 -3 |   3
                 -2 |   2
                  1 |  -1
                  2 |  -2
                ----------
                (4 rows)
        """
        return UnaryExpr("-", self)

    def __abs__(self) -> "UnaryExpr":
        """
        Operator :code:`abs()`.

        Returns a Unary Expression ABSOLUTE of self

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1,), (2,), (-3,), (-2,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df.assign(abs=lambda t: abs(t["id"])).order_by("id")[:]
                ----------
                 id | abs
                ----+-----
                 -3 |   3
                 -2 |   2
                  1 |   1
                  2 |   2
                ----------
                (4 rows)
        """
        return UnaryExpr("ABS", self)

    def __invert__(self) -> "UnaryExpr":
        """
        Operator :code:`~` for logical :code:`NOT`.

        Returns a Unary Expression NOT of self

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(1, True,), (2, False,), (3, False,), (4, True,)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id", "is"])
                >>> df.assign(inv=lambda t: ~t["is"]).order_by("id")[:]
                --------------------
                 id | is    | inv
                ----+-------+-------
                  1 |     1 |     0
                  2 |     0 |     1
                  3 |     0 |     1
                  4 |     1 |     0
                --------------------
                (4 rows)
        """
        return UnaryExpr("NOT", self)

    def like(self, pattern: str) -> "BinaryExpr":
        """
        Return BinaryExpr for pattern matching with the `LIKE` clause in SQL.

        Args:
            pattern: str: regex pattern

        Returns:
            Expr

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [("aaa",), ("bba",), ("acac",)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df.assign(
                ...     a_start=lambda t: t["id"].like(r"a%"),
                ...     a_end=lambda t: t["id"].like(r"%a"),
                ...     a_middle=lambda t: t["id"].like(r"%a%"),
                ...     a_sec_posi=lambda t: t["id"].like(r"_a%")
                ... ).order_by("id")[:]
                ------------------------------------------------
                 id   | a_start | a_end | a_middle | a_sec_posi
                ------+---------+-------+----------+------------
                 aaa  |       1 |     1 |        1 |          1
                 acac |       1 |     0 |        1 |          0
                 bba  |       0 |     1 |        1 |          0
                ------------------------------------------------
                (3 rows)
        """
        return BinaryExpr("LIKE", self, pattern)

    def __str__(self) -> str:
        """Return string statement of Expr."""
        return self._serialize(db=None)

    def _serialize(self, db: Optional[Database] = None) -> str:
        raise NotImplementedError()

    # NOTE: We cannot use __contains__() because the return value will always
    # be converted to bool.
    #
    # NOTE: Nested IN expression, e.g. `t["a"].in_(t2.["b"].in_(t3["c"]))`
    # is not supported yet. We probably should not encourge user to write
    # nested IN expressions.
    def in_(self, container: Union["Expr", List[Any]]) -> "InExpr":
        """
        Test whether each value of current :class:`~expr.Expr` is in the container.

        It is analogous to the built-in `in` operator of Python and SQL.

        Args:
            container: A collection of values. It can either be another
                :class:`~expr.Expr` representing a transformed column of
                GreenplumPython :class:`~dataframe.DataFrame`, or a `list` of values of the same type as the
                current :class:`~expr.Expr`.

        Returns:
            :class:`~expr.InExpr`: A boolean :class:`~expr.Expr` whose values are of the\
                same length as the current :class:`~expr.Expr`.

        Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(i,) for i in range(5)]
                >>> df = db.create_dataframe(rows=rows, column_names=["id"])
                >>> df.assign(in_list=lambda t: t["id"].in_([1, 2, 3])).order_by("id")[:]
                --------------
                 id | in_list
                ----+---------
                  0 |       0
                  1 |       1
                  2 |       1
                  3 |       1
                  4 |       0
                --------------
                (5 rows)
        """
        return InExpr(self, container, self._dataframe)


import psycopg2.sql


def _serialize_to_expr(obj: Any, db: Optional[Database] = None) -> str:
    # noqa: D400
    """
    :meta private:

    Converts any Python object to a SQL expression.

    Note:
        It is OK to consider UTF-8 only since all `strs` are encoded in UTF-8
        in Python 3 and Python 2 is EOL officially.
    """
    if isinstance(obj, Expr):
        return obj._serialize(db=db)
    assert db is not None
    ret: str = psycopg2.sql.Literal(obj).as_string(db._conn)
    return ret


class BinaryExpr(Expr):
    """Representation of a Binary Expression."""

    @singledispatchmethod
    def _init(
        self,
        operator: str,
        left: Any,
        right: Any,
    ):
        # noqa: D107
        dataframe = left._dataframe if isinstance(left, Expr) else None
        if dataframe is not None and isinstance(right, Expr):
            dataframe = right._dataframe
        other_dataframe = left._other_dataframe if isinstance(left, Expr) else None
        if other_dataframe is not None and isinstance(right, Expr):
            other_dataframe = right._other_dataframe
        super().__init__(dataframe=dataframe, other_dataframe=other_dataframe)
        self._operator = operator
        self._left = left
        self._right = right

    @overload
    def __init__(
        self,
        operator: str,
        left: "Expr",
        right: "Expr",
    ):
        # noqa: D107
        ...

    @overload
    def __init__(
        self,
        operator: str,
        left: "Expr",
        right: int,
    ):
        # noqa: D107
        ...

    @overload
    def __init__(
        self,
        operator: str,
        left: "Expr",
        right: str,
    ):
        # noqa: D107
        ...

    def __init__(
        self,
        operator: str,
        left: Any,
        right: Any,
    ):
        # noqa: D107 D205 D400
        """

        Args:
            left: Any : could be an :class:`Expr` or object in primitive types (int, str, etc)
            right: Any : could be an :class:`Expr` or object in primitive types (int, str, etc)
        """
        self._init(operator, left, right)

    def _serialize(self, db: Optional[Database] = None) -> str:
        left_str = _serialize_to_expr(self._left, db=db)
        right_str = _serialize_to_expr(self._right, db=db)
        return f"({left_str} {self._operator} {right_str})"


class UnaryExpr(Expr):
    """Representation of a Unary Expression."""

    def __init__(
        self,
        operator: str,
        right: Any,
    ):
        # noqa: D107
        dataframe, other_dataframe = (
            (right._dataframe, right._other_dataframe) if isinstance(right, Expr) else (None, None)
        )
        super().__init__(dataframe=dataframe, other_dataframe=other_dataframe)
        self.operator = operator
        self.right = right

    def _serialize(self, db: Optional[Database] = None) -> str:
        right_str = _serialize_to_expr(self.right, db=db)
        return f"{self.operator} ({right_str})"


class InExpr(Expr):
    # noqa: D101
    def __init__(
        self,
        item: "Expr",
        container: Union["Expr", List[Any]],
        dataframe: Optional["DataFrame"] = None,
    ) -> None:
        # noqa: D107
        super().__init__(
            dataframe,
            other_dataframe=container._dataframe if isinstance(container, Expr) else None,
        )
        self._item = item
        self._container = container

    def _serialize(self, db: Optional[Database] = None) -> str:
        if isinstance(self._container, Expr):
            assert self._other_dataframe is not None, "DataFrame of container is unknown."
        # Using either IN or = any() will violate
        # https://wiki.postgresql.org/wiki/Don't_Do_This#Don.27t_use_NOT_IN
        # when combining with `～` (bitwise not) operator.
        container_name: str = "cte_" + uuid4().hex
        if isinstance(self._container, Expr) and self._other_dataframe is not None:
            return (
                f"(EXISTS (SELECT FROM {self._other_dataframe._name}"
                f" WHERE ({self._container._serialize(db=db)} = {self._item._serialize(db=db)})))"
            )

        return (
            f'(EXISTS (SELECT FROM unnest({_serialize_to_expr(self._container, db=db)}) AS "{container_name}"'
            f' WHERE ("{container_name}" = {self._item._serialize(db=db)})))'
        )
