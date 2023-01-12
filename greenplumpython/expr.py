"""
This module creates a Python object Expr.
"""
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any, List, Optional, Union, overload

from greenplumpython.db import Database

if TYPE_CHECKING:
    from greenplumpython.dataframe import DataFrame


class Expr:
    """
    Representation of Expr. It can be

        - a Column
        - an Operator
        - a Function
    """

    def __init__(
        self,
        dataframe: Optional["DataFrame"] = None,
        other_dataframe: Optional["DataFrame"] = None,
        db: Optional[Database] = None,
    ) -> None:
        self._dataframe = dataframe
        self._other_dataframe = other_dataframe
        self._db = db if db is not None else (dataframe.db if dataframe is not None else None)

    def __hash__(self) -> int:
        return hash(self.serialize())

    def __and__(self, other: Any) -> "Expr":
        """
        Operator **&**

        Returns a Binary Expression AND between self and another :class:`Expr` or constant

        Example:
            .. code-block::  Python

                t["type"] == "type_1" & t["val"] > 0

        """
        return BinaryExpr("AND", self, other)

    def __or__(self, other: Any) -> "Expr":
        """
        Operator **|**

        Returns a Binary Expression OR between self and another :class:`Expr` or constant

        Example:
            .. code-block::  Python

                t["type"] == "type_1" | t["type"] == "type_2"
        """
        return BinaryExpr("OR", self, other)

    def __eq__(self, other: Any) -> "Expr":
        """
        Operator **==**

        Returns a Binary Expression EQUAL between self and another :class:`Expr` or constant

        Example:
            .. code-block::  Python

                t["type"] == "type_1"
        """
        if other is None:
            return BinaryExpr("IS", self, other)
        return BinaryExpr("=", self, other)

    def __lt__(self, other: Any) -> "Expr":
        """
        Operator **<**

        Returns a Binary Expression LESS THAN between self and another :class:`Expr` or constant

        Example:
            .. code-block::  Python

                t["val"] < 0
        """
        if other is None:
            return BinaryExpr("IS NOT", self, other)
        return BinaryExpr("<", self, other)

    def __le__(self, other: Any) -> "Expr":
        """
        Operator **<=**

        Returns a Binary Expression LESS EQUAL between self and another :class:`Expr` or constant

        Example:
            .. code-block::  Python

                t["val"] <= 0
        """
        return BinaryExpr("<=", self, other)

    def __gt__(self, other: Any) -> "Expr":
        """
        Operator **>**

        Returns a Binary Expression GREATER THAN between self and another :class:`Expr` or constant

        Example:
            .. code-block::  Python

                t["val"] > 0
        """
        return BinaryExpr(">", self, other)

    def __ge__(self, other: Any) -> "Expr":
        """
        Operator **>=**

        Returns a Binary Expression GREATER EQUAL between self and another :class:`Expr` or constant

        Example:
            .. code-block::  Python

                t["val"] >= 0
        """
        return BinaryExpr(">=", self, other)

    def __ne__(self, other: Any) -> "Expr":
        """
        Operator **!=**

        Returns a Binary Expression NOT EQUAL between self and another :class:`Expr` or constant

        Example:
            .. code-block::  Python

                t["val"] != 0
        """
        return BinaryExpr("!=", self, other)

    def __mod__(self, other: Any) -> "Expr":
        """
        Operator **%**

        Returns a Binary Expression Modulo between an :class:`Expr` and another :class:`Expr` or constant

        Example:
            .. code-block::  Python

                t["val"] % 2
        """
        return BinaryExpr("%", self, other)

    def __add__(self, other: Any) -> "Expr":
        """
        Operator **+**

        Returns a Binary Expression Addition between an :class:`Expr` and another :class:`Expr` or constant

        Example:
            .. code-block::  Python

                t["val"] + 2
        """
        return BinaryExpr("+", self, other)

    def __sub__(self, other: Any) -> "Expr":
        """
        Operator **-**

        Returns a Binary Expression Subtraction between an :class:`Expr` and another :class:`Expr` or constant

        Example:
            .. code-block::  Python

                t["val"] - 2
        """
        return BinaryExpr("-", self, other)

    def __mul__(self, other: Any) -> "Expr":
        """
        Operator *****

        Returns a Binary Expression Multiplication between an :class:`Expr` and another :class:`Expr` or constant

        Example:
            .. code-block::  Python

                t["val"] * 2
        """
        return BinaryExpr("*", self, other)

    def __truediv__(self, other: Any) -> "Expr":
        """
        Operator **/**

        Returns a Binary Expression Division between an :class:`Expr` and another :class:`Expr` or constant.
        It results integer division between two integers, and true division if one of the arguments is a float.

        Example:
            .. code-block::  Python

                t["val"] / 2
        """
        return BinaryExpr("/", self, other)

    def __pos__(self) -> "Expr":
        """
        Operator **+**

        Returns a Unary Expression POSITIVE of self

        Example:
            .. code-block::  Python

                +t["val"]
        """
        return UnaryExpr("+", self)

    def __neg__(self) -> "Expr":
        """
        Operator **-**

        Returns a Unary Expression NEGATIVE of self

        Example:
            .. code-block::  Python

                -t["val"]
        """
        return UnaryExpr("-", self)

    def __abs__(self) -> "Expr":
        """
        Operator **abs()**

        Returns a Unary Expression ABSOLUTE of self

        Example:
            .. code-block::  Python

                abs(t["val"])
        """
        return UnaryExpr("ABS", self)

    def __invert__(self) -> "Expr":
        """
        Operator **~**

        Returns a Unary Expression NOT of self

        Example:
            .. code-block::  Python

                not(t["val"])
        """
        return UnaryExpr("NOT", self)

    def like(self, pattern: str) -> "Expr":
        """
        Returns BinaryExpr in order to apply LIKE statement on self with pattern

        Args:
            pattern: str: regex pattern

        Returns:
            Expr

        Example:
            Select rows where id begins with "a"

            .. code-block::  Python

                t[t["id"].like(r"a%")]

        """
        return BinaryExpr("LIKE", self, pattern)

    def __str__(self) -> str:
        """
        Returns string statement of Expr
        """
        return self.serialize()

    def serialize(self) -> str:
        raise NotImplementedError()

    @property
    def db(self) -> Optional[Database]:
        """
        Returns Expr associated :class:`~db.Database`

        Returns:
            Optional[:class:`~db.Database`]: Database associated with :class:`Expr`
        """
        return self._db

    @property
    def dataframe(self) -> Optional["DataFrame"]:
        """
        Returns Expr associated :class:`~dataframe.DataFrame`

        Returns:
            Optional[:class:`~dataframe.DataFrame`]: GreenplumPython DataFrame associated with :class:`Expr`
        """
        return self._dataframe

    @property
    def other_dataframe(self) -> Optional["DataFrame"]:
        return self._other_dataframe

    # NOTE: We cannot use __contains__() because the return value will always
    # be converted to bool.
    #
    # NOTE: Nested IN expression, e.g. `t["a"].in_(t2.["b"].in_(t3["c"]))`
    # is not supported yet. We probably should not encourge user to write
    # nested IN expressions.
    def in_(self, container: Union["Expr", List[Any]]) -> "InExpr":
        """
        Tests whether each value of current :class:`Expr` is in the container.

        It is analogous to the built-in `in` operator of Python and SQL.

        Args:
            container: A collection of values. It can either be another
                :class:`Expr` representing a transformed column of
                GreenplumPython :class:`DataFrame`, or a `list` of values of the same type as the
                current `Expr`.

        Returns:
            :class:`InExpr`: A boolean :class:`Expr` whose values are of the
                same length as the current :class:`Expr`.
        """
        return InExpr(self, container, self.dataframe, self.db)


from psycopg2.extensions import adapt  # type: ignore


def serialize(value: Any) -> str:
    """
    :meta private:

    Converts a value to UTF-8 encoded str to be used in a SQL statement

    Note:
        It is OK to consider UTF-8 only since all `strs` are encoded in UTF-8
        in Python 3 and Python 2 is EOL officially.
    """
    if isinstance(value, Expr):
        return value.serialize()
    return adapt(value).getquoted().decode("utf-8")  # type: ignore


class BinaryExpr(Expr):
    """
    Inherited from :class:`Expr`.

    Representation of a Binary Expression
    """

    @singledispatchmethod
    def _init(
        self,
        operator: str,
        left: Any,
        right: Any,
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ):
        dataframe = left.dataframe if isinstance(left, Expr) else None
        if dataframe is not None and isinstance(right, Expr):
            dataframe = right.dataframe
        other_dataframe = left.other_dataframe if isinstance(left, Expr) else None
        if other_dataframe is not None and isinstance(right, Expr):
            other_dataframe = right.other_dataframe
        super().__init__(dataframe=dataframe, other_dataframe=other_dataframe, db=db)
        self.operator = operator
        self.left = left
        self.right = right

    @overload
    def __init__(
        self,
        operator: str,
        left: "Expr",
        right: "Expr",
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ):
        ...

    @overload
    def __init__(
        self,
        operator: str,
        left: "Expr",
        right: int,
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ):
        ...

    @overload
    def __init__(
        self,
        operator: str,
        left: "Expr",
        right: str,
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ):
        ...

    def __init__(
        self,
        operator: str,
        left: Any,
        right: Any,
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ):
        """

        Args:
            left: Any : could be an :class:`Expr` or object in primitive types (int, str, etc)
            right: Any : could be an :class:`Expr` or object in primitive types (int, str, etc)
        """
        self._init(operator, left, right, as_name, db)

    def serialize(self) -> str:
        from greenplumpython.expr import serialize

        left_str = serialize(self.left)
        right_str = serialize(self.right)
        return f"({left_str} {self.operator} {right_str})"


class UnaryExpr(Expr):
    """
    Inherited from :class:`Expr`.

    Representation of a Unary Expression.
    """

    def __init__(
        self,
        operator: str,
        right: Any,
        db: Optional[Database] = None,
    ):
        """

        Args:
            right: :class:`Expr`
        """
        dataframe, other_dataframe = (
            (right.dataframe, right.other_dataframe) if isinstance(right, Expr) else (None, None)
        )
        super().__init__(dataframe=dataframe, other_dataframe=other_dataframe, db=db)
        self.operator = operator
        self.right = right

    def serialize(self) -> str:
        right_str = str(self.right)
        return f"{self.operator}({right_str})"


class InExpr(Expr):
    def __init__(
        self,
        item: "Expr",
        container: Union["Expr", List[Any]],
        dataframe: Optional["DataFrame"] = None,
        db: Optional[Database] = None,
    ) -> None:
        super().__init__(
            dataframe,
            other_dataframe=container.dataframe if isinstance(container, Expr) else None,
            db=db,
        )
        self._item = item
        self._container = container

    def serialize(self) -> str:
        if isinstance(self._container, Expr):
            assert self.other_dataframe is not None, "DataFrame of container is unknown."
        container_str = (
            f"SELECT {self._container.serialize()} FROM {self.other_dataframe.name}"
            if isinstance(self._container, Expr) and self.other_dataframe is not None
            else serialize(self._container)
        )
        return f"{self._item.serialize()} = ANY({container_str})"
