"""
This module creates a Python object Expr.
"""
import copy
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any, Callable, Optional, Union, overload

from greenplumpython.db import Database

if TYPE_CHECKING:
    from greenplumpython.func import FunctionExpr
    from greenplumpython.table import Table
    from greenplumpython.type import Type


class Expr:
    """
    Representation of Expr. It can be

        - a Column
        - an Operator
        - a Function
    """

    def __init__(
        self,
        table: Optional["Table"] = None,
        db: Optional[Database] = None,
    ) -> None:
        self._table = table
        self._db = db if db is not None else (table.db if table is not None else None)

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
        Returns string statement of Expr, ie : name + AS (optional)
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
    def table(self) -> Optional["Table"]:
        """
        Returns Expr associated :class:`~table.Table`

        Returns:
        Optional[:class:`~table.Table`]: Table associated with :class:`Expr`
        """
        return self._table

    def as_table(self) -> "Table":
        """
        Returns a :class:`~table.Table`

        Method for Function object
        """
        from greenplumpython.table import Table

        from_clause = f"FROM {self.table.name}" if self.table is not None else ""
        parents = [self.table] if self.table is not None else []
        return Table(f"SELECT {str(self)} {from_clause}", parents=parents, db=self._db)


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
        table: Optional[Table] = None
        if isinstance(left, Expr) and left.table is not None:
            table = left.table
        if isinstance(right, Expr) and right.table is not None:
            table = right.table
        super().__init__(as_name=as_name, table=table, db=db)
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
        from greenplumpython.type import to_pg_const

        left_str = str(self.left) if isinstance(self.left, Expr) else to_pg_const(self.left)
        right_str = str(self.right) if isinstance(self.right, Expr) else to_pg_const(self.right)
        return f"({left_str} {self.operator} {right_str})"


class UnaryExpr(Expr):
    """
    Inherited from :class:`Expr`.

    Representation of a Unary Expression.
    """

    def __init__(
        self,
        operator: str,
        right: Expr,
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ):
        """

        Args:
            right: :class:`Expr`
        """
        table = right.table
        super().__init__(as_name=as_name, table=table, db=db)
        self.operator = operator
        self.right = right

    def serialize(self) -> str:
        right_str = str(self.right)
        return f"{self.operator}({right_str})"


class ColumnField(Expr):
    def __init__(
        self,
        column: "Column",
        field_name: str,
        as_name: Optional[str] = None,
        table: Optional["Table"] = None,
        db: Optional[Database] = None,
    ) -> None:
        self._field_name = field_name
        self._column = column
        self._table = column.table
        super().__init__(as_name, table, db)

    @property
    def column(self) -> "Column":
        return self._column

    def serialize(self) -> str:
        return f"({self.column.serialize()}).{self._field_name}"


class Column(Expr):
    """
    Inherited from :class:`Expr`.

    Representation of a Python object :class:`.Column`.
    """

    def __init__(self, name: str, table: "Table", as_name: Optional[str] = None) -> None:
        super().__init__(as_name=as_name, table=table)
        self._name = name
        self._type: Optional[Type] = None  # TODO: Add type inference

    def serialize(self) -> str:
        assert self.table is not None
        return self.table.name + "." + self.name

    @property
    def name(self) -> str:
        """
        Returns :class:`Column` name

        Returns:
            str: column name
        """
        return self._name

    @property
    def table(self) -> Optional["Table"]:
        """
        Returns :class:`Column` associated :class:`~table.Table`

        Returns:
            Optional[Table]: :class:`~table.Table` associated with :class:`Column`
        """
        return self._table

    def __getitem__(self, field_name: str) -> ColumnField:
        return ColumnField(self, field_name=field_name)

    def expand(self) -> ColumnField:
        return ColumnField(self, "*")


class ConstExpr(Expr):
    """
    Inherited from :class:`Expr`.

    Representation of a constant object :class:`.Const`.
    """

    def __init__(self, val: str, as_name: Optional[str] = None) -> None:
        super().__init__(as_name=as_name)
        self._val = val

    def serialize(self) -> str:
        from greenplumpython.type import to_pg_const

        return to_pg_const(self._val)
