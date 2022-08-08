"""
This module creates a Python object Expr.
"""
import copy
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from .db import Database
from .type import to_pg_const

if TYPE_CHECKING:
    from .table import Table


class Expr:
    """
    Representation of Expr. It can be

        - a Column
        - an Operator
        - a Function
    """

    def __init__(
        self,
        as_name: Optional[str] = None,
        table: Optional["Table"] = None,
        db: Optional[Database] = None,
    ) -> None:
        self._as_name = as_name
        self._table = table
        self._db = table.db if table is not None else db
        assert self._db is not None

    def __and__(self, other: "Expr") -> "Expr":
        """
        Operator &
        Returns a Binary Expression AND between self and another Expr
        """
        return BinaryExpr("AND", self, other)

    def __or__(self, other: "Expr") -> "Expr":
        """
        Operator |
        Returns a Binary Expression OR between self and another Expr
        """
        return BinaryExpr("OR", self, other)

    def __eq__(self, other: "Expr") -> "Expr":
        """
        Operator ==
        Returns a Binary Expression EQUAL between self and another Expr
        """
        if isinstance(other, type(None)):
            return BinaryExpr("is", self, other)
        return BinaryExpr("=", self, other)

    def __lt__(self, other: "Expr") -> "Expr":
        """
        Operator <
        Returns a Binary Expression LESS THAN between self and another Expr
        """
        return BinaryExpr("<", self, other)

    def __le__(self, other: "Expr") -> "Expr":
        """
        Operator <=
        Returns a Binary Expression LESS EQUAL between self and another Expr
        """
        return BinaryExpr("<=", self, other)

    def __gt__(self, other: "Expr") -> "Expr":
        """
        Operator >
        Returns a Binary Expression GREATER THAN between self and another Expr
        """
        return BinaryExpr(">", self, other)

    def __ge__(self, other: "Expr") -> "Expr":
        """
        Operator >=
        Returns a Binary Expression GREATER EQUAL between self and another Expr
        """
        return BinaryExpr(">=", self, other)

    def __ne__(self, other: "Expr") -> "Expr":
        """
        Operator !=
        Returns a Binary Expression NOT EQUAL between self and another Expr
        """
        return BinaryExpr("!=", self, other)

    def __mod__(self, other: Union[int, "Expr"]) -> "Expr":
        """
        Operator %
        Returns a Binary Expression Modulo between an Expr and an integer or an Expr
        """
        return BinaryExpr("%", self, other)

    def __pos__(self) -> "Expr":
        """
        Operator +
        Returns a Unary Expression POSITIVE of self
        """
        return UnaryExpr("+", self)

    def __neg__(self) -> "Expr":
        """
        Operator -
        Returns a Unary Expression NEGATIVE of self
        """
        return UnaryExpr("-", self)

    def __abs__(self) -> "Expr":
        """
        Operator abs()
        Returns a Unary Expression ABSOLUTE of self
        """
        return UnaryExpr("ABS", self)

    def __invert__(self) -> "Expr":
        """
        Operator ~
        Returns a Unary Expression NOT of self
        """
        return UnaryExpr("NOT", self)

    def like(self, pattern: str) -> "Expr":
        """
        Returns BinaryExpr in order to apply LIKE statement on self with pattern
        """
        return BinaryExpr("LIKE", self, pattern)

    def __str__(self) -> str:
        """
        Returns string statement of Expr, ie : name + AS (optional)
        """
        return self._serialize() + (" AS " + self._as_name if self._as_name is not None else "")

    def rename(self, new_name: str) -> "Expr":
        """
        Return copy of Expr with a new name
        """
        new_expr = copy.copy(self)  # Shallow copy
        new_expr._as_name = new_name
        return new_expr

    def _serialize(self) -> str:
        raise NotImplementedError()

    @property
    def name(self) -> str:
        """
        Returns Expr name
        """
        raise NotImplementedError()

    @property
    def as_name(self) -> str:
        """
        Returns Expr Alias name
        """
        return self._as_name

    @property
    def db(self) -> Optional[Database]:
        """
        Returns Expr associated database
        """
        return self._db

    @property
    def table(self) -> Optional["Table"]:
        """
        Returns Expr associated table
        """
        return self._table

    def to_table(self) -> "Table":
        """
        Returns a Table, method for Function object
        """
        from .table import Table

        from_clause = f"FROM {self.table.name}" if self.table is not None else ""
        parents = [self.table] if self.table is not None else []
        return Table(f"SELECT {str(self)} {from_clause}", parents=parents, db=self._db)


class BinaryExpr(Expr):
    """
    Inherited from Expr. Representation of a Binary Expression
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
            left: Any : could be an Expr object or object in primitive types (int, str, etc)
            right: Any : could be an Expr object or object in primitive types (int, str, etc)
        """
        self._init(operator, left, right, as_name, db)

    def _serialize(self) -> str:
        left_str = str(self.left) if isinstance(self.left, Expr) else to_pg_const(self.left)
        right_str = str(self.right) if isinstance(self.right, Expr) else to_pg_const(self.right)
        return f"({left_str} {self.operator} {right_str})"


class UnaryExpr(Expr):
    """
    Inherited from Expr. Representation of a Unary Expression.
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
            right: Expr
        """
        table = right.table
        super().__init__(as_name=as_name, table=table, db=db)
        self.operator = operator
        self.right = right

    def _serialize(self) -> str:
        right_str = str(self.right)
        return f"{self.operator}({right_str})"


class TypeCast(Expr):
    """
    Inherited from Expr. Representation of a Type Casting.
    """

    def __init__(
        self,
        obj: object,
        type_name: str,
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ) -> None:
        """

        Args:
            obj: object : which will be applied type casting
            type_name : str : name of type which object will be cast
        """
        table = obj.table if isinstance(obj, Expr) else None
        super().__init__(as_name, table, db)
        self._obj = obj
        self._type_name = type_name

    def _serialize(self) -> str:
        obj_str = self._obj._serialize() if isinstance(self._obj, Expr) else to_pg_const(self._obj)
        return f"{obj_str}::{self._type_name}"


class Type:
    """
    A Type object in Greenplum Database.
    """

    def __init__(self, name: str, db: Database) -> None:
        self._name = name
        self._db = db

    def __call__(self, obj: object) -> TypeCast:
        return TypeCast(obj, self._name, db=self._db)


# FIXME: Rename gp.table(), gp.function(), etc. to get_table(), get_function(), etc.
# FIXME: Make these functions methods of a Database,
#  e.g. from gp.get_type("int", db) to db.get_type("int")
def get_type(name: str, db: Database) -> Type:
    """
    Returns the type corresponding to the name in the database given.
    """

    return Type(name, db=db)


class Column(Expr):
    """
    Inherited from Expr. Representation of a python object Column.
    """

    def __init__(self, name: str, table: "Table", as_name: Optional[str] = None) -> None:
        super().__init__(as_name=as_name, table=table)
        self._name = name
        self._type: Optional[Type] = None  # TODO: Add type inference

    def _serialize(self) -> str:
        assert self.table is not None
        return self.table.name + "." + self.name

    @property
    def name(self) -> str:
        """
        Returns column name
        """
        return self._name

    @property
    def table(self) -> Optional["Table"]:
        """
        Returns column associated table
        """
        return self._table
