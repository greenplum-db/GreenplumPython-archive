"""
This module creates a Python object Expr.
"""
import copy
from typing import Optional

from .db import Database
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

    def __and__(self, other):
        """
        Operator &
        Returns a Binary Expression AND
        """
        return BinaryExpr("AND", self, other)

    def __or__(self, other):
        """
        Operator |
        Returns a Binary Expression OR
        """
        return BinaryExpr("OR", self, other)

    def __eq__(self, other):
        """
        Operator ==
        Returns a Binary Expression EQUAL
        """
        if isinstance(other, type(None)):
            return BinaryExpr("is", self, other)
        return BinaryExpr("=", self, other)

    def __lt__(self, other):
        """
        Operator <
        Returns a Binary Expression LESS THAN
        """
        return BinaryExpr("<", self, other)

    def __le__(self, other):
        """
        Operator <=
        Returns a Binary Expression LESS EQUAL
        """
        return BinaryExpr("<=", self, other)

    def __gt__(self, other):
        """
        Operator >
        Returns a Binary Expression GREATER THAN
        """
        return BinaryExpr(">", self, other)

    def __ge__(self, other):
        """
        Operator >=
        Returns a Binary Expression GREATER EQUAL
        """
        return BinaryExpr(">=", self, other)

    def __ne__(self, other):
        """
        Operator !=
        Returns a Binary Expression NOT EQUAL
        """
        return BinaryExpr("!=", self, other)

    def __pos__(self):
        """
        Operator +
        Returns a Unary Expression POSITIVE
        """
        return UnaryExpr("+", self)

    def __neg__(self):
        """
        Operator -
        Returns a Unary Expression NEGATIVE
        """
        return UnaryExpr("-", self)

    def __abs__(self):
        """
        Operator abs()
        Returns a Unary Expression ABSOLUTE
        """
        return UnaryExpr("ABS", self)

    def __invert__(self):
        """
        Operator ~
        Returns a Unary Expression NOT
        """
        return UnaryExpr("NOT", self)

    def __str__(self) -> str:
        """
        Returns string statement of Expr
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
    def db(self) -> Optional[Database]:
        """
        Returns Expr associated database
        """
        return self._db

    @property
    def table(self) -> Optional[Table]:
        """
        Returns Expr associated table
        """
        return self._table

    def to_table(self) -> Table:
        """
        Returns a Table
        """
        from_clause = f"FROM {self.table.name}" if self.table is not None else ""
        parents = [self.table] if self.table is not None else []
        return Table(f"SELECT {str(self)} {from_clause}", parents=parents, db=self._db)


class BinaryExpr(Expr):
    """
    Herited from Expr. Representation of a Binary Expression
    """

    def __init__(
        self,
        operator: str,
        left,
        right,
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

    def _serialize(self) -> str:
        if isinstance(self.right, type(None)):
            return str(self.left) + " " + self.operator + " " + "NULL"
        if isinstance(self.right, str):
            if self.operator == "LIKE":
                self.right = self.right.replace("%", "%%")
            return str(self.left) + " " + self.operator + " '" + self.right + "'"
        if isinstance(self.right, bool):
            if self.right:
                return str(self.left) + " " + self.operator + " TRUE"
            else:
                return str(self.left) + " " + self.operator + " FALSE"

        return str(self.left) + " " + self.operator + " " + str(self.right)


class UnaryExpr(Expr):
    """
    Herited from Expr. Representation of an Unary Expression.
    """

    def __init__(
        self,
        operator: str,
        right: Expr,
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ):
        table = right.table if isinstance(right, Expr) else None
        super().__init__(as_name=as_name, table=table, db=db)
        if operator not in ["NOT", "ABS", "+", "-"]:
            raise NotImplementedError(
                f"{operator.upper()} is not a supported unary operator for Column\n"
                f"Can only support 'NOT', 'ABS', 'POS' and 'NEG' unary operators"
            )
        self.operator = operator
        self.right = right

    def _serialize(self) -> str:
        if self.operator == "NOT":
            return "NOT(" + str(self.right) + ")"
        if self.operator == "ABS":
            return "ABS(" + str(self.right) + ")"

        return self.operator + str(self.right)


class Column(Expr):
    """
    Herited from Expr. Representation of a python object Column.
    """

    def __init__(self, name: str, table: "Table", as_name: Optional[str] = None) -> None:
        super().__init__(as_name=as_name, table=table)
        self._name = name

    def _serialize(self) -> str:
        assert self.table is not None
        return self.table.name + "." + self.name

    @property
    def name(self) -> str:
        """
        Reurns column name
        """
        return self._name

    @property
    def table(self) -> Optional["Table"]:
        """
        Returns column associated table
        """
        return self._table

    def like(self, cond: str) -> Expr:
        """
        Returns BinaryExpr in order to deploy LIKE statement
        """
        return BinaryExpr("LIKE", self, cond)
