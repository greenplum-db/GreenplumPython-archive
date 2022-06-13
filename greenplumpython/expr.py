from typing import TYPE_CHECKING, Iterable, Optional

from .db import Database

if TYPE_CHECKING:
    from .table import Table


class Expr:
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
        return BinaryExpr("AND", self, other)

    def __or__(self, other):
        return BinaryExpr("OR", self, other)

    def __eq__(self, other):
        if isinstance(other, type(None)):
            return BinaryExpr("is", self, other)
        return BinaryExpr("=", self, other)

    def __lt__(self, other):
        return BinaryExpr("<", self, other)

    def __le__(self, other):
        return BinaryExpr("<=", self, other)

    def __gt__(self, other):
        return BinaryExpr(">", self, other)

    def __ge__(self, other):
        return BinaryExpr(">=", self, other)

    def __ne__(self, other):
        return BinaryExpr("!=", self, other)

    def __pos__(self):
        return UnaryExpr("+", self, as_name='"+' + str(self) + '"')

    def __neg__(self):
        return UnaryExpr("-", self, as_name='"-' + str(self) + '"')

    def __abs__(self):
        return UnaryExpr("ABS", self, as_name='"Abs(' + str(self) + ')"')

    def __invert__(self):
        return UnaryExpr("NOT", self, as_name='"Not(' + str(self) + ')"')

    def __str__(self) -> str:
        raise NotImplementedError()

    @property
    def name(self) -> str:
        raise NotImplementedError()

    @property
    def db(self) -> Optional[Database]:
        return self._db

    @property
    def table(self) -> Optional["Table"]:
        return self._table


class BinaryExpr(Expr):
    def __init__(self, operator: str, left: Expr, right, as_name: Optional[str] = None):
        table = left.table if left is not None and left.table is not None else right.table
        db = left.db if left is not None and left.db is not None else right.db
        super().__init__(as_name=as_name, table=table, db=db)
        self.operator = operator
        self.left = left
        self.right = right

    def __str__(self) -> str:
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
    def __init__(self, operator: str, right: Expr, as_name: Optional[str] = None):
        table = right.table
        db = right.db
        super().__init__(as_name=as_name, table=table, db=db)
        if operator not in ["NOT", "ABS", "+", "-"]:
            raise NotImplementedError(
                f"{operator.upper()} is not a supported unary operator for Column\n"
                f"Can only support 'NOT', 'ABS', 'POS' and 'NEG' unary operators"
            )
        self.operator = operator
        self.right = right

    def __str__(self) -> str:
        if self.operator == "NOT":
            return (
                "NOT("
                + str(self.right)
                + ") "
                + ("AS " + self._as_name if self._as_name is not None else "")
            )
        if self.operator == "ABS":
            return (
                "ABS("
                + str(self.right)
                + ") "
                + ("AS " + self._as_name if self._as_name is not None else "")
            )

        return (
            self.operator
            + str(self.right)
            + " "
            + ("AS " + self._as_name if self._as_name is not None else "")
        )


class Column(Expr):
    def __init__(self, name: str, table: "Table", as_name: Optional[str] = None) -> None:
        super().__init__(as_name=as_name, table=table)
        self._name = name

    def __str__(self) -> str:
        assert self.table is not None
        return self.table.name + "." + self.name

    @property
    def name(self) -> str:
        return self._name

    @property
    def table(self) -> Optional["Table"]:
        return self._table

    def like(self, cond: str) -> Expr:
        return BinaryExpr("LIKE", self, cond)
