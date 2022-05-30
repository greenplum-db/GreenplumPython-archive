from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from .db import Database
    from .table import Table


class Expr:
    def __init__(self, text: str, parents: Iterable["Expr"] = [], as_name: str = None) -> None:
        self.text = text
        self.parents = parents
        self.as_name = as_name
        self.db: Database = next(iter(parents)).db

    def __eq__(self, other):
        return BinaryExpr("=", self, other)

    def __str__(self) -> str:
        raise NotImplementedError()

    # Attribute
    def name(self):
        raise NotImplementedError()


class BinaryExpr:
    def __init__(self, operator: str, left: Expr, right):
        self.operator = operator
        self.left = left
        self.right = right

    def __str__(self) -> str:
        if isinstance(self.right, type(None)):
            return str(self.left) + " " + self.operator + " " + "NULL"
        if isinstance(self.right, str):
            return str(self.left) + " " + self.operator + " \"" + self.right + "\""
        if isinstance(self.right, bool):
            if self.right:
                return str(self.left) + " " + self.operator + " TRUE"
            else:
                return str(self.left) + " " + self.operator + " FALSE"

        return str(self.left) + " " + self.operator + " " + str(self.right)


class Column(Expr):
    def __init__(self, name: str, table: "Table", as_name: str = None) -> None:
        super().__init__(name, parents=[table], as_name=as_name)
        self.table = table
        self._name = name
        self.db = table.db

    def __str__(self) -> str:
        return self.table.name + "." + self.name()

    def name(self) -> str:
        return self._name
