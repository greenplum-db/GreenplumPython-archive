from typing import TYPE_CHECKING, Iterable, Optional

if TYPE_CHECKING:
    from .db import Database
    from .table import Table


class Expr:
    def __init__(self, as_name: Optional[str] = None) -> None:
        self._as_name = as_name

    def __eq__(self, other):
        return BinaryExpr("=", self, other)

    def __str__(self) -> str:
        raise NotImplementedError()

    # Attribute
    def name(self):
        raise NotImplementedError()


class BinaryExpr:
    def __init__(self, operator: str, left: Expr, right: Expr):
        raise NotImplementedError()

    def __str__(self) -> str:
        raise NotImplementedError()


class Column(Expr):
    def __init__(self, name: str, table: "Table", as_name: Optional[str] = None) -> None:
        super().__init__(name, parents=[table], as_name=as_name)
        self._table = table
        self._name = name

    def __str__(self) -> str:
        return self.table.name + "." + self.name()

    def name(self) -> str:
        return self._name
