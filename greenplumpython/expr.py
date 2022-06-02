from typing import TYPE_CHECKING, Iterable, Optional

from .db import Database

if TYPE_CHECKING:
    from .table import Table


class Expr:
    def __init__(self, as_name: Optional[str] = None, db: Optional[Database] = None) -> None:
        self._as_name = as_name
        self._db = db

    def __eq__(self, other):
        return BinaryExpr("=", self, other)

    def __str__(self) -> str:
        raise NotImplementedError()

    @property
    def name():
        raise NotImplementedError()

    @property
    def db():
        return self._db


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
        raise NotImplementedError()

    def name():
        raise NotImplementedError()
