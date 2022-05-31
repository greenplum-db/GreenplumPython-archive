from typing import Callable, Iterable, Optional
from uuid import uuid4

from .db import Database
from .expr import Expr
from .table import Table


class FunctionCall(Expr):
    def __init__(
        self, func_name: str, db: Database, args: Iterable[Expr] = [], as_name: Optional[str] = None
    ) -> None:
        super().__init__(as_name)
        self._func_name = func_name
        self._args = args
        self._db = db

    def __str__(self) -> str:
        args_string = ",".join([str(arg) for arg in self._args]) if self._args else ""
        return f"{self._func_name}({args_string})"

    def to_table(self) -> Table:
        as_string = f"AS {self._as_name}" if self._as_name is not None else ""
        ret_table = Table(f"SELECT * FROM {str(self)} {as_string}", db=self._db)
        return Table(f"SELECT * FROM {ret_table.name}", parents=[ret_table], db=self._db)


def function(name: str, db: Database) -> Callable[..., FunctionCall]:
    def make_callable(*args: Expr, as_name: Optional[str] = None) -> FunctionCall:
        return FunctionCall(name, db, args, as_name=as_name)

    return make_callable


def create_function():
    pass
