import functools
import inspect
import re
import textwrap
from typing import Callable, Iterable, Optional

from .db import Database
from .expr import Expr
from .table import Table
from .type import primitive_type_map


class FunctionCall(Expr):
    def __init__(
        self,
        func_name: str,
        args: Iterable[Expr] = [],
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ) -> None:
        table: Optional[Table] = None
        for arg in args:
            if isinstance(arg, Expr) and arg.table is not None:
                if table is None:
                    table = arg.table
                elif table.name != arg.table.name:
                    raise Exception("Cannot pass arguments from more than one tables")
        super().__init__(as_name, table=table, db=db)
        self._func_name = func_name
        self._args = args

    def __str__(self) -> str:
        args_string = ",".join([str(arg) for arg in self._args]) if any(self._args) else ""
        return f"{self._func_name}({args_string})"

    def to_table(self) -> Table:
        as_string = f"AS {self._as_name}" if self._as_name is not None else ""
        from_caluse = f"FROM {self.table.name}" if self.table is not None else ""
        parents = [self.table] if self.table is not None else []
        orig_func_table = Table(
            f"SELECT {str(self)} {as_string} {from_caluse}", db=self._db, parents=parents
        )
        return Table(
            f"SELECT * FROM {orig_func_table.name}", parents=[orig_func_table], db=self._db
        )


def function(name: str, db: Database) -> Callable[..., FunctionCall]:
    def make_function_call(*args: Expr, as_name: Optional[str] = None) -> FunctionCall:
        return FunctionCall(name, args, as_name=as_name, db=db)

    return make_function_call


def create_function(
    func: Callable,
    name: Optional[str] = None,
    schema: Optional[str] = None,
    temp: bool = True,
    replace_if_exists: bool = False,
    language_handler: str = "plpython3u",
) -> Callable:
    @functools.wraps(func)
    def make_function_call(
        *args: Expr, as_name: Optional[str] = None, db: Optional[Database] = None
    ) -> FunctionCall:
        or_replace = "OR REPLACE" if replace_if_exists else ""
        schema_qualifier = "pg_temp." if temp else f"{schema}." if schema is not None else ""
        func_name = func.__name__ if name is None else name
        qualified_func_name = schema_qualifier + func_name
        if not temp and name is None:
            raise NotImplementedError("Name is required for a non-temp function")
        func_sig = inspect.signature(func)
        func_args_string = ",".join(
            [
                f"{param_name} {primitive_type_map[func_sig.parameters[param_name].annotation]}"
                for param_name in func_sig.parameters
            ]
        )
        # FIXME: include things in func.__closure__
        func_lines = textwrap.dedent(inspect.getsource(func)).split("\n")
        func_body = "\n".join([line for line in func_lines if re.match(r"^\s", line)])
        if db is None:
            for arg in args:
                print(arg.db)
                if isinstance(arg, Expr) and arg.db is not None:
                    db = arg.db
                    break
        if db is None:
            raise Exception("Database is required to create function")
        db.execute(
            (
                f"CREATE {or_replace} FUNCTION {qualified_func_name} ({func_args_string}) "
                f"RETURNS {primitive_type_map[func_sig.return_annotation]} "
                f"LANGUAGE {language_handler} "
                f"AS $$\n"
                f"{textwrap.dedent(func_body)} $$"
            ),
            has_results=False,
        )
        return FunctionCall(qualified_func_name, args, as_name=as_name, db=db)

    return make_function_call
