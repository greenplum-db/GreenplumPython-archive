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
        self, func_name: str, db: Database, args: Iterable[Expr] = [], as_name: Optional[str] = None
    ) -> None:
        super().__init__(as_name)
        self._func_name = func_name
        self._args = args
        self._db = db

    def __str__(self) -> str:
        args_string = ",".join([str(arg) for arg in self._args]) if any(self._args) else ""
        return f"{self._func_name}({args_string})"

    def to_table(self) -> Table:
        as_string = f"AS {self._as_name}" if self._as_name is not None else ""
        ret_table = Table(f"SELECT * FROM {str(self)} {as_string}", db=self._db)
        return Table(f"SELECT * FROM {ret_table.name}", parents=[ret_table], db=self._db)


def function(name: str, db: Database) -> Callable[..., FunctionCall]:
    def make_function_call(*args: Expr, as_name: Optional[str] = None) -> FunctionCall:
        return FunctionCall(name, db, args, as_name=as_name)

    return make_function_call


def create_function(
    db: Database,
    name: Optional[str] = None,
    schema: Optional[str] = None,
    temp: bool = True,
    replace_if_exists: bool = False,
    language_handler: str = "plpython3u",
) -> Callable[[Callable], Callable]:
    def func_decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def make_function_call(*args: Expr, as_name: Optional[str] = None) -> FunctionCall:
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
            db.execute(
                textwrap.dedent(
                    f"""
                    CREATE {or_replace} FUNCTION {qualified_func_name} ({func_args_string})
                    RETURNS {primitive_type_map[func_sig.return_annotation]}
                    LANGUAGE {language_handler}
                    AS $$ 
                    {textwrap.dedent(func_body)} 
                    $$
                    """
                ),
                has_results=False,
            )
            return FunctionCall(qualified_func_name, db, args, as_name=as_name)

        return make_function_call

    return func_decorator
