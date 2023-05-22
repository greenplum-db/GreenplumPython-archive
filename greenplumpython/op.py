from typing import Any, Union, Optional
from greenplumpython.expr import BinaryExpr, UnaryExpr


class Operator:
    def __init__(self, name: str, schema: Optional[str]) -> None:
        self._name = name
        self._schema = schema

    @property
    def qualified_name(self) -> str:
        if self._schema is not None:
            return f'OPERATOR("{self._schema}".{self._name})'
        else:
            return f"OPERATOR({self._name})"

    def __call__(self, *args: Any) -> Union[UnaryExpr, BinaryExpr]:
        if len(args) == 1:
            return UnaryExpr(self.qualified_name, args[0])
        if len(args) == 2:
            return BinaryExpr(self.qualified_name, args[0], args[1])
        else:
            raise Exception("Too many operands.")


def operator(name: str, schema: Optional[str]) -> Operator:
    return Operator(name, schema)
