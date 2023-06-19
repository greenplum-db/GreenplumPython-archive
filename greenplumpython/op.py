"""
Indexing is essential for fast data searching in database.

In pandas, each DataFrame or Series object can have only one index.

Unlike pandas, a database allows for the creation of multiple indexes on columns or sets of columns within a table.
These indexes are separate data structures created to optimize data retrieval and query performance.

Moreover, a database also allows you to create multiple indexes with different access method on the same column or set
of columns. Each index serves as a separate data structure that facilitates efficient data retrieval for different query
scenarios.

However, in most traditional databases, indexes cannot be used directly with functions. Therefore, GreenplumPython
allows you to retrieve database operators with the :class:`~op.Operator` object, because indexes are used in conjunction
with operators in the database.

"""
from typing import Any, Optional, Union

from greenplumpython.expr import BinaryExpr, UnaryExpr


class Operator:
    def __init__(self, name: str, schema: Optional[str] = None) -> None:
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


def operator(name: str, schema: Optional[str] = None) -> Operator:
    """
    Get access to a predefined dataframe :class:`Operator` from database.

    Args:
        name: Name of the operator.
        schema: Schema (a.k.a namespace) of the operator in database.

    Returns
        The :class:`Operator` with the specified :code:`name`
        and :code:`schema`.

    """
    return Operator(name, schema)
