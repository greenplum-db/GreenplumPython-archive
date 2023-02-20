"""
This module manage a Column and sub column
"""
from typing import TYPE_CHECKING, Optional

from greenplumpython.db import Database
from greenplumpython.expr import Expr
from greenplumpython.type import Type

if TYPE_CHECKING:
    from greenplumpython.dataframe import DataFrame


class ColumnField(Expr):
    """
    Inherited from :class:`~expr.Expr`.

    Representation of a field of a :class:`~col.Column` of composite type. This type
    allows to access to the fields in a dict-like manner.
    """

    def __init__(
        self,
        column: "Column",
        field_name: str,
    ) -> None:
        self._field_name = field_name
        self._column = column
        super().__init__(column._dataframe)

    def _serialize(self) -> str:
        return f'({self._column._serialize()})."{self._field_name}"'


class Column(Expr):
    """
    Inherited from :class:`~expr.Expr`.

    Representation of a Python object :class:`~col.Column`.
    """

    def __init__(self, name: str, dataframe: "DataFrame") -> None:
        super().__init__(dataframe=dataframe)
        self._name = name
        self._type: Optional[Type] = None  # TODO: Add type inference

    def _serialize(self) -> str:
        assert self._dataframe is not None
        # Quote both dataframe name and column name to avoid SQL injection.
        schema, df_name = self._dataframe._qualified_name
        df_qualified_name = f'"{schema}"."{df_name}"' if schema is not None else f'"{df_name}"'
        return (
            f'{df_qualified_name}."{self._name}"' if self._name != "*" else f"{df_qualified_name}.*"
        )

    def __getitem__(self, field_name: str) -> ColumnField:
        """
        Used when want to use Field of Column for computation.
        Returns :class:`~col.ColumnField` of self by matching field_name

        Args:
            field_name: str

        Returns:
            ColumnField

        """
        return ColumnField(self, field_name=field_name)
