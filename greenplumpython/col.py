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
    Inherited from :class:`Expr`.

    Representation of a field of a :class:`.Column` of composite type. This type
    allows to access to the fields in a dict-like manner.
    """

    def __init__(
        self,
        column: "Column",
        field_name: str,
        dataframe: Optional["DataFrame"] = None,
        db: Optional[Database] = None,
    ) -> None:
        self._field_name = field_name
        self._column = column
        self._dataframe = column.dataframe
        super().__init__(dataframe, db)

    @property
    def column(self) -> "Column":
        return self._column

    def serialize(self) -> str:
        return f'({self.column.serialize()})."{self._field_name}"'


class Column(Expr):
    """
    Inherited from :class:`Expr`.

    Representation of a Python object :class:`.Column`.
    """

    def __init__(self, name: str, dataframe: "DataFrame") -> None:
        super().__init__(dataframe=dataframe)
        self._name = name
        self._type: Optional[Type] = None  # TODO: Add type inference

    def serialize(self) -> str:
        assert self.dataframe is not None
        # Quote both dataframe name and column name to avoid SQL injection.
        return (
            f'"{self.dataframe.name}"."{self.name}"'
            if self.name != "*"
            else f'"{self.dataframe.name}".*'
        )

    @property
    def name(self) -> str:
        """
        Returns :class:`Column` name

        Returns:
            str: column name
        """
        return self._name

    @property
    def dataframe(self) -> Optional["DataFrame"]:
        """
        Returns :class:`Column` associated :class:`~dataframe.DataFrame`

        Returns:
            Optional[DataFrame]: :class:`~dataframe.DataFrame` associated with :class:`Column`
        """
        return self._dataframe

    def __getitem__(self, field_name: str) -> ColumnField:
        """
        Returns :class:`ColumnField` of self by matching field_name

        Args:
            field_name: str

        Returns:
            ColumnField

        """
        return ColumnField(self, field_name=field_name)
