"""Utilties to access a column and one field of a column if the column is composite."""
from typing import TYPE_CHECKING, Optional

from greenplumpython.db import Database
from greenplumpython.expr import Expr
from greenplumpython.type import DataType

if TYPE_CHECKING:
    from greenplumpython.dataframe import DataFrame


class ColumnField(Expr):
    """
    Inherited from :class:`~expr.Expr`.

    Representation of a field of a :class:`~col.Column` of composite type. This
    type allows to access to the fields in a dict-like manner.
    """

    def __init__(
        self,
        column: "Column",
        field_name: str,
    ) -> None:
        # noqa
        """:meta private:"""
        self._field_name = field_name
        self._column = column
        super().__init__(column._dataframe)

    def _serialize(self, db: Optional[Database] = None) -> str:
        return (
            f'({self._column._serialize(db=db)})."{self._field_name}"'
            if self._field_name != "*"
            else f"({self._column._serialize(db=db)}).*"
        )


class Column(Expr):
    """
    Inherited from :class:`~expr.Expr`.

    Representation of a Python object :class:`~col.Column`.
    """

    def __init__(self, name: str, dataframe: "DataFrame") -> None:
        # noqa: D400
        """:meta private:"""
        super().__init__(dataframe=dataframe)
        self._name = name
        self._type: Optional[DataType] = None  # TODO: Add type inference

    def _serialize(self, db: Optional[Database] = None) -> str:
        assert self._dataframe is not None
        # Quote both dataframe name and column name to avoid SQL injection.
        return (
            f'{self._dataframe._name}."{self._name}"'
            if self._name != "*"
            else f"{self._dataframe._name}.*"
        )

    def __getitem__(self, field_name: str) -> ColumnField:
        """
        Get access to a field of the current column.

        Args:
            field_name: str

        Returns:
            Field of the column with the specified name.
        """
        return ColumnField(self, field_name=field_name)

    def _bind(
        self,
        dataframe: Optional["DataFrame"] = None,
        db: Optional[Database] = None,
    ):
        # noqa D400
        """:meta private:"""
        c = Column(
            self._name,
            self._dataframe,
        )
        c._db = db if db is not None else dataframe._db if dataframe is not None else self._db
        assert c._db is not None
        return c
