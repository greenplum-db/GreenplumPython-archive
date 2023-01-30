"""
This module creates a Python object :class:`Row` for GreenplumPython DataFrame iteration.
"""
from typing import Any, Dict, Iterable, List, Tuple, Union


class Row:
    """
    Represents a row of :class:`~dataframe.DataFrame`.

    A :class:`Row` is conceptually an immutable :class:`dict`.
    """

    def __init__(self, contents: Dict[str, Union[Any, List[Any]]]):
        self._contents = contents

    def __getitem__(self, column_name: str) -> Any:
        """
        Get the value of the column by the specified name.
        """
        return self._contents[column_name]

    def __contains__(self, column_name: str) -> bool:
        """
        Checks whether the current row contains the specific column by name.
        """
        return column_name in self._contents

    def __str__(self) -> str:
        return str(self._contents)

    def __iter__(self):
        """
        Iterate over column names in the current row.
        """
        return iter(self._contents)

    def __len__(self):
        """
        Returns the number of columns in the current row.
        """
        return len(self._contents)

    def keys(self) -> Iterable[str]:
        """
        Return Iterable of column names of row

        Returns:
            Iterable[str]: Iterable of column names

        """
        return self._contents.keys()

    def values(self) -> Iterable[Any]:
        """
        Return Iterable of values of row

        Returns:
            Iterable[Any]

        """
        return self._contents.values()

    def items(self) -> Iterable[Tuple[str, Any]]:
        return self._contents.items()
