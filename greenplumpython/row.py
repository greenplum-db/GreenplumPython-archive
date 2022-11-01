"""
This module creates a Python object :class:`Row` for table iteration.
"""
from typing import Any, Dict, List, Union


class Row:
    """
    Represents a row of :class:`~table.Table`.
    """

    def __init__(self, contents: Dict[str, Union[str, List[str]]]):
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

    def column_names(self) -> List[str]:
        """
        Return list of column names of row

        Returns:
            List[str]: list of column names

        """
        return list(self._contents.keys())

    def values(self) -> List[Any]:
        """
        Return list of values of row

        Returns:
            List[Any]

        """
        return list(self._contents.values())
