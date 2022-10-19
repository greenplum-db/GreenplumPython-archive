"""
This module creates a Python object Row for table iteration.
"""
from typing import Any, List

from psycopg2.extras import RealDictRow


class Row:
    """
    Represents a row of :class:`~table.Table`.
    """

    def __init__(self, contents: RealDictRow):
        self._contents = contents

    def __getitem__(self, name: str) -> Any:
        return self._contents[name]

    def __contains__(self, name: str) -> bool:
        return name in self._contents

    def __str__(self) -> str:
        return str(self._contents)

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
