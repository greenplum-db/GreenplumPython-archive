"""
This module creates a Python object Row for table iteration.
"""
import collections
import json
from typing import Any, List

from psycopg2.extras import RealDictRow


class Row:
    """
    Represents a row of :class:`~table.Table`.
    """

    def __init__(self, contents: RealDictRow):

        def detect_duplicate_keys(json_pairs):
            key_count = collections.Counter(k for k, v in json_pairs)
            duplicate_keys = ", ".join(k for k, v in key_count.items() if v > 1)

            if len(duplicate_keys) > 0:
                raise Exception("Duplicate key(s) found: {}".format(duplicate_keys))

        def validate_data(json_pairs):
            detect_duplicate_keys(json_pairs)
            return dict(json_pairs)

        if "to_json" in contents:
            self._contents = json.loads(contents["to_json"], object_pairs_hook=validate_data)
        else:
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
