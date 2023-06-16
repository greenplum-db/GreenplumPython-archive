"""
Indexing is essential for fast data searching in database.

In pandas, an index is a fundamental component of a pandas DataFrame or a Series object. It provides an immutable, labeled
structure that allows for easy and efficient data alignment, indexing, and selection operations. The index in pandas is
similar to a database index in that it provides a way to access data quickly. However, the pandas index is not a
separate data structure like a database index. Instead, it is a part of the DataFrame or Series object itself.

Unlike pandas, in the context of databases, an index is a data structure that enhances the speed of data retrieval
operations on database tables. It provides a way to quickly locate rows in a table based on the values of one or more
columns.

When an index is created on a column or set of columns, it creates a separate data structure that organizes the values
in a sorted manner, allowing for efficient searching, sorting, and filtering of data. The index in a database is
typically managed by the database management system (DBMS) and is used to optimize query performance.

To summarize, while a pandas index is an integral part of a DataFrame or Series object, providing a labeled structure
for efficient data manipulation and selection, the database index is a separate data structure used to optimize data
retrieval in a database.

**************************
Combination with operator
**************************

In a database, the combination of operators with an index is used to efficiently search and retrieve data based on
specific criteria. Here are a few reasons why combining operators with an index is beneficial:

1. **Faster Data Retrieval**: When you query a database table without an index, the database engine has to scan through all
the rows sequentially to find the matching records.

2. **Query Optimization**: The database optimizer utilizes indexes to determine the most efficient way to execute a query.

3. **Selectivity Improvement**: The selectivity of a query refers to the ratio of the number of rows returned by the query
to the total number of rows in the table.

4. **Index Range Scans**: When using range operators (such as greater than, less than, between, etc.) with an index,
the database can perform efficient index range scans.

This module enable the access to database operator, so user can combine it with an index for faster data retrieval,
query optimization, selectivity improvement, and efficient range scans.
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
