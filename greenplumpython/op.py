"""
Indexes are essential for fast data searching.

With indexes, we can retrieve rows we want by scanning the index, rather than
the entire dataframe. As a result, when the dataframe is large, the amount of
data to be scanned is typically much smaller with an index.

In pandas, a :code:`DataFrame` or :code:`Series` can have only one index. This
means that we can do index scan based on one column. While for other columns,
we will have to go through all rows. This can be rather inefficient.

Backed by database systems, GreenplumPython overcomes this limitation by
allowing the creation of

- multiple indexes, each for one set of columns, and
- multiple types of indexes for same set of columns, each for one order.

In this way, we can search a GreenplumPython's dataframe with index scan, on
more than one column set in more than one order.

For example, a dataframe containing AI-generated embeddings may contain

- a column of IDs and
- a column of vectors.

We can search for embeddings by either IDs or vectors using index scan after
creating an index on the ID column and another index on the vector column.

As another example, suppose we want to search for Approximate Nearest Neighbors
for a given vector based on not only cosine similarity, but also L_2
distance. We can create two indexes on the vector column, each for one
similarity metric.

How to search a dataframe with index is defined by a set of operators on the
indexed columns. For example, when scanning a B-tree index, relational
operators, such as :code:`>`, :code:`<`, and :code:`=`, are required for
comparing two values. These operators are encapsulated as an `operator class
<https://www.postgresql.org/docs/current/indexes-opclass.html>`_.
Different data types have different operator classes for an index. For example,
integers and floats are compared in different ways. Even for the same data type,
we can change how two values are compared by changing the operator class.

Since indexes depend on operators to work, to use index scan, we need to specify
the filering predicate in the :code:`[]` operator and :meth:`~DataFrame.where()`
with operators when doing comparison or computing similarity. To ease the use of
operators in database,

- for Python's built-in operators, we map it to the database operators of the
  same name, and
- for others that do not have a built-in equivalence, we can use the
  :func:`operator()` function to map a database operator to a Python
  :class:`Callable`. Calling the Python function will apply the operator.
"""
from typing import Any, Optional, Union

from greenplumpython.expr import BinaryExpr, UnaryExpr


class Operator:
    """
    Represents an operator in database.

    As a Python object, an :class:`Operator` can be called like a function.
    This is because unlike SQL, Python does not support defining new operators.

    When an :class:`Operator` is called, the corresponding operator in database
    will be applied.
    """

    def __init__(self, name: str, schema: Optional[str] = None) -> None:
        # noqa: D400
        """:meta private:"""
        self._name = name
        self._schema = schema

    @property
    def _qualified_name(self) -> str:
        if self._schema is not None:
            return f'OPERATOR("{self._schema}".{self._name})'
        else:
            return f"OPERATOR({self._name})"

    def __call__(self, *operands: Any) -> Union[UnaryExpr, BinaryExpr]:
        """
        Call the :class:`Operator` like a function to apply it.

        Args:
            operands: operands to apply the operator on.

        Returns:
            - a :class:`UnaryExpr` if the operator is unary, or
            - a :class:`BinaryExpr` if the operator is binary.
        """
        if len(operands) == 1:
            return UnaryExpr(self._qualified_name, operands[0])
        if len(operands) == 2:
            return BinaryExpr(self._qualified_name, operands[0], operands[1])
        else:
            raise Exception("Too many operands.")


def operator(name: str, schema: Optional[str] = None) -> Operator:
    """
    Get access to a predefined :class:`Operator` in database.

    Args:
        name: Name of the operator.
        schema: Schema (a.k.a. namespace) of the operator in database.

    Returns
        An :class:`Operator` that is :class:`Callable`. When the
        :class:`Operator` is called, the corresponding database operator will
        be applied.

    """
    return Operator(name, schema)
