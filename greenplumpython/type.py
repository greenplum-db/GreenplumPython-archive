# noqa: D100
from typing import Any, Dict, List, Optional, Set, Tuple, get_type_hints
from uuid import uuid4

from greenplumpython.db import Database
from greenplumpython.expr import Expr, _serialize


class TypeCast(Expr):
    """
    An expression of type casting.

    Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [("01-01-1990",), ("05-01-98",)]
                >>> df = db.create_dataframe(rows=rows, column_names=["date_str"])
                >>> date_type = gp.type_("date")
                >>> result = df.assign(date=lambda t: date_type(t["date_str"]))
                >>> result
                -------------------------
                 date_str   | date
                ------------+------------
                 01-01-1990 | 1990-01-01
                 05-01-98   | 1998-05-01
                -------------------------
                (2 rows)
    """

    def __init__(
        self,
        obj: object,
        type_name: str,
        schema: Optional[str] = None,
    ) -> None:
        # noqa: D205 D400
        """
        Args:
            obj: object : which will be applied type casting
            type_name : str : name of type which object will be cast
        """
        dataframe = obj._dataframe if isinstance(obj, Expr) else None
        super().__init__(dataframe)
        self._obj = obj
        self._type_name = type_name
        self._schema = schema

    def _serialize(self) -> str:
        obj_str = _serialize(self._obj)
        schema, type_name = self._qualified_name
        qualified_name = f'"{schema}"."{type_name}"' if schema is not None else f'"{type_name}"'
        return f"({obj_str}::{qualified_name})"

    @property
    def _qualified_name(self) -> Tuple[Optional[str], str]:
        """
        Return the schema name and name of :class:`~type.TypeCast`.

        Returns:
            Tuple[str, str]: schema name and :class:`~type.TypeCast`'s name.
        """
        return self._schema, self._type_name


class Type:
    """
    Represents a type of values in a :class:`~dataframe.DataFrame`.

    It is mapped to a type in database, including

        - A predefined type, such as :code:`integer` for :class:`int` and\
            :code:`text` for :class:`str` in Python. In this case,\
            `name` is specified for its name in database.
        - A user-defined composite type, for Python :code:`class`. In this\
            case, a type annotation object is provided such as the defined\
            :code:`class`.

    A :class:`~type.Type` object is callable. when called, it casts the object in
    the argument to the mapped type in database.
    """

    def __init__(
        self, name: str, annotation: Optional[type] = None, schema: Optional[str] = None
    ) -> None:
        # noqa: D107
        self._name = name
        self._annotation = annotation
        self._created_in_dbs: Optional[Set[Database]] = set() if annotation is not None else None
        self._schema = schema

    # -- Creation of a composite type in Greenplum corresponding to the class_type given
    def _create_in_db(self, db: Database):
        # noqa: D400
        """
        :meta private:

        Create a new composite type in database and returns its name.

        Args:
            class_type : object : class which user want to reproduce in Greenplum
            db : :class:`~db.Database` : where the type will be created

        Returns:
            str: name of the created composite type

        """
        if self._created_in_dbs is None or db in self._created_in_dbs:
            return
        schema = "pg_temp"
        members = get_type_hints(self._annotation)
        if len(members) == 0:
            raise Exception(f"Failed to get annotations for type {self._annotation}")
        att_type_str = ",\n".join(
            [f"{name} {to_pg_type(type_t, db)}" for name, type_t in members.items()]
        )
        db._execute(
            f'CREATE TYPE "{schema}"."{self._name}" AS (\n' f"{att_type_str}\n" f");",
            has_results=False,
        )
        self._created_in_dbs.add(db)

    def __call__(self, obj: Any) -> TypeCast:
        """
        Cast the argument :code:`obj` to the corresponding type in database.

        Args:
            obj: the object to be cast. It can be one of the following

                - Any adaptable Python object, or
                - A :class:`Column` of a :class:`DataFrame`, or
                - Any :class:`Expr` consisting of adaptable Python objects and
                    :class:`Column`s of a :class:`DataFrame`.
        """
        return TypeCast(obj, self._name, self._schema)

    @property
    def _qualified_name(self) -> Tuple[Optional[str], str]:
        """
        Return the schema name and name of :class:`~type.Type`.

        Returns:
            Tuple[str, str]: schema name and :class:`~type.Type`'s name.
        """
        return self._schema, self._name


# -- Map between Python and Greenplum primitive types
_defined_types: Dict[Optional[type], Type] = {
    None: Type(name="void"),
    int: Type(name="int4"),
    float: Type(name="float8"),
    bool: Type(name="bool"),
    str: Type(name="text"),
    bytes: Type(name="bytea"),
}


def type_(name: str, schema: Optional[str] = None) -> Type:
    """
    Get access to a type predefined in database.

    Args:
        name: str: name of type
        schema: Optional[str]: name of schema

    Returns:
        The predefined type as a :class:`~type.Type` object.
    """
    return Type(name, schema=schema)


def to_pg_type(
    annotation: Optional[type],
    db: Optional[Database] = None,
    for_return: bool = False,
) -> str:
    # noqa: D400
    """
    :meta private:

    Convert a Python type annotation to a SQL type.

    Args:
        annotation: type annotation in Python
        schema: schema name
        db: database that the type will be created in if not present.
        for_return: if the type is used as a function's return type

    Returns:
        str: name of type in SQL
    """
    if annotation is not None and hasattr(annotation, "__origin__"):
        # The `or` here is to make the function work on Python 3.6.
        # Python 3.6 is the default Python version on CentOS 7 and Ubuntu 18.04
        if annotation.__origin__ == list or annotation.__origin__ == List:
            args: Tuple[type, ...] = annotation.__args__
            if for_return:
                return f"SETOF {to_pg_type(args[0], db)}"  # type: ignore
            if args[0] in _defined_types:
                return f"{to_pg_type(args[0], db)}[]"  # type: ignore
        raise NotImplementedError()
    else:
        assert db is not None, "Database is required to create type"
        if annotation not in _defined_types:
            type_name = "type_" + uuid4().hex
            _defined_types[annotation] = Type(name=type_name, annotation=annotation)
        _defined_types[annotation]._create_in_db(db)
        schema, type_name = _defined_types[annotation]._qualified_name
        type_qualified_name = (
            f'"{schema}"."{type_name}"' if schema is not None else f'"{type_name}"'
        )
        return type_qualified_name
