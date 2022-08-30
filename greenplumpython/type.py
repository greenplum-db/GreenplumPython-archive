from typing import Any, List, Optional, Union, get_type_hints
from uuid import uuid4

from psycopg2.extensions import adapt  # type: ignore

from greenplumpython.db import Database
from greenplumpython.expr import Expr

# -- Map between Python and Greenplum primitive types
primitive_type_map = {
    None: "void",
    int: "integer",
    float: "double precision",
    bool: "boolean",
    str: "text",
    bytes: "bytea",
}


class TypeCast(Expr):
    """
    Inherited from :class:`~expr.Expr`.

    Representation of a Type Casting.

    Example:
            .. code-block::  Python

                rows = [(i,) for i in range(10)]
                series = gp.values(rows, db, column_names=["val"]).save_as("series")
                regclass = gp.get_type("regclass", db)
                table_name = regclass(series["tableoid"]).rename("table_name")
    """

    def __init__(
        self,
        obj: object,
        type_name: str,
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ) -> None:
        """

        Args:
            obj: object : which will be applied type casting
            type_name : str : name of type which object will be cast
        """
        table = obj.table if isinstance(obj, Expr) else None
        super().__init__(as_name, table, db)
        self._obj = obj
        self._type_name = type_name

    def serialize(self) -> str:
        obj_str = self._obj.serialize() if isinstance(self._obj, Expr) else to_pg_const(self._obj)
        return f"{obj_str}::{self._type_name}"


class Type:
    """
    A Type object in Greenplum database.
    """

    def __init__(self, name: str, db: Database) -> None:
        self._name = name
        self._db = db

    def __call__(self, obj: object) -> TypeCast:
        return TypeCast(obj, self._name, db=self._db)


# FIXME: Rename gp.table(), gp.function(), etc. to get_table(), get_function(), etc.
# FIXME: Make these functions methods of a Database,
#  e.g. from gp.get_type("int", db) to db.get_type("int")
def get_type(name: str, db: Database) -> Type:
    """
    Returns the type corresponding to the name in the :class:`~db.Database` given.

    Args:
        name: str: name of type
        db: :class:`~db.Database`: database where stored type

    Returns:
        Type: type object
    """

    return Type(name, db=db)


# -- Creation of a composite type in Greenplum corresponding to the class_type given
# TODO : Add tests for all function
def create_type(
    class_type: object,
    db: Database,
    as_name: Optional[str] = None,
    is_temp: bool = True,
) -> str:
    """
    Creates a new composite type in database and returns its name

    Args:
        class_type : object : class which user want to reproduce in Greenplum
        db : :class:`~db.Database` : where the type will be created
        as_name : Optional[str] : name of the created type if different from class
        is_temp : bool : if type exists only for current session

    Returns:
        str: name of the created composite type

    """
    type_name = "type_" + uuid4().hex if as_name is None else as_name
    temp_str = "pg_temp." if is_temp else ""
    att_type_str = ",\n\t\t\t\t".join(
        [
            f"""
            {name} {to_pg_type(type_t, db)}
            """
            for name, type_t in get_type_hints(class_type).items()
        ]
    )
    db.execute(
        f"""
            CREATE TYPE {temp_str}{type_name} AS (
                {att_type_str}
            )
        """,
        has_results=False,
    )
    return type_name


def drop_type(type_name: str, db: Database):
    """
    Drop type in :class:`~db.Database`

    Args:
        type_name: str: type name
        db: :class:`~db.Database`: database where stored type
    """
    db.execute(
        f"DROP TYPE IF EXISTS {type_name} CASCADE",
        has_results=False,
    )


# FIXME: Annotate the argument type for this function
def to_pg_type(
    annotation: Any,
    db: Optional[Database] = None,
    as_name: Optional[str] = None,
    is_temp: bool = True,
    is_return: bool = False,
) -> Union[str, None]:
    """
    Conversion of Type from Python to Greenplum

    Args:
        annotation: Any: object annotation
        db: Optional[:class:`~db.Database`]: None if primitive type or database associated with type
        as_name: Optional[str]: None or its alias name
        is_temp: bool: define if it is a temporary creation
        is_return: bool: define if the object is use as a function's return type

    Returns:
        Union[str, None]: name of type or None if not exists
    """
    if hasattr(annotation, "__origin__"):
        # The `or` here is to make the function work on Python 3.6.
        # Python 3.6 is the default Python version on CentOS 7 and Ubuntu 18.04
        if annotation.__origin__ == list or annotation.__origin__ == List:
            if annotation.__args__[0] in primitive_type_map:
                return f"{to_pg_type(annotation.__args__[0], db)}[]"
            if is_return:
                return f"SETOF {to_pg_type(annotation.__args__[0], db)}"
        raise NotImplementedError()
    else:
        if annotation in primitive_type_map:
            return primitive_type_map[annotation]
        else:
            if db is not None:
                return create_type(annotation, db, as_name=as_name, is_temp=is_temp)


def to_pg_const(obj: object) -> str:
    """
    Converts a const to UTF-8 encoded str
    """
    # In Python 3, all `str`s are encoded in UTF-8
    from greenplumpython.expr import Expr

    if isinstance(obj, Expr):
        return str(obj)
    return adapt(obj).getquoted().decode("utf-8")  # type: ignore
