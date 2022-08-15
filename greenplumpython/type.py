from typing import Any, List, Optional, Union, get_type_hints
from uuid import uuid4

from psycopg2.extensions import adapt  # type: ignore

from greenplumpython.db import Database

# -- Map between Python and Greenplum primitive types
primitive_type_map = {
    None: "void",
    int: "integer",
    float: "double precision",
    bool: "boolean",
    str: "text",
    bytes: "bytea",
}


# -- Creation of a composite type in Greenplum corresponding to the class_type given
# TODO : Add tests for all function
def create_type(
    class_type: object,
    db: Database,
    as_name: Optional[str] = None,
    is_temp: bool = True,
) -> str:
    """
    Returns:
        Name of created composite type

    Args:
        class_type : object : class which user want to reproduce in Greenplum
        db : Database : where the type will be created
        as_name : Optional[str] : name of the created type if different from class
        is_temp : bool : if type exists only for current session
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
    Drop type in Greenplum Database
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
    return adapt(obj).getquoted().decode("utf-8")  # type: ignore
