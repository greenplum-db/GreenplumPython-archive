import re
import typing
from typing import get_type_hints

import greenplumpython as gp

primitive_type_map = {
    None: "void",
    int: "integer",
    float: "double precision",
    bool: "boolean",
    str: "text",
    bytes: "bytea",
}


# TODO : Add tests for all function
def create_type(class_type, type_name, db):
    att_type_str = ",\n\t\t\t\t".join(
        [
            f"""
            {re.sub(r"^_+|_+$", "", name)} {to_pg_type(type_t, db) }
            """
            for name, type_t in get_type_hints(class_type).items()
        ]
    )
    db.execute(
        f"""
            CREATE TYPE {type_name} AS (
                {att_type_str}
            )
        """,
        has_results=False,
    )
    db.add_udt(type_name)


def drop_type(type_name, db):
    db.execute(
        f"DROP TYPE IF EXISTS {type_name} CASCADE",
        has_results=False,
    )
    db.remove_udt(type_name)


def type_exists(class_type, db):
    return class_type.__name__ in db.get_udt_list()


# FIXME: Annotate the argument type for this function
def to_pg_type(annotation, db) -> str:
    if hasattr(annotation, "__origin__"):
        # The `or` here is to make the function work on Python 3.6.
        # Python 3.6 is the default Python version on CentOS 7 and Ubuntu 18.04
        if annotation.__origin__ == list or annotation.__origin__ == typing.List:
            return f"{to_pg_type(annotation.__args__[0], db)}[]"
        raise NotImplementedError()
    else:
        if annotation in primitive_type_map:
            return primitive_type_map[annotation]
        else:
            if annotation.__name__ in db.get_udt_list():
                return annotation.__name__
        raise NotImplementedError()  # TODO: Support composite types
