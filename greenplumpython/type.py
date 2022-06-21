import re
import typing

import greenplumpython as gp

primitive_type_map = {
    None: "void",
    int: "integer",
    float: "double precision",
    bool: "boolean",
    str: "text",
    bytes: "bytea",
}
# One for db
global composite_type_map
composite_type_map = {}
global inverse_composite_type_map
inverse_composite_type_map = {}


# TODO : Add tests for all function
def create_type(obj, type_name, db):
    att_type_str = ",\n\t\t\t\t".join(
        [
            f'{re.sub(r"^_+|_+$", "", name)} {to_pg_type(type(getattr(obj, name)))}'
            for name in obj.__dict__
        ]
    )
    db.execute(
        f"""
            DROP TYPE IF EXISTS {type_name} CASCADE;
            CREATE TYPE {type_name} AS (
                {att_type_str}
            )
        """,
        has_results=False,
    )
    composite_type_map[type(obj)] = type_name
    inverse_composite_type_map[type_name] = type(obj)


# TODO : Not working because of composite_type_map not reserve to db
def drop_type(type_name, db):
    db.execute(
        f"DROP TYPE IF EXISTS {type_name} CASCADE",
        has_results=False,
    )
    composite_type_map.pop(inverse_composite_type_map[type_name])
    inverse_composite_type_map.pop(type_name)


def type_exists(obj_type):
    print(inverse_composite_type_map)
    return type(obj_type) in inverse_composite_type_map or type(obj_type) in primitive_type_map


# FIXME: Annotate the argument type for this function
def to_pg_type(annotation) -> str:
    if hasattr(annotation, "__origin__"):
        # The `or` here is to make the function work on Python 3.6.
        # Python 3.6 is the default Python version on CentOS 7 and Ubuntu 18.04
        if annotation.__origin__ == list or annotation.__origin__ == typing.List:
            return f"{to_pg_type(annotation.__args__[0])}[]"
        raise NotImplementedError()
    else:
        if annotation in primitive_type_map:
            return primitive_type_map[annotation]
        if annotation in composite_type_map:
            return composite_type_map[annotation]
        raise NotImplementedError()  # TODO: Support composite types
