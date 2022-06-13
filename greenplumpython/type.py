from typing import Type

primitive_type_map = {
    None: "void",
    int: "integer",
    float: "double precision",
    bool: "boolean",
    str: "text",
    bytes: "bytea",
}


def is_primitive_type(type_: Type) -> bool:
    return type_ in primitive_type_map
