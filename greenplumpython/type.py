from typing import GenericMeta, List

primitive_type_map = {
    None: "void",
    int: "integer",
    float: "double precision",
    bool: "boolean",
    str: "text",
    bytes: "bytea",
}


# FIXME: Annotate the argument type for this function
def to_pg_type(annotation) -> str:
    if not isinstance(annotation, GenericMeta):
        if annotation in primitive_type_map:
            return primitive_type_map[annotation]
        return NotImplementedError()  # TODO: Support composite types
    else:
        if annotation.__origin__ == List:
            return f"{to_pg_type(annotation.__args__[0])}[]"
        return NotImplementedError()
