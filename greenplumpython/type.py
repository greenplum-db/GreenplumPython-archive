import typing

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
    if hasattr(annotation, "__origin__"):
        print("Origin =", annotation.__origin__)
        if annotation.__origin__ == typing.List:
            return f"{to_pg_type(annotation.__args__[0])}[]"
        raise NotImplementedError()
    else:
        if annotation in primitive_type_map:
            return primitive_type_map[annotation]
        raise NotImplementedError()  # TODO: Support composite types
