from typing import List, Optional, Set, Tuple, get_type_hints
from uuid import uuid4

from greenplumpython.db import Database
from greenplumpython.expr import Expr, serialize


class TypeCast(Expr):
    """
    Inherited from :class:`~expr.Expr`.

    Representation of a Type Casting.

    Example:
            .. highlight:: python
            .. code-block::  Python

                >>> rows = [(i,) for i in range(10)]
                >>> series = db.create_dataframe(rows=rows, column_names=["val"]).save_as("series", column_names=["val"])
                >>> regclass = gp.get_type("regclass")
                >>> dataframe_name = series.assign(dataframe_name=lambda t: regclass(t["tableoid"]))
                ----------------------
                 val | dataframe_name
                -----+----------------
                   0 | serie
                   1 | serie
                   2 | serie
                   3 | serie
                   4 | serie
                   5 | serie
                   6 | serie
                   7 | serie
                   8 | serie
                   9 | serie
                ----------------------
                (10 rows)
    """

    def __init__(
        self,
        obj: object,
        type_name: str,
        db: Optional[Database] = None,
    ) -> None:
        """

        Args:
            obj: object : which will be applied type casting
            type_name : str : name of type which object will be cast
        """
        dataframe = obj.dataframe if isinstance(obj, Expr) else None
        super().__init__(dataframe, db=db)
        self._obj = obj
        self._type_name = type_name

    def serialize(self) -> str:
        obj_str = serialize(self._obj)
        return f"({obj_str}::{self._type_name})"


class Type:
    """
    A Type object in Greenplum database.
    """

    def __init__(self, name: str, annotation: Optional[type] = None) -> None:
        self._name = name
        self._annotation = annotation
        self._created_in_dbs: Optional[Set[Database]] = set() if annotation is not None else None

    # -- Creation of a composite type in Greenplum corresponding to the class_type given
    def create_in_db(self, db: Database):
        """
        :meta private:

        Creates a new composite type in database and returns its name

        Args:
            class_type : object : class which user want to reproduce in Greenplum
            db : :class:`~db.Database` : where the type will be created

        Returns:
            str: name of the created composite type

        """
        if self._created_in_dbs is None or db in self._created_in_dbs:
            return
        schema = "pg_temp"
        att_type_str = ",".join(
            [
                f"{name} {to_pg_type(type_t, db)}"
                for name, type_t in get_type_hints(self._annotation).items()
            ]
        )
        db.execute(
            f"""
                CREATE TYPE {schema}.{self._name} AS (
                    {att_type_str}
                )
            """,
            has_results=False,
        )
        self._created_in_dbs.add(db)

    def __call__(self, obj: object) -> TypeCast:
        return TypeCast(obj, self._name)

    @property
    def name(self) -> str:
        return self._name


# -- Map between Python and Greenplum primitive types
_defined_types: dict[Optional[type], Type] = {
    None: Type(name="void"),
    int: Type(name="integer"),
    float: Type(name="double precision"),
    bool: Type(name="boolean"),
    str: Type(name="text"),
    bytes: Type(name="bytea"),
}


def get_type(name: str) -> Type:
    """
    Returns the type corresponding to the name in the :class:`~db.Database` given.

    Args:
        name: str: name of type
        db: :class:`~db.Database`: database where stored type

    Returns:
        Type: :class:`Type`
    """

    return Type(name)


def to_pg_type(
    annotation: Optional[type],
    db: Optional[Database] = None,
    for_return: bool = False,
) -> str:
    """
    :meta private:

    Conversion of Type from Python to Greenplum

    Args:
        annotation: type annotation in Python
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
        _defined_types[annotation].create_in_db(db)
        return _defined_types[annotation].name
