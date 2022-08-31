"""
This module creates a Python object Func which able creation and calling of Greenplum UDF and UDA.
"""
import functools
import inspect
import re
import textwrap
from typing import Any, Callable, Iterable, Optional, Union
from uuid import uuid4

from greenplumpython.db import Database
from greenplumpython.expr import Expr
from greenplumpython.group import TableRowGroup
from greenplumpython.table import Table
from greenplumpython.type import primitive_type_map, to_pg_const, to_pg_type


class FunctionExpr(Expr):
    """
    Inherited from :class:`~expr.Expr`.

    A Function object associated with a Greenplum function which can be called and applied to
    Greenplum data.
    """

    def __init__(
        self,
        func_name: str,
        args: Iterable[Any] = [],
        group_by: Optional[TableRowGroup] = None,
        as_name: Optional[str] = None,
        table: Optional[Table] = None,
        db: Optional[Database] = None,
        is_return_comp: bool = False,
    ) -> None:
        if table is None and group_by is not None:
            table = group_by.table
        for arg in args:
            if isinstance(arg, Expr) and arg.table is not None:
                if table is None:
                    table = arg.table
                elif table.name != arg.table.name:
                    raise Exception("Cannot pass arguments from more than one tables")
        super().__init__(as_name, table=table, db=db)
        self._func_name = func_name
        self._args = args
        self._group_by = group_by
        self._is_return_comp = is_return_comp

    def __call__(self, group_by: Optional[TableRowGroup] = None, table: Optional[Table] = None):
        return FunctionExpr(
            self._func_name,
            self._args,
            group_by=group_by,
            as_name=self._as_name,
            table=table,
            db=self._db,
            is_return_comp=self.is_return_comp,  # type: ignore
        )

    def serialize(self) -> str:
        args_string = (
            ",".join(
                [str(arg) if isinstance(arg, Expr) else to_pg_const(arg) for arg in self._args]
            )
            if any(self._args)
            else ""
        )
        return f"{self._func_name}({args_string})"

    def to_table(self) -> Table:
        """
        Returns the result table of the self function applied on args, with potential Group By if
        it is an Aggregation function.
        """
        from_caluse = f"FROM {self.table.name}" if self.table is not None else ""
        group_by_clause = (
            self._group_by.make_group_by_clause() if self._group_by is not None else ""
        )
        parents = [self.table] if self.table is not None else []
        if self._is_return_comp and self._as_name is None:
            self._as_name = "func_" + uuid4().hex
        orig_func_table = Table(
            " ".join(
                [
                    f"SELECT {str(self)}",
                    ("," + ",".join([str(target) for target in self._group_by.get_targets()]))
                    if self._group_by is not None
                    else "",
                    from_caluse,
                    group_by_clause,
                ]
            ),
            db=self._db,
            parents=parents,
        )
        # We use 2 `Table`s here because on GPDB 6X and PostgreSQL <= 9.6, a
        # function returning records that contains more than one attributes
        # will be called multiple times if we do
        # ```sql
        # SELECT (func(a, b)).* FROM t;
        # ```
        # which might cause performance issue. To workaround we need to do
        # ```sql
        # WITH func_call AS (
        #     SELECT func(a, b) AS result FROM t
        # )
        # SELECT (result).* FROM func_call;
        # ```
        result = f"({self._as_name}).*" if self._is_return_comp else "*"
        return Table(
            " ".join(
                [
                    f"SELECT {str(result)}",
                    ("," + ",".join([str(target) for target in self._group_by.get_targets()]))
                    if self._group_by is not None
                    else "",
                    f"FROM {orig_func_table.name}",
                ]
            ),
            db=self._db,
            parents=[orig_func_table],
        )

    @property
    def qualified_func_name(self) -> str:
        """
        Returns qualified function name

        Returns:
            str: function's qualified name
        """
        return self._func_name

    @property
    def is_return_comp(self) -> bool:
        """
        Returns a boolean telling if the return type is composite or not

        Returns:
            bool: Tell if the function returns a composite type
        """
        if self._is_return_comp is not None:
            return self._is_return_comp
        return False


class ArrayFunctionExpr(FunctionExpr):
    """
    Inherited from :class:`FunctionExpr`.

    Specialized for an Array Function.
    It will array aggregate all the columns given by the user.
    """

    def serialize(self) -> str:
        args_string_list = []
        args_string = ""
        if any(self._args):
            for i in range(len(self._args)):
                if isinstance(self._args[i], Expr):
                    if (self._group_by is None) or (
                        self._group_by is not None
                        and (self._args[i].name not in self._group_by.get_targets())
                    ):
                        s = f"array_agg({str(self._args[i])})"  # type: ignore
                    else:
                        s = str(self._args[i])  # type: ignore
                else:
                    s = to_pg_const(self._args[i])  # type: ignore
                args_string_list.append(s)
            args_string = ",".join(args_string_list)
        return f"{self._func_name}({args_string})"

    def __call__(self, group_by: Optional[TableRowGroup] = None, table: Optional[Table] = None):
        return ArrayFunctionExpr(
            self._func_name,
            self._args,
            group_by=group_by,
            as_name=self._as_name,
            table=table,
            db=self._db,
            is_return_comp=self.is_return_comp,  # type: ignore
        )


def function(name: str) -> Callable[..., FunctionExpr]:
    """
    A wrap in order to call function

    Example:
            .. code-block::  Python

                generate_series = gp.function("generate_series", db)

    """

    def make_function_call(
        *args: Expr, as_name: Optional[str] = None, db: Optional[Database] = None
    ) -> FunctionExpr:
        return FunctionExpr(name, args, as_name=as_name, db=db)

    return make_function_call


def aggregate(name: str) -> Callable[..., FunctionExpr]:
    """
    A wrap in order to call an aggregate function

    Example:
            .. code-block::  Python

                count = gp.aggregate("count", db=db)

    """

    def make_function_call(
        *args: Expr,
        group_by: Optional[TableRowGroup] = None,
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ) -> FunctionExpr:
        return FunctionExpr(name, args, group_by=group_by, as_name=as_name, db=db)

    return make_function_call


# FIXME: Add test cases for optional parameters
def create_function(
    func: Optional[Callable] = None,  # type: ignore
    name: Optional[str] = None,
    schema: Optional[str] = None,
    temp: bool = True,
    replace_if_exists: bool = False,
    language_handler: str = "plpython3u",
    return_type_as_name: Optional[str] = None,
    type_is_temp: bool = True,
) -> Callable:  # type: ignore
    """
    Creates a User Defined Python Function (UDF) in Greenplum Database according to the Python
    function code given.

    Args:
        func : python function
        name : Optional[str] : name of the UDF
        schema : Optional[str] : where the UDF will be stored
        temp: bool : if the creation of the UDF is temporary exists only for current session
        replace_if_exists : bool : if the creation replaces an existing UDF with same name and args
        language_handler : str : language handler extension to create UDF in Greenplum, by default plpython3u, but can also choose plcontainer.
        return_type_as_name : Optional[str] : name of the return composite type
        type_is_temp : bool : if the return composite type is temporary

    Returns:
        Callable : FunctionCall

    Example:
            .. code-block::  Python

                    @gp.create_function
                    def multiply(a: int, b: int) -> int:
                        return a * b

                    results = multiply(series["a"], series["b"], as_name="result").to_table().fetch()

    """
    # If user needs extra parameters when creating a function
    if not func:
        return functools.partial(
            create_function,  # type: ignore
            name=name,
            schema=schema,
            temp=temp,
            replace_if_exists=replace_if_exists,
            language_handler=language_handler,
            return_type_as_name=return_type_as_name,
            type_is_temp=type_is_temp,
        )

    @functools.wraps(func)  # type: ignore
    def make_function_call(
        *args: Any,
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ) -> FunctionExpr:
        """
        Function wrap

        Args:
            *args : Any : arguments of function, could be constant or columns
            as_name : Optional[str] : name of the UDF
            db : Optional[:class:`~db.Database`] : where the function will be created
        """
        # -- Prepare function creation env in Greenplum
        if db is None:
            for arg in args:
                if isinstance(arg, Expr) and arg.db is not None:
                    db = arg.db
                    break
        if db is None:
            raise Exception("Database is required to create function")
        or_replace = "OR REPLACE" if replace_if_exists else ""
        schema_name = "pg_temp" if temp else (schema if schema is not None else "")
        # -- Loading Python Function information
        func_name = func.__name__ if name is None else name
        if len(func_name) > 63:  # i.e. NAMEDATALEN - 1 in PostgreSQL
            raise Exception("Function name should be no longer than 63 bytes.")
        qualified_func_name = ".".join([schema_name, func_name])
        if not temp and name is None:
            raise NotImplementedError("Name is required for a non-temp function")
        func_sig = inspect.signature(func)  # type: ignore
        func_args_string = ",".join(
            [
                f"{param.name} {to_pg_type(param.annotation, db)}"
                for param in func_sig.parameters.values()
            ]
        )
        # -- Loading Python code of Function
        # FIXME: include things in func.__closure__
        func_lines = textwrap.dedent(inspect.getsource(func)).split("\n")  # type: ignore
        func_body = "\n".join([line for line in func_lines if re.match(r"^\s", line)])
        return_type = to_pg_type(
            func_sig.return_annotation, db, return_type_as_name, type_is_temp, True
        )
        # -- Creation of UDF in Greenplum
        db.execute(
            (
                f"CREATE {or_replace} FUNCTION {qualified_func_name} ({func_args_string}) "
                f"RETURNS {return_type} "
                f"LANGUAGE {language_handler} "
                f"AS $$\n"
                f"{textwrap.dedent(func_body)} $$"
            ),
            has_results=False,
        )
        # -- Return FunctionCall object corresponding to UDF created
        is_return_comp = func_sig.return_annotation not in primitive_type_map
        return FunctionExpr(
            qualified_func_name, args=args, as_name=as_name, db=db, is_return_comp=is_return_comp
        )

    return make_function_call


# FIXME: Add test cases for optional parameters
def create_aggregate(
    trans_func: Optional[Callable] = None,  # type: ignore
    name: Optional[str] = None,
    schema: Optional[str] = None,
    temp: bool = True,
    language_handler: str = "plpython3u",
) -> Callable:  # type: ignore
    """
    Creates a User Defined Python Aggregation (UDA) in Greenplum Database according to the Python
    function code given.

    Args:
        trans_func : python function
        name : Optional[str] : name of the UDA
        schema : Optional[str] : where the UDA will be stored
        temp: bool : if the creation of the UDA is temporary exists only for current session
        language_handler : str : language handler extension to create UDA in Greenplum, by default plpython3u, but can also choose plcontainer.

    Returns:
        Callable : FunctionCall

    Example:
            .. code-block::  Python

                @gp.create_aggregate
                def my_sum(result: int, val: int) -> int:
                    if result is None:
                        return val
                    return result + val

                rows = [(1,) for _ in range(10)]
                numbers = gp.values(rows, db=db, column_names=["val"])
                results = list(my_sum(numbers["val"], as_name="result").to_table().fetch())

    """
    # If user needs extra parameters when creating a function
    if not trans_func:
        return functools.partial(
            create_aggregate,  # type: ignore
            name=name,
            schema=schema,
            temp=temp,
            language_handler=language_handler,
        )

    @functools.wraps(trans_func)  # type: ignore
    def make_function_call(
        *args: Expr,
        group_by: Optional[TableRowGroup] = None,
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ) -> FunctionExpr:
        """
        Function wrap

        Args:
            *args : Any : arguments of function, could be constant or columns
            group_by : Optional[Iterable[Union[Expr, str]]] : aggregation group by index
            as_name : Optional[str] : name of the UDA
            db : Optional[:class:`~db.Database`] : where the function will be created
        """
        trans_func_call = create_function(  # type: ignore
            trans_func, "func_" + uuid4().hex, schema, temp, False, language_handler
        )(*args, as_name=as_name, db=db)
        schema_name = "pg_temp" if temp else schema if schema is not None else ""
        # -- Loading Python Function information
        agg_name = trans_func.__name__ if name is None else name
        qualified_agg_name = ".".join([schema_name, agg_name])
        if not temp and name is None:
            raise NotImplementedError("Name is required for a non-temp function")
        sig = inspect.signature(trans_func)  # type: ignore
        param_list = iter(sig.parameters.values())
        state_param = next(param_list)
        args_string = ",".join(
            [f"{param.name} {to_pg_type(param.annotation, db)}" for param in param_list]
        )
        trans_func_call_func_name: str = trans_func_call.qualified_func_name  # type: ignore
        # -- Creation of UDA in Greenplum
        trans_func_call.db.execute(  # type: ignore
            f"""
            CREATE AGGREGATE {qualified_agg_name} ({args_string}) (
                SFUNC = {trans_func_call_func_name},
                STYPE = {to_pg_type(state_param.annotation, db)}
            )
            """,
            has_results=False,
        )
        return FunctionExpr(
            qualified_agg_name, args=args, group_by=group_by, as_name=as_name, db=db
        )

    return make_function_call


# FIXME: Add test cases for optional parameters
def create_array_function(
    func: Optional[Callable] = None,  # type: ignore
    name: Optional[str] = None,
    schema: Optional[str] = None,
    temp: bool = True,
    replace_if_exists: bool = False,
    language_handler: str = "plpython3u",
) -> Callable:  # type: ignore
    """
    Creates a User Defined Python Function (UDF) in Greenplum Database according to the Python
    function code. But it will array aggregate all args then pass them to the UDF.

    Args:
        func : python function
        name : Optional[str] : name of the UDF
        schema : Optional[str] : where the UDF will be stored
        temp: bool : if the creation of the UDF is temporary exists only for current session
        replace_if_exists : bool : if the creation replaces an existing UDF with same name and args
        language_handler : str : language handler extension to create UDF in Greenplum, by default plpython3u, but can also choose plcontainer.

    Returns:
        Callable : FunctionCall

    Example:
            .. code-block::  Python

                @gp.create_array_function
                def my_sum(val_list: List[int]) -> int:
                    return sum(val_list)

                rows = [(1, i % 2 == 0) for i in range(10)]
                numbers = gp.values(rows, db=db, column_names=["val", "is_even"])
                results = list(
                    my_sum(numbers["val"], group_by=numbers.group_by("is_even"), as_name="result")
                    .to_table()
                    .fetch()
                )
    """
    # If user needs extra parameters when creating a function
    if not func:
        return functools.partial(
            create_array_function,  # type: ignore
            name=name,
            schema=schema,
            temp=temp,
            replace_if_exists=replace_if_exists,
            language_handler=language_handler,
        )

    @functools.wraps(func)  # type: ignore
    def make_function_call(
        *args: Union[Expr, Any],
        group_by: Optional[TableRowGroup] = None,
        as_name: Optional[str] = None,
        db: Optional[Database] = None,
    ) -> ArrayFunctionExpr:
        """
        Array Function wrap

        Args:
            *args : Any : arguments of function, could be constant or columns
            group_by : Optional[Iterable[Union[Expr, str]]] : array_aggregate group by index
            as_name : Optional[str] : name of the UDF
            db : Optional[:class:`~db.Database`] : where the function will be created
        """
        array_func_call = create_function(  # type: ignore
            func, name, schema, temp, replace_if_exists, language_handler
        )(*args, as_name=as_name, db=db)
        return ArrayFunctionExpr(
            array_func_call.qualified_func_name,  # type: ignore
            args=args,
            group_by=group_by,
            as_name=as_name,
            db=db,
            is_return_comp=array_func_call.is_return_comp,  # type: ignore
        )

    return make_function_call
