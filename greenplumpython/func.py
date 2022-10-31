"""
This module creates a Python object Func which able creation and calling of Greenplum UDF and UDA.
"""
import functools
import inspect
import re
import textwrap
from typing import Any, Callable, Dict, Optional, Set, Tuple
from uuid import uuid4

from greenplumpython.col import Column
from greenplumpython.db import Database
from greenplumpython.expr import Expr
from greenplumpython.group import TableGroupingSets
from greenplumpython.table import Table
from greenplumpython.type import to_pg_const, to_pg_type


class FunctionExpr(Expr):
    """
    Inherited from :class:`~expr.Expr`.

    A Function Expression object associated with a Greenplum function which can be called and applied to
    Greenplum data.
    """

    def __init__(
        self,
        func: "_AbstractFunction",
        args: Tuple[Any] = [],
        group_by: Optional[TableGroupingSets] = None,
        table: Optional[Table] = None,
        db: Optional[Database] = None,
        distinct: bool = False,
    ) -> None:
        if table is None and group_by is not None:
            table = group_by.table
        for arg in args:
            if isinstance(arg, Expr) and arg.table is not None:
                if table is None:
                    table = arg.table
                elif table.name != arg.table.name:
                    raise Exception("Cannot pass arguments from more than one tables")
        super().__init__(table=table, db=db)
        self._func = func
        self._args = args
        self._group_by = group_by
        self._distinct = distinct

    def bind(
        self,
        group_by: Optional[TableGroupingSets] = None,
        table: Optional[Table] = None,
        db: Optional[Database] = None,
    ):
        """:meta private:"""
        return FunctionExpr(
            self._func,
            self._args,
            group_by=group_by,
            table=table,
            db=db if db is not None else self._db,
            distinct=self._distinct,
        )

    def serialize(self) -> str:
        """:meta private:"""
        self.function.create_in_db(self._db)
        distinct = "DISTINCT" if self._distinct else ""
        args_string = (
            ",".join(
                [
                    str(arg) if isinstance(arg, Expr) else to_pg_const(arg)
                    for arg in self._args
                    if arg is not None
                ]
            )
            if any(self._args)
            else ""
        )
        return f"{self._func.qualified_name}({distinct} {args_string})"

    def apply(self, expand: Optional[bool] = None, as_name: Optional[str] = None) -> Table:
        """
        :meta private:
        Returns the result table of the self function applied on args, with potential GROUP BY if
        it is an Aggregation function.
        """
        self.function.create_in_db(self._db)
        from_clause = f"FROM {self.table.name}" if self.table is not None else ""
        group_by_clause = self._group_by.clause() if self._group_by is not None else ""
        if expand and as_name is None:
            as_name = "func_" + uuid4().hex
        parents = [self.table] if self.table is not None else []
        grouping_col_names = self._group_by.flatten() if self._group_by is not None else None
        # FIXME: The names of GROUP BY exprs can collide with names of fields in
        # the comosite type, making the column names of the resulting table not
        # unique. This can be mitigated after we implement table column
        # inference by raising an error when the function gets called.
        grouping_cols = (
            [Column(name, self._table).serialize() for name in grouping_col_names]
            if grouping_col_names is not None and len(grouping_col_names) != 0
            else None
        )
        orig_func_table = Table(
            " ".join(
                [
                    f"SELECT {str(self)} {'AS ' + as_name if as_name is not None else ''}",
                    ("," + ",".join(grouping_cols)) if (grouping_cols is not None) else "",
                    from_clause,
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
        rebased_grouping_cols = (
            [Column(name, orig_func_table).serialize() for name in grouping_col_names]
            if grouping_col_names is not None
            else None
        )
        results = (
            "*"
            if not expand
            else f"({as_name}).*"
            if rebased_grouping_cols is None or len(rebased_grouping_cols) == 0
            else f"({orig_func_table.name}).*"
            if not expand
            else f"{','.join(rebased_grouping_cols)}, ({as_name}).*"
        )

        return Table(
            f"SELECT {str(results)} FROM {orig_func_table.name}",
            db=self._db,
            parents=[orig_func_table],
        )

    @property
    def function(self) -> "_AbstractFunction":
        return self._func


class ArrayFunctionExpr(FunctionExpr):
    """
    Inherited from :class:`FunctionExpr`.

    Specialized for an Array Function.
    It will array aggregate all the columns given by the user.
    """

    def serialize(self) -> str:
        """:meta private:"""
        self.function.create_in_db(self._db)
        args_string_list = []
        args_string = ""
        grouping_col_names = self._group_by.flatten() if self._group_by is not None else None
        grouping_cols = (
            {Column(name, self._table) for name in grouping_col_names}
            if grouping_col_names is not None
            else None
        )
        if any(self._args):
            for i in range(len(self._args)):
                if self._args[i] is None:
                    continue
                if isinstance(self._args[i], Expr):
                    if grouping_cols is None or self._args[i] not in grouping_cols:
                        s = f"array_agg({str(self._args[i])})"  # type: ignore
                    else:
                        s = str(self._args[i])  # type: ignore
                else:
                    s = to_pg_const(self._args[i])  # type: ignore
                args_string_list.append(s)
            args_string = ",".join(args_string_list)
        return f"{self._func.qualified_name}({args_string})"

    def bind(
        self,
        group_by: Optional[TableGroupingSets] = None,
        table: Optional[Table] = None,
        db: Optional[Database] = None,
    ):
        """:meta private:"""
        return ArrayFunctionExpr(
            self._func,
            self._args,
            group_by=group_by,
            table=table,
            db=db if db is not None else self._db,
        )


# The parent class for all database functions.
# It is not a Callable by design to prevent misuse.
class _AbstractFunction:
    def __init__(
        self,
        wrapped_func: Optional[Callable[..., Any]],
        name: Optional[str],
        schema: Optional[str],
    ) -> None:
        NAMEDATALEN = 64  # See definition in PostgreSQL
        # if wrapped_func is None, the function object is obtained by
        # gp.function() rather than gp.create_function(). Otherwise a
        # Python function will be passed to wrapped_func.
        _name = wrapped_func.__name__ if wrapped_func is not None else name
        assert _name is not None
        assert (
            len(_name) < NAMEDATALEN
        ), f"Function name '{_name}' should be shorter than {NAMEDATALEN} bytes."
        qualified_name = (
            (name if schema is None else f"{schema}.{name}")
            if name is not None
            else f"pg_temp.{wrapped_func.__name__}"
            if wrapped_func is not None
            else None
        )
        assert (
            qualified_name not in _global_scope
        ), f'Function named "{qualified_name}" has been defined before.'
        self._qualified_name = qualified_name
        _global_scope[qualified_name] = self

    @property
    def qualified_name(self) -> str:
        return self._qualified_name

    def create_in_db(self, _: Database) -> None:
        """:meta private:"""
        raise NotImplementedError("Cannot create abstract function in database")


_global_scope: Dict[str, _AbstractFunction] = {}


class NormalFunction(_AbstractFunction):
    """
    Inherited from :class:`_AbstractFunction`.

    It will wrap a python function wrote by user which will be created in database.
    """

    def __init__(
        self,
        wrapped_func: Optional[Callable[..., Any]] = None,
        name: Optional[str] = None,
        schema: Optional[str] = None,
        language_handler: str = "plpython3u",
    ) -> None:
        super().__init__(wrapped_func, name, schema)
        self._created_in_dbs: Optional[Set[Database]] = set() if wrapped_func is not None else None
        self._wrapped_func = wrapped_func
        self._language_handler = language_handler

    def unwrap(self) -> Callable[..., Any]:
        """
        Get the wrapped Python function in the database function.
        """
        assert self._wrapped_func is not None, "No Python function is wrapped inside."
        return self._wrapped_func

    def create_in_db(self, db: Database) -> None:
        """:meta private:"""
        if self._wrapped_func is None:  # Function has already existed.
            return
        assert self._created_in_dbs is not None
        if db not in self._created_in_dbs:
            func_sig = inspect.signature(self._wrapped_func)
            func_args = ",".join(
                [
                    f"{param.name} {to_pg_type(param.annotation, db)}"
                    for param in func_sig.parameters.values()
                ]
            )
            # -- Loading Python code of Function
            # FIXME: include things in func.__closure__
            func_lines = textwrap.dedent(inspect.getsource(self._wrapped_func)).split("\n")
            func_body = "\n".join([line for line in func_lines if re.match(r"^\s", line)])
            return_type = to_pg_type(func_sig.return_annotation, db, for_return=True)
            assert (
                db.execute(
                    (
                        f"CREATE FUNCTION {self._qualified_name} ({func_args}) "
                        f"RETURNS {return_type} "
                        f"AS $$\n"
                        f"{textwrap.dedent(func_body)} $$"
                        f"LANGUAGE {self._language_handler};"
                    ),
                    has_results=False,
                )
                is None
            )
            self._created_in_dbs.add(db)

    def __call__(self, *args: Any, db: Optional[Database] = None) -> FunctionExpr:
        """:meta private:"""
        return FunctionExpr(self, args, db=db)


def function(name: str, schema: Optional[str] = None) -> NormalFunction:
    """
    A wrap in order to call function

    Example:
        .. code-block::  Python

            generate_series = gp.function("generate_series")

    """
    if name not in _global_scope:
        return NormalFunction(name=name, schema=schema)
    func = _global_scope[name]
    assert isinstance(func, NormalFunction), f'"{name}" is not a normal function'
    return func


class AggregateFunction(_AbstractFunction):
    """
    Inherited from :class:`_AbstractFunction`.

    It will wrap a python function wrote by user as the transition function of an aggregate function which will be created in database.
    """

    def __init__(
        self,
        transition_func: Optional[NormalFunction] = None,
        name: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> None:
        super().__init__(
            transition_func.unwrap() if transition_func is not None else None,
            name,
            schema,
        )
        self._transition_func = transition_func
        self._created_in_dbs: Optional[Set[Database]] = (
            set() if transition_func is not None else None
        )

    @property
    def transition_function(self) -> NormalFunction:
        """Return the transition function of aggregate function"""
        assert (
            self._transition_func is not None
        ), f'Transition function of the aggregate function "{self.qualified_name}" is unknown.'
        return self._transition_func

    def create_in_db(self, db: Database) -> None:
        """:meta private:"""
        # If self._transition_func is None, then the aggregate function is not
        # created with gp.create_aggregate(), but only refers to an existing
        # aggregate function.
        if self._transition_func is None:
            return
        assert self._created_in_dbs is not None
        if db not in self._created_in_dbs:
            self._transition_func.create_in_db(db)
            sig = inspect.signature(self.transition_function.unwrap())
            param_list = iter(sig.parameters.values())
            state_param = next(param_list)
            args_string = ",".join(
                [f"{param.name} {to_pg_type(param.annotation, db)}" for param in param_list]
            )
            # -- Creation of UDA in Greenplum
            db.execute(
                f"""
                CREATE AGGREGATE {self.qualified_name} ({args_string}) (
                    SFUNC = {self.transition_function.qualified_name},
                    STYPE = {to_pg_type(state_param.annotation, db)}
                )
                """,
                has_results=False,
            )
            self._created_in_dbs.add(db)

    def distinct(self, *args: Any) -> FunctionExpr:
        """
        Apply the current aggregate function to only each distinct set of the
        arguments.

        For example, `count.distinct(t['a'])` means applying the
        `count()` function to each distinct value of :class:`Column` `t['a']`.

        Args:
            args: Argument of the aggregate function.

        Returns:
            FunctionExpr: An expression represents the function call.
        """
        return FunctionExpr(self, args, distinct=True)

    def __call__(self, *args: Any) -> FunctionExpr:
        return FunctionExpr(self, args)


def aggregate_function(name: str, schema: Optional[str] = None) -> AggregateFunction:
    """
    A wrap in order to call an aggregate function

    Example:
        .. code-block::  Python

            count = gp.aggregate_function("count")
    """
    if name not in _global_scope:
        return AggregateFunction(name=name, schema=schema)
    func = _global_scope[name]
    assert isinstance(func, AggregateFunction), f'"{name}" is not an aggregate function'
    return func


# FIXME: Add test cases for optional parameters
def create_function(
    wrapped_func: Optional[Callable[..., Any]] = None, language_handler: str = "plpython3u"
) -> NormalFunction:
    """
    Creates a User Defined Function (UDF) in database from the given Python
    function.

    Args:
        wrapped_func : the Python function to be wrapped into a database function
            language_handler language handler to run the UDF, defaults to plpython3u,
            will also support plcontainer later.

    Returns:
        :class:`NormalFunction`

    Example:
        .. code-block::  Python

            @gp.create_function
            def multiply(a: int, b: int) -> int:
                return a * b

            db.assign(result=lambda: multiply(1, 2))

    """
    # If user needs extra parameters when creating a function
    if wrapped_func is None:
        return functools.partial(create_function, language_handler=language_handler)
    return NormalFunction(wrapped_func=wrapped_func, language_handler=language_handler)


# FIXME: Add test cases for optional parameters
def create_aggregate(
    transition_func: Optional[Callable[..., Any]] = None, language_handler: str = "plpython3u"
) -> AggregateFunction:
    """
    Creates a User Defined Aggregate (UDA) in Database using the given Python
    function as the state transition function.

    Args:
        transition_func : python function
        language_handler : language handler to run the aggregate function in database,
            defaults to plpython3u, will also support plcontainer later.

    Returns:
        :class:`AggregateFunction`

    Example:
        .. code-block::  Python

            @gp.create_aggregate
            def my_sum(result: int, val: int) -> int:
                if result is None:
                    return val
                return result + val

            rows = [(1,) for _ in range(10)]
            numbers = gp.to_table(rows, db=db, column_names=["val"])
            results = numbers.group_by().assign(result=lambda t: my_sum(t["val"]))

    """
    # If user needs extra parameters when creating a function
    if transition_func is None:
        return functools.partial(create_aggregate, language_handler=language_handler)
    return AggregateFunction(
        transition_func=NormalFunction(
            transition_func,
            name="func_" + uuid4().hex,
            schema="pg_temp",
            language_handler=language_handler,
        )
    )


class ArrayFunction(NormalFunction):
    def __call__(self, *args: Any) -> ArrayFunctionExpr:
        return ArrayFunctionExpr(self, args=args)


# FIXME: Add test cases for optional parameters
def create_array_function(
    wrapped_func: Optional[Callable[..., Any]] = None, language_handler: str = "plpython3u"
) -> ArrayFunction:
    """
    Creates a User Defined Array Function in database from the given Python
    function.

    Args:
        wrapped_func: python function
        language_handler: language handler to run the UDF, defaults to plpython3u,
            will also support plcontainer later.

    Returns:
        :class:`ArrayFunction`

    Example:
            .. code-block::  Python

                @gp.create_array_function
                def my_sum_array(val_list: List[int]) -> int:
                    return sum(val_list)

                rows = [(1, i % 2 == 0) for i in range(10)]
                numbers = gp.to_table(rows, db=db, column_names=["val", "is_even"])
                results = numbers.group_by("is_even").assign(result=lambda t: my_sum(t["val]))
    """
    # If user needs extra parameters when creating a function
    if wrapped_func is None:
        return functools.partial(create_array_function, language_handler=language_handler)
    return ArrayFunction(wrapped_func=wrapped_func, language_handler=language_handler)
