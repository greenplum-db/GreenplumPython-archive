from typing import Dict, Iterable, List, Optional, Union, Any, Tuple, Literal

import greenplumpython.dataframe as gp
import greenplumpython.db as db


class DataFrame:
    def __init__(
        self,
        data: Union[List[Union[Tuple[Any, ...], Dict[str, Any]]], Dict[str, List[Any]]],
        columns: Optional[List[str]] = None,
        con: Optional[db.Database] = None,
    ) -> None:
        if isinstance(data, gp.DataFrame):
            self._proxy = data
        else:
            assert con is not None
            if isinstance(data, Dict):
                self._proxy = con.create_dataframe(columns=data)
            elif isinstance(data, List):
                self._proxy = con.create_dataframe(rows=data, column_names=columns)
            else:
                raise NotImplementedError

    def to_sql(self, name: str, con: db.Database, schema:Optional[str] = None):
        pass

    def read_sql(self, sql: str, con: db.Database):
        pass

    def sort_values(self, by: Union[str, List[str]], ascending: bool):
        pass

    def drop_duplicates(self, subset: Union[str, List[str]], keep: Literal["first", "last", False] = "first"):
        pass

    def merge(self,
        right: "DataFrame",
        how: str = "inner",
        on: Optional[Union[str, Iterable[str]]] = None,
        left_on: Optional[Union[str, Iterable[str]]] = None,
        right_on: Optional[Union[str, Iterable[str]]] = None,
        sort: bool = False,
    ):
        pass

    def groupby(self, by: Union[str, List[str]], sort: bool = False):
        pass

    def head(self, n: int):
        pass

    def read_csv(
        self,
        filepath: str,
        sep: str,
        delimiter: str,
        names: List[str],
    ):
        pass
