import random
import string
from typing import Iterable, NamedTuple
import sqlalchemy


class Database:
    def __init__(self, host: str = "localhost", db_name: str = "") -> None:
        url = f"postgresql+psycopg2://{host}/{db_name}"
        engine = sqlalchemy.create_engine(url)
        self.conn = engine.connect()
        self.name = db_name

    def execute(self, query: str) -> Iterable:
        return self.conn.execute(query)


def database(host: str = "localhost", db_name: str = "") -> Database:
    return Database(host=host, db_name=db_name)


class Node:
    def __init__(self) -> None:
        pass


class Table(Node):
    def __init__(
        self,
        query: str,
        parents: Iterable["Table"] = [],
        table_name: str = None,
        db: Database = None,
    ) -> None:
        self.query = query
        self.parents = parents
        if table_name is not None:
            self.name = table_name
        else:
            self.name = "tb_" + "".join(
                random.choice(string.ascii_lowercase) for _ in range(60)
            )
        if any(parents):
            self.db = next(iter(parents)).db
        else:
            self.db = db

    def __getitem__(self, key) -> "Table":
        """
        Returns
        - a Column of the current Table if key is string, or
        - a new Table from the current Table per the type of key:
            - if key is a list, then SELECT a subset of columns, a.k.a. targets;
            - if key is an Expr, then SELECT a subset of rows per the value of the Expr;
            - if key is a slice, then SELECT a portion of consecutive rows
        """
        if isinstance(key, str):
            return Column(key, self)
        if isinstance(key, list):
            return Table(
                f"SELECT {','.join([str(target) for target in key])} FROM {self.name}",
                parents=[self],
            )
        if isinstance(key, Expr):
            return Table(f"SELECT * FROM {self.name} WHERE {str(key)}", parents=[self])
        if isinstance(key, slice):
            if key.step is not None:
                raise NotImplementedError()
            offset_clause = "" if key.start is None else f"OFFSET {key.start}"
            return Table(
                f"SELECT * FROM {self.name} LIMIT {key.stop} {offset_clause}",
                parents=[self],
            )

    def list_lineage(self) -> Iterable["Table"]:
        lineage = [self]
        current = 0
        while current < len(lineage):
            for table in lineage[current].parents:
                lineage.append(table)
            current += 1
        return lineage

    def build_full_query(self) -> str:
        if not any(self.parents):
            return self.query
        lineage = self.list_lineage()
        return (
            "WITH "
            + ",".join([f"{tb.name} AS ({tb.query})" for tb in reversed(lineage[1:])])
            + self.query
        )

    def fetch(self) -> Iterable[NamedTuple]:
        return self.db.execute(self.build_full_query())

    def save_as(self, table_name: str) -> "Table":
        raise NotImplementedError()


def table(table_name: str, db: Database) -> Table:
    return Table(f"TABLE {table_name}", table_name=table_name, db=db)


class Expr(Node):
    def __init__(
        self, text: str, parents: Iterable["Node"] = [], as_name: str = None
    ) -> None:
        self.text = text
        self.parents = parents
        self.as_name = as_name
        self.db: Database = next(iter(parents)).db

    def __str__(self) -> str:
        raise NotImplementedError()

    def as_table(self) -> Table:
        raise NotImplementedError()

    def fetch(self) -> Iterable[NamedTuple]:
        return self.as_table().fetch()

    def save_as(self, table_name: str) -> Table:
        return self.as_table.save_as(table_name)


class Column(Expr):
    def __init__(self, name: str, table: Table, as_name: str = None) -> None:
        super().__init__(name, parents=[table], as_name=as_name)
        self.table = table
        self.name = name
        self.db = table.db


if __name__ == "__main__":
    db = database()
    t = table("t", db)
    t = t[["i"]]
    print("Full query: ", t.build_full_query())
    print("Result:", t.fetch().all())
