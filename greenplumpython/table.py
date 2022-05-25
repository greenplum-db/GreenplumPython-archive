from .database import Database

# table_name can be table/view name
def table(table_name: str, db: Database) -> Table:
    return Table(f"TABLE {table_name}", table_name=table_name, db=db)


class Table:
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
            # FIXME: short UUID?
            self.name = "tb_" + "".join(random.choice(string.ascii_lowercase) for _ in range(60))
        if any(parents):
            self.db = next(iter(parents)).db
        else:
            self.db = db

    def __getitem__(self, key):
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
            return self.select(key)
        if isinstance(key, Expr):
            return self.filter(key)
        if isinstance(key, slice):
            raise NotImplementedError()

    def filter(expr):
        return Table(f"SELECT * FROM {self.name} WHERE {str(key)}", parents=[self])

    def select(self, expr_list: list):
        return Table(
            f"SELECT {','.join([str(target) for target in key])} FROM {self.name}", parents=[self]
        )

    def describe(self):
        raise NotImplementedError()

    def name(self):
        raise NotImplementedError()

    # If all = false, use cursor to fetch
    def fetch(all: bool = true):
        raise NotImplementedError()
