import pytest

import greenplumpython as gp
from tests import db


@pytest.fixture
def table(db: gp.Database):
    # fmt: off
    db.execute("""CREATE TABLE IF NOT EXISTS Zebi (
        categorical     text    null,
        numeric         float   null,
        object          text    null
    )""", has_results=False)
    rows1 = [("zebi", 0, "nami",), ("9lawia", 60, "termtek",), ("97ab", 13, "7tchoun",)]
    # fmt: on
    return gp.table("Zebi", db=db)


def test_describe_all_columns(db: gp.Database, table: gp.Table):
    table.describe()
