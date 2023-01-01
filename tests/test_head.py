import pytest

import greenplumpython as gp
from tests import db


@pytest.fixture
def table(db: gp.Database):

    rows1 = [("a", 0, "b",),
             ("c", 60, "d",),
             ("e", 13, "f",)]
    return gp.to_table(rows1, db=db, column_names=["categorical", "numeric", "text"])

def test_head(db: gp.Database, table: gp.Table):
    t = table.head(1)
    assert len(list(t)) == 1
    assert next(iter(t))['categorical'] == "a"
    assert next(iter(t))['numeric'] == 0
    assert next(iter(t))['text'] == "b"

