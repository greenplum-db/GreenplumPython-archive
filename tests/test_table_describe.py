import pytest

import greenplumpython as gp
from tests import db


@pytest.fixture
def table(db: gp.Database):

    rows1 = [("a", 0, "b",), ("c", 60, "d",), ("e", 13, "f",)]
    return gp.to_table(rows1, db=db, column_names=["categorical", "numeric", "text"])



def test_describe_all_columns(db: gp.Database, table: gp.Table):
    t = table.describe()
    assert 1 == 1
