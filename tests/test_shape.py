import pandas as pd
import pytest

import greenplumpython as gp
from tests import db


@pytest.fixture
def table(db: gp.Database):

    rows1 = [("a", 0, "b",),
             ("c", 60, "d",),
             ("e", 13, "f",)]
    return gp.to_table(rows1, db=db, column_names=["categorical", "numeric", "text"])

def test_shape(db: gp.Database, table: gp.Table):
    assert table.shape() == (3,3)


def test_shape_slice(db: gp.Database, table: gp.Table):
    assert table[:1].shape() == (1,3)

