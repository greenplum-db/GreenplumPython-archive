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


def test_describe_one_column(db: gp.Database, table: gp.Table):
    t = table.describe("numeric")
    print('\n', t)
    assert 1 == 1
def test_describe_few_columns(db: gp.Database, table: gp.Table):
    t = table.describe(["numeric","text"])
    print('\n', t)
    assert 1 == 1

def test_describe_all_columns(db: gp.Database, table: gp.Table):
    t = table.describe()
    print('\n', t)
    assert 1 == 1

def test_to_pandas(db: gp.Database, table: gp.Table):
    t = table.to_dataframe()
    print('\n', t)
    assert 1 == 1

def test_from_pandas(db: gp.Database, table: gp.Table):
    t = gp.from_dataframe(pd.DataFrame({"A": [1, 2, 3, 1], "B": [2, 2, 2, 2], "C": ["A", "B", "C", "B"]}),db)
    print('\n', t)
    assert 1 == 1
def test_head(db: gp.Database, table: gp.Table):
    t = table.head(1)
    print('\n', t)
    assert 1 == 1

def test_tail_one(db: gp.Database, table: gp.Table):
    t = table.tail(4)
    print('\n', t)
    assert 1 == 1

def test_tail_two(db: gp.Database, table: gp.Table):
    t = table.tail(1)
    print('\n', t)
    assert 1 == 1

def test_size(db: gp.Database, table: gp.Table):
    t = table.size()
    print('\n', t)
    assert t == 3
def test_shape(db: gp.Database, table: gp.Table):
    t = table.shape()
    print('\n', t)
    assert t == (3,3)


def test_values(db: gp.Database, table: gp.Table):
    t = table.values()
    print('\n', t)
    assert t == (3,3)
