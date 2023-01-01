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

def test_to_pandas(db: gp.Database, table: gp.Table):
    df = table.to_dataframe()
    df2 = pd.DataFrame({"categorical":["a", "c", "e"],"numeric":[0, 60, 13],"text":["b","d", "f"]})
    assert df.shape == (3, 3)
    assert df.equals(df2)

