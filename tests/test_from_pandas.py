import pandas as pd
import pytest
import greenplumpython as gp
from tests import db

def test_from_pandas(db: gp.Database):
    t = gp.from_dataframe(pd.DataFrame({"a": [1, 2, 3, 1], "b": [2, 2, 2, 2], "c": ["A", "B", "C", "B"]}),db)
    assert len(list(t)) == 4
    assert next(iter(t))["a"] == 1
    assert next(iter(t))["b"] == 2
    assert next(iter(t))["c"] == "A"
