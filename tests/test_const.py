from os import environ

import pytest

import greenplumpython as gp
from tests import db


def test_expr_const(db: gp.Database):
    generate_series = gp.function("generate_series")
    results = generate_series(0, 9, as_name="id", db=db).to_table()
    ret = results[
        [
            results["id"],
            gp.rename("a", "label"),
            gp.rename(1, "type"),
        ]
    ].fetch()
    assert list(list(ret)[0].keys()) == ["id", "label", "type"]
    assert sorted([row["id"] for row in ret]) == list(range(10))
    assert [row["label"] for row in ret] == ["a"] * 10
    assert [row["type"] for row in ret] == [1] * 10
