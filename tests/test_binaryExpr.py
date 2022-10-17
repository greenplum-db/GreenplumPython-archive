from os import environ

import pytest

import greenplumpython as gp
from greenplumpython.builtin.function import generate_series
from tests import db


def test_expr_bin_equal_int(db: gp.Database):
    rows = [(1,), (2,), (3,), (2,)]
    t = gp.values(rows, db=db).save_as("temp1", temp=True, column_names=["id"])
    b1 = t["id"] == 2
    assert str(b1) == str(gp.expr.BinaryExpr("=", t["id"], 2))
    assert str(b1) == "(temp1.id = 2)"
    assert t[b1].ndim == 2


def test_expr_bin_equal_str(db: gp.Database):
    rows = [("aaa",), ("bbb",), ("ccc",)]
    t = gp.values(rows, db=db).save_as("temp2", temp=True, column_names=["id"])
    b2 = t["id"] == "aaa"
    assert str(b2) == str(gp.expr.BinaryExpr("=", t["id"], "aaa"))
    assert str(b2) == "(temp2.id = 'aaa')"
    assert t[b2].ndim == 1


def test_expr_bin_equal_none(db: gp.Database):
    rows = [("aa",), (None,), ("cc",)]
    t = gp.values(rows, db=db).save_as("temp3", temp=True, column_names=["id"])
    b3 = t["id"] == None
    assert str(b3) == str(gp.expr.BinaryExpr("is", t["id"], None))
    assert str(b3) == "(temp3.id is NULL)"
    assert t[b3].ndim == 1


def test_expr_bin_equal_2expr(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t1 = gp.values(rows, db=db).save_as("temp4", temp=True, column_names=["id"])
    t2 = gp.values(rows, db=db).save_as("temp5", temp=True, column_names=["id"])
    b4 = t1["id"] == t2["id"]
    assert str(b4) == str(gp.expr.BinaryExpr("=", t1["id"], t2["id"]))
    assert str(b4) == "(temp4.id = temp5.id)"
    assert t1.join(t2, using=["id"]).ndim == 3


def test_expr_bin_equal_bool(db: gp.Database):
    rows = [(True,), (False,), (False,), (True,)]
    t = gp.values(rows, db=db).save_as("temp1", temp=True, column_names=["id"])
    b5 = t["id"] == True
    assert str(b5) == "(temp1.id = true)"
    assert t[b5].ndim == 2


@pytest.fixture
def table_num(db: gp.Database):
    t = db.call(generate_series, 0, 9).rename("id").to_table()
    return t


def test_expr_bin_lt(db: gp.Database, table_num: gp.Table):
    b1 = table_num["id"] < 3
    assert table_num[b1].ndim == 3


def test_expr_bin_le(db: gp.Database, table_num: gp.Table):
    b1 = table_num["id"] <= 3
    assert table_num[b1].ndim == 4


def test_expr_bin_gt(db: gp.Database, table_num: gp.Table):
    b1 = table_num["id"] > 3
    assert table_num[b1].ndim == 6


def test_expr_bin_ge(db: gp.Database, table_num: gp.Table):
    b1 = table_num["id"] >= 3
    assert table_num[b1].ndim == 7


def test_expr_bin_ne(db: gp.Database, table_num: gp.Table):
    b1 = table_num["id"] != 3
    assert table_num[b1].ndim == 9


def test_expr_bin_and(db: gp.Database, table_num: gp.Table):
    b = (table_num["id"] >= 3) & (table_num["id"] < 8)
    for row in table_num[b]:
        assert 3 <= row["id"] < 8
    assert table_num[b].ndim == 5


def test_expr_bin_or(db: gp.Database):
    rows = [(1,), (2,), (3,), (-2,)]
    t = gp.values(rows, db=db, column_names=["id"])
    b = (t["id"] >= 3) | (t["id"] < 0)
    for row in t[b]:
        assert 3 <= row["id"] or row["id"] < 0
    assert t[b].ndim == 2


def test_table_like(db: gp.Database):
    rows = [("aaa",), ("bba",), ("acac",)]
    t = gp.values(rows, db=db, column_names=["id"])
    assert t[t["id"].like(r"a%")].ndim == 2
    assert t[t["id"].like(r"%a")].ndim == 2
    assert t[t["id"].like(r"%a%")].ndim == 3
    assert t[t["id"].like(r"a%c")].ndim == 1
    assert t[t["id"].like(r"_a%")].ndim == 1


def test_table_add(db: gp.Database):
    nums = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    results = nums.extend("add", nums["num"] + 1)

    for row in results:
        assert row["num"] + 1 == row["add"]


def test_table_sub(db: gp.Database):
    nums = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    results = nums.extend("sub", nums["num"] - 1)

    for row in results:
        assert row["num"] - 1 == row["sub"]


def test_table_mul(db: gp.Database):
    nums = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    results = nums.extend("mul", nums["num"] * nums["num"])

    for row in results:
        assert row["num"] ** 2 == row["mul"]


def test_table_true_div(db: gp.Database):
    nums = gp.values([(i,) for i in range(1, 10)], db, column_names=["num"])
    results = nums.extend("div", nums["num"] / nums["num"])

    for row in results:
        assert row["div"] == 1


def test_table_true_div_integers(db: gp.Database):
    nums = gp.values([(i,) for i in range(4, 5)], db, column_names=["num"])
    results = nums.extend("div", nums["num"] / 2)

    for row in results:
        assert row["div"] == 2


def test_table_true_div_integer_float(db: gp.Database):
    nums = gp.values([(i,) for i in range(5, 8, 2)], db, column_names=["num"])
    float_type = gp.get_type("float", db)
    results = nums.extend("div", float_type(nums["num"]) / 2)

    for row in results:
        assert row["div"] == 2.5 or row["div"] == 3.5


def test_table_true_div_zero(db: gp.Database):
    nums = gp.values([(i,) for i in range(5)], db, column_names=["num"])
    with pytest.raises(Exception) as exc_info:
        nums.extend("div", nums["num"] / nums["num"])._fetch()

    assert "division by zero\n" == str(exc_info.value)
