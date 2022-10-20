from os import environ

import pytest

import greenplumpython as gp
from greenplumpython.builtin.function import generate_series
from tests import db


@pytest.fixture
def t(db: gp.Database):
    t = db.apply(lambda: generate_series(0, 9)).rename("id").to_table()
    return t


def test_const_table(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t = gp.values(rows, db=db, column_names=["id"])
    t = t.save_as("const_table", column_names=["id"], temp=True)
    assert sorted([tuple(row.values()) for row in t]) == sorted(rows)

    assert len(next(iter(t)).column_names()) == 1
    for row in next(iter(t)).column_names():
        assert row == "id"


def test_table_getitem_str(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t = gp.values(rows, db=db, column_names=["id"])
    c = t["id"]
    assert str(c) == (t.name + ".id")


def test_table_getitem_sub_columns(db: gp.Database):
    # fmt: off
    rows = [(1, 2,), (1, 3,), (2, 2,), (3, 1,), (3, 4,)]
    # fmt: on
    t = gp.values(rows, db=db, column_names=["id", "num"])
    t_sub = t[["id", "num"]]
    for row in t_sub:
        assert "id" in row and "num" in row


def test_table_getitem_slice_limit(db: gp.Database, t: gp.Table):
    assert len(list(t[:2])) == 2


def test_table_getitem_slice_offset(db: gp.Database, t: gp.Table):
    assert len(list(t[7:])) == 3


def test_table_getitem_slice_off_limit(db: gp.Database, t: gp.Table):
    assert len(list(t[2:5])) == 3


def test_table_display_repr(db: gp.Database):
    # fmt: off
    rows = [(1, "Lion",), (2, "Tiger",), (3, "Wolf",), (4, "Fox")]
    # fmt: on
    t = gp.values(rows, db=db, column_names=["id", "animal"])
    expected = (
        "| id || animal |\n"
        "================\n"
        "|  1 || Lion   |\n"
        "|  2 || Tiger  |\n"
        "|  3 || Wolf   |\n"
        "|  4 || Fox    |\n"
    )
    assert str(t.order_by("id").head(4)) == expected


def test_table_display_repr_long_content(db: gp.Database):
    # fmt: off
    rows = [(1, "Lion",), (2, "Tigerrrrrrrrrrrr",), (3, "Wolf",), (4, "Fox")]
    # fmt: on
    t = gp.values(rows, db=db, column_names=["iddddddddddddddddddd", "animal"])
    expected = (
        "| iddddddddddddddddddd || animal           |\n"
        "============================================\n"
        "|                    1 || Lion             |\n"
        "|                    2 || Tigerrrrrrrrrrrr |\n"
        "|                    3 || Wolf             |\n"
        "|                    4 || Fox              |\n"
    )
    assert str(t.order_by("iddddddddddddddddddd").head(4)) == expected


def test_table_display_repr_html(db: gp.Database):
    # fmt: off
    rows = [(1, "Lion",), (2, "Tiger",), (3, "Wolf",), (4, "Fox")]
    # fmt: on
    t = gp.values(rows, db=db, column_names=["id", "animal"])
    expected = (
        "<table>\n"
        "\t<tr>\n"
        "\t\t<th>id</th>\n"
        "\t\t<th>animal</th>\n"
        "\t</tr>\n"
        "\t<tr>\n"
        "\t\t<td>1</td>\n"
        "\t\t<td>Lion</td>\n"
        "\t</tr>\n"
        "\t<tr>\n"
        "\t\t<td>2</td>\n"
        "\t\t<td>Tiger</td>\n"
        "\t</tr>\n"
        "\t<tr>\n"
        "\t\t<td>3</td>\n"
        "\t\t<td>Wolf</td>\n"
        "\t</tr>\n"
        "\t<tr>\n"
        "\t\t<td>4</td>\n"
        "\t\t<td>Fox</td>\n"
        "\t</tr>\n"
        "</table>"
    )
    assert (t.order_by("id").head(4)._repr_html_()) == expected


def test_table_display_repr_empty_result(db: gp.Database):
    # fmt: off
    rows = [(1, "Lion",), (2, "Tiger",), (3, "Wolf",), (4, "Fox")]
    # fmt: on
    t = gp.values(rows, db=db, column_names=["id", "animal"])
    assert str(t[lambda t: t["id"] == 0]) == ""
    assert (t[lambda t: t["id"] == 0]._repr_html_()) == ""


def test_table_assign_const(db: gp.Database):
    nums = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    results = nums.assign(x=lambda _: "hello")
    for row in results:
        assert "num" in row and "x" in row and row["x"] == "hello"


@gp.create_function
def add_one(num: int) -> int:
    return num + 1


def test_table_assign_expr(db: gp.Database):

    nums = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    # FIXME: How to remove the intermdeiate variable `nums`?
    # FIXME: How to support functions returning more than one column?
    results = nums.assign(result=lambda nums: add_one(nums["num"]))
    for row in results:
        assert row["result"] == row["num"] + 1


def test_table_assign_composite_type(db: gp.Database):
    class rank_label:
        val: int
        label: str

    @gp.create_function
    def my_count_sum(val: int) -> rank_label:
        return {"val": val, "label": "label"}

    nums = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    results = nums.assign(result=lambda nums: my_count_sum(nums["num"]))
    results = results.assign(result2=lambda nums: my_count_sum(nums["num"]))
    results = results.assign(next_val=lambda nums: add_one(nums["num"]))
    for row in results:
        assert row["num"] == row["result"]["val"] and row["result"]["label"] == "label"
        assert (
            row["result2"]["val"] == row["result"]["val"]
            and row["result2"]["label"] == row["result"]["label"]
        )
        assert row["next_val"] == row["num"] + 1


def test_table_assign_same_base(db: gp.Database):
    nums = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    nums2 = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    with pytest.raises(Exception) as exc_info:
        nums.assign(num2=lambda _: nums2["num"])
    assert (
        str(exc_info.value)
        == "Current table and newly included expression must be based on the same table"
    )


def test_table_assign_multiple_col(db: gp.Database):
    nums = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    results = nums.assign(a=lambda t: t["num"], b=lambda t: t["num"])
    for row in results:
        assert row["num"] == row["a"] == row["b"]


def test_iter_break(db: gp.Database):
    nums = gp.values([(i,) for i in range(3)], db, column_names=["num"])
    results = []
    for row in nums:
        results.append(row["num"])
        break
    for row in nums:
        results.append(row["num"])
    assert results == [0, 0, 1, 2]
