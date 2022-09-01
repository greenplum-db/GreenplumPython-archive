from os import environ

import pytest

import greenplumpython as gp
from tests import db


@pytest.fixture
def t(db: gp.Database):
    generate_series = gp.function("generate_series")
    t = generate_series(0, 9, as_name="id", db=db).to_table()
    return t


def test_const_table(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t = gp.values(rows, db=db, column_names=["id"])
    t = t.save_as("const_table", column_names=["id"], temp=True)
    assert sorted([tuple(row.values()) for row in t.fetch()]) == sorted(rows)

    t_cols = t.column_names().fetch()
    assert len(list(t_cols)) == 1
    for row in t_cols:
        assert row["column_name"] == "id"


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
    for row in t_sub.fetch():
        assert "id" in row and "num" in row


def test_table_getitem_slice_limit(db: gp.Database, t: gp.Table):
    ret = list(t[:2].fetch())
    assert len(ret) == 2


def test_table_getitem_slice_offset(db: gp.Database, t: gp.Table):
    ret = list(t[7:].fetch())
    assert len(ret) == 3


def test_table_getitem_slice_off_limit(db: gp.Database, t: gp.Table):
    ret = list(t[2:5].fetch())
    assert len(ret) == 3


def test_table_display_repr(db: gp.Database):
    # fmt: off
    rows = [(1, "Lion",), (2, "Tiger",), (3, "Wolf",), (4, "Fox")]
    # fmt: on
    t = gp.values(rows, db=db, column_names=["id", "animal"])
    expected = (
        "| id         || animal     |\n"
        "============================\n"
        "|          1 || Lion       |\n"
        "|          2 || Tiger      |\n"
        "|          3 || Wolf       |\n"
        "|          4 || Fox        |\n"
    )
    assert str(t.order_by(t["id"]).head(4)) == expected


def test_table_display_repr_long_content(db: gp.Database):
    # fmt: off
    rows = [(1, "Lion",), (2, "Tigerrrrrrrrrrrr",), (3, "Wolf",), (4, "Fox")]
    # fmt: on
    t = gp.values(rows, db=db, column_names=["iddddddddddddddddddd", "animal"])
    expected = (
        "| iddddddddddddddddddd || animal     |\n"
        "============================\n"
        "|          1 || Lion       |\n"
        "|          2 || Tigerrrrrrrrrrrr |\n"
        "|          3 || Wolf       |\n"
        "|          4 || Fox        |\n"
    )
    assert str(t.order_by(t["iddddddddddddddddddd"]).head(4)) == expected


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
    assert (t.order_by(t["id"]).head(4)._repr_html_()) == expected


def test_table_extend_const(db: gp.Database):
    nums = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    results = nums.extend("x", "hello").fetch()
    for row in results:
        assert "num" in row and "x" in row and row["x"] == "hello"


def test_table_extend_expr(db: gp.Database):
    @gp.create_function
    def add_one(num: int) -> int:
        return num + 1

    nums = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    # FIXME: How to remove the intermdeiate variable `nums`?
    # FIXME: How to support functions returning more than one column?
    results = nums.extend("result", add_one(nums["num"])).fetch()
    for row in results:
        assert row["result"] == row["num"] + 1


def test_table_extend_same_base(db: gp.Database):
    nums = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    nums2 = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    with pytest.raises(Exception) as exc_info:
        nums.extend("num2", nums2["num"])
    assert (
        str(exc_info.value)
        == "Current table and included expression must be based on the same table"
    )


def test_table_extend_multiple_col(db: gp.Database):
    nums = gp.values([(i,) for i in range(10)], db, column_names=["num"])
    results = nums.extend("a", nums["num"])

    # NOTE: `nums.extend("a", nums["num"]).extend("b", nums["num"])` is not
    # supported because in this case `extend()` is supposed to modify `nums`
    # implicitly and thus is NOT pure.
    results = results.extend("b", results["num"])
    for row in results.fetch():
        assert row["num"] == row["a"] == row["b"]
