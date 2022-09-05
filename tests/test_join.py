from os import environ

import pytest

import greenplumpython as gp
from tests import db


@pytest.fixture
def t1(db: gp.Database):
    # fmt: off
    rows1 = [(1, 0, "a1",), (2, 0, "a2",), (3, 0, "a3",)]
    # fmt: on
    t = gp.values(rows1, db=db).save_as("temp1", temp=True, column_names=["id1", "idd1", "n1"])
    return t


@pytest.fixture
def t2(db: gp.Database):
    # fmt: off
    rows2 = [(1, 0, "b1",), (2, 0, "b2",), (3, 0, "b3",)]
    # fmt: on
    t = gp.values(rows2, db=db).save_as("temp2", temp=True, column_names=["id2", "idd2", "n2"])
    return t


@pytest.fixture
def zoo_1(db: gp.Database):
    # fmt: off
    rows = [(1, "Lion",), (2, "Tiger",), (3, "Wolf",), (4, "Fox")]
    # fmt: on
    t = gp.values(rows, db=db, column_names=["id", "animal"])
    return t


@pytest.fixture
def zoo_2(db: gp.Database):
    # fmt: off
    rows = [(1, "Tiger",), (2, "Lion",), (3, "Rhino",), (4, "Panther")]
    # fmt: on
    t = gp.values(rows, db=db, column_names=["id", "animal"])
    return t


def test_join_both_all_targets(db: gp.Database, t1: gp.Table, t2: gp.Table):
    ret = t1._join(
        t2,
        targets=[t1["*"], t2["*"]],
        how="INNER JOIN",
        on_str="ON temp1.id1 = temp2.id2",
    ).fetch()
    assert list(list(ret)[0].keys()) == ["id1", "idd1", "n1", "id2", "idd2", "n2"]


def test_join_both_empty_targets(db: gp.Database, t1: gp.Table, t2: gp.Table):
    ret = t1._join(t2, targets=[], how="INNER JOIN", on_str="ON temp1.id1 = temp2.id2").fetch()
    assert list(list(ret)[0].keys()) == ["id1", "idd1", "n1", "id2", "idd2", "n2"]


def test_join_all_empty_targets(db: gp.Database, t1: gp.Table, t2: gp.Table):
    ret = t1._join(
        t2, targets=[t2["*"]], how="INNER JOIN", on_str="ON temp1.id1 = temp2.id2"
    ).fetch()
    assert list(list(ret)[0].keys()) == ["id2", "idd2", "n2"]


def test_join_all_one_targets(db: gp.Database, t1: gp.Table, t2: gp.Table):
    ret = t1._join(
        t2, targets=[t2["*"], t1["id1"]], how="INNER JOIN", on_str="ON temp1.id1 = temp2.id2"
    )
    assert list(list(ret.fetch())[0].keys()) == ["id2", "idd2", "n2", "id1"]
    assert ret.columns is None


def test_join_both_mulp_targets(db: gp.Database, t1: gp.Table, t2: gp.Table):
    ret = t1._join(
        t2,
        targets=[t1["id1"], t1["n1"], t2["idd2"]],
        how="INNER JOIN",
        on_str="ON temp1.id1 = temp2.id2",
    ).fetch()
    assert list(list(ret)[0].keys()) == ["id1", "n1", "idd2"]


# FIXME : Test for no exist target column


def test_join_same_column_names_alias(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t1 = gp.values(rows, db=db).save_as("temp1", temp=True, column_names=["id"])
    t2 = gp.values(rows, db=db).save_as("temp2", temp=True, column_names=["id"])
    ret = t1._join(
        t2,
        targets=[t1["id"].rename("t1_id"), t2["id"].rename("t2_id")],
        how="INNER JOIN",
        on_str="ON temp1.id = temp2.id",
    )
    for row in ret.fetch():
        assert "t1_id" in row and "t2_id" in row


def test_join_same_column_names(db: gp.Database):
    rows = [(1, 1), (2, 1), (3, 1)]
    t1 = gp.values(rows, db=db).save_as("temp1", temp=True, column_names=["id", "n1"])
    t2 = gp.values(rows, db=db).save_as("temp2", temp=True, column_names=["id", "n2"])
    ret = t1._join(
        t2,
        targets=[],
        how="INNER JOIN",
        on_str="ON temp1.id = temp2.id",
    ).fetch()
    # FIXME: Test for when there are same name in selected columns


def test_table_inner_join(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.inner_join(
        zoo_2,
        zoo_1["animal"] == zoo_2["animal"],
        targets=[
            zoo_1["animal"].rename("zoo1_animal"),
            zoo_2["animal"].rename("zoo2_animal"),
            zoo_1["id"].rename("zoo1_id"),
            zoo_2["id"].rename("zoo2_id"),
        ],
    ).fetch()
    assert len(list(ret)) == 2
    for row in list(ret):
        assert row["zoo1_animal"] == row["zoo2_animal"]
        assert row["zoo1_animal"] == "Lion" or row["zoo1_animal"] == "Tiger"


def test_table_left_join(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.left_join(
        zoo_2,
        zoo_1["animal"] == zoo_2["animal"],
        targets=[
            zoo_1["animal"].rename("zoo1_animal"),
            zoo_2["animal"].rename("zoo2_animal"),
            zoo_1["id"].rename("zoo1_id"),
            zoo_2["id"].rename("zoo2_id"),
        ],
    ).fetch()
    assert len(list(ret)) == 4
    for row in list(ret):
        if row["zoo1_animal"] == "Lion" or row["zoo1_animal"] == "Tiger":
            assert row["zoo1_animal"] == row["zoo2_animal"]
        else:
            assert row["zoo2_animal"] is None
            assert row["zoo2_id"] is None


def test_table_right_join(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.right_join(
        zoo_2,
        zoo_1["animal"] == zoo_2["animal"],
        targets=[
            zoo_1["animal"].rename("zoo1_animal"),
            zoo_2["animal"].rename("zoo2_animal"),
            zoo_1["id"].rename("zoo1_id"),
            zoo_2["id"].rename("zoo2_id"),
        ],
    ).fetch()
    assert len(list(ret)) == 4
    for row in list(ret):
        if row["zoo2_animal"] == "Lion" or row["zoo2_animal"] == "Tiger":
            assert row["zoo1_animal"] == row["zoo2_animal"]
        else:
            assert row["zoo1_animal"] is None
            assert row["zoo1_id"] is None


def test_table_full_join(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.full_outer_join(
        zoo_2,
        zoo_1["animal"] == zoo_2["animal"],
        targets=[
            zoo_1["animal"].rename("zoo1_animal"),
            zoo_2["animal"].rename("zoo2_animal"),
            zoo_1["id"].rename("zoo1_id"),
            zoo_2["id"].rename("zoo2_id"),
        ],
    ).fetch()
    assert len(list(ret)) == 6
    for row in list(ret):
        if row["zoo2_animal"] == "Lion" or row["zoo2_animal"] == "Tiger":
            assert row["zoo1_animal"] == row["zoo2_animal"]
        else:
            assert (row["zoo1_animal"] is None and row["zoo2_animal"] is not None) or (
                row["zoo1_animal"] is not None and row["zoo2_animal"] is None
            )
            assert (row["zoo1_id"] is None and row["zoo2_id"] is not None) or (
                row["zoo1_id"] is not None and row["zoo2_id"] is None
            )


def test_table_natural_join(db: gp.Database):
    # fmt: off
    rows1 = [("Smart Phone", 1,), ("Laptop", 2,), ("Tablet", 3,)]
    rows2 = [("iPhone", 1,), ("Samsung Galaxy", 1,), ("HP Elite", 2,),
             ("Lenovo Thinkpad", 2,), ("iPad", 3,), ("Kindle Fire", 3)]
    # fmt: on
    categories = gp.values(rows1, db=db, column_names=["category_name", "category_id"])
    products = gp.values(rows2, db=db, column_names=["product_name", "category_id"])

    ret = categories.natural_join(products).fetch()
    assert len(list(ret)) == 6
    assert list(list(ret)[0].keys()) == ["category_id", "category_name", "product_name"]
    for row in list(ret):
        if row["category_name"] == "Smart Phone":
            assert row["category_id"] == 1
        elif row["category_name"] == "Laptop":
            assert row["category_id"] == 2
        elif row["category_name"] == "Tablet":
            assert row["category_id"] == 3


def test_table_cross_join(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.cross_join(
        zoo_2,
        targets=[
            zoo_1["animal"].rename("zoo1_animal"),
            zoo_2["animal"].rename("zoo2_animal"),
            zoo_1["id"].rename("zoo1_id"),
            zoo_2["id"].rename("zoo2_id"),
        ],
    ).fetch()
    assert len(list(ret)) == 16
    ini1_dict = {"Tiger": 0, "Lion": 0, "Wolf": 0, "Fox": 0}
    ini2_dict = {"Tiger": 0, "Lion": 0, "Rhino": 0, "Panther": 0}
    zoo2_cpt = {"Tiger": ini2_dict, "Lion": ini2_dict, "Wolf": ini2_dict, "Fox": ini2_dict}
    zoo1_cpt = {"Tiger": ini1_dict, "Lion": ini1_dict, "Rhino": ini1_dict, "Panther": ini1_dict}
    for row in list(ret):
        zoo2_cpt[row["zoo1_animal"]][row["zoo2_animal"]] += 1
        zoo1_cpt[row["zoo2_animal"]][row["zoo1_animal"]] += 1
    for key, values in zoo1_cpt.items():
        assert values == {"Tiger": 4, "Lion": 4, "Wolf": 4, "Fox": 4}
    for key, values in zoo2_cpt.items():
        assert values == {"Tiger": 4, "Lion": 4, "Rhino": 4, "Panther": 4}


def test_table_self_join(db: gp.Database, zoo_1: gp.Table):
    zoo_2 = zoo_1.rename("zoo2")
    ret = zoo_1.inner_join(
        zoo_2,
        zoo_1["animal"] == zoo_2["animal"],
        targets=[
            zoo_1["animal"].rename("zoo1_animal"),
            zoo_2["animal"].rename("zoo2_animal"),
            zoo_1["id"].rename("zoo1_id"),
            zoo_2["id"].rename("zoo2_id"),
        ],
    ).fetch()
    assert len(list(ret)) == 4
    for row in list(ret):
        assert row["zoo1_animal"] == row["zoo2_animal"]


def test_table_join_save(db: gp.Database, zoo_1: gp.Table):
    zoo_2 = zoo_1.rename("zoo2")
    t_join = zoo_1.inner_join(
        zoo_2,
        zoo_1["animal"] == zoo_2["animal"],
        targets=[
            zoo_1["id"].rename("zoo1_id"),
            zoo_1["animal"].rename("zoo1_animal"),
            zoo_2["id"].rename("zoo2_id"),
            zoo_2["animal"].rename("zoo2_animal"),
        ],
    )
    t_join.save_as("table_join", temp=True)
    t_join_reload = gp.table("table_join", db=db)
    ret = t_join_reload.fetch()
    assert len(list(list(ret)[0].keys())) == 4
    assert list(list(ret)[0].keys()) == ["zoo1_id", "zoo1_animal", "zoo2_id", "zoo2_animal"]
    for row in list(ret):
        assert row["zoo1_animal"] == row["zoo2_animal"]


def test_table_join_ine(db: gp.Database):
    rows1 = [(1,), (2,), (3,)]
    rows2 = [(2,), (3,), (4,)]
    t1 = gp.values(rows1, db=db, column_names=["a"])
    t2 = gp.values(rows2, db=db, column_names=["b"])
    ret = t1.inner_join(t2, t1["a"] < t2["b"]).fetch()
    assert len(list(ret)) == 6
    for row in list(ret):
        assert row["a"] < row["b"]


def test_table_multiple_self_join(db: gp.Database, zoo_1: gp.Table):
    zoo_2 = zoo_1.rename("zoo2")
    zoo_3 = zoo_2.rename("zoo3")
    t_join = zoo_1.inner_join(
        zoo_2,
        zoo_1["animal"] == zoo_2["animal"],
        targets=[
            zoo_1["id"].rename("zoo1_id"),
            zoo_1["animal"].rename("zoo1_animal"),
            zoo_2["id"].rename("zoo2_id"),
            zoo_2["animal"].rename("zoo2_animal"),
        ],
    )
    ret = t_join.inner_join(
        zoo_3,
        t_join["zoo1_animal"] == zoo_3["animal"],
    ).fetch()
    assert len(list(ret)) == 4
    for row in list(ret):
        assert row["zoo2_animal"] == row["animal"]


# This test case is to guarantee that the CTEs are generated in the reversed
# topological order (i.e. the DFS order) in the lineage graph.
#
# For that, we must ensure there are at least 4 nodes in the graph and the 2
# direct parents of the final node must not be adjacent.
def test_lineage_dfs_order(db: gp.Database):
    rows = [(i,) for i in range(10)]
    numbers = gp.values(rows, db=db, column_names=["val"])
    mod = numbers.extend("mod", numbers["val"] % 2)
    mod3 = mod.extend("mod3", mod["val"] % 3)
    results = mod3.inner_join(numbers, numbers["val"] == mod3["val"], targets=[mod3["val"]])
    assert len(list(results.fetch())) == 10
