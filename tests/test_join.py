from os import environ

import pytest

import greenplumpython as gp
from tests import db


@pytest.fixture
def t1(db: gp.Database):
    # fmt: off
    rows1 = [(1, 0, "a1",), (2, 0, "a2",), (3, 0, "a3",)]
    # fmt: on
    return gp.to_table(rows1, db=db, column_names=["id1", "idd1", "n1"])


@pytest.fixture
def t2(db: gp.Database):
    # fmt: off
    rows2 = [(1, 0, "b1",), (2, 0, "b2",), (3, 0, "b3",)]
    # fmt: on
    return gp.to_table(rows2, db=db, column_names=["id2", "idd2", "n2"])


@pytest.fixture
def zoo_1(db: gp.Database):
    # fmt: off
    rows = [(1, "Lion",), (2, "Tiger",), (3, "Wolf",), (4, "Fox")]
    # fmt: on
    return gp.to_table(rows, db=db, column_names=["id", "animal"])


@pytest.fixture
def zoo_2(db: gp.Database):
    # fmt: off
    rows = [(1, "Tiger",), (2, "Lion",), (3, "Rhino",), (4, "Panther")]
    # fmt: on
    return gp.to_table(rows, db=db, column_names=["id", "animal"])


def test_join_all_and_all_columns(db: gp.Database, t1: gp.Table, t2: gp.Table):
    row = next(
        iter(
            t1.join(
                t2,
                cond=lambda t1, t2: t1["id1"] == t2["id2"],
                self_columns={"*"},
                other_columns={"*"},
            )
        )
    )
    for col in ["id1", "idd1", "n1", "id2", "idd2", "n2"]:
        assert col in row


def test_join_no_and_no_columns(db: gp.Database, t1: gp.Table, t2: gp.Table):
    ret = next(iter(t1.join(t2, cond=lambda t1, t2: t1["id1"] == t2["id2"])))
    assert "id1" not in ret and "id2" not in ret  # NOTE: The row is not empty with psycopg2


def test_join_all_and_no_columns(db: gp.Database, t1: gp.Table, t2: gp.Table):
    ret = next(iter(t1.join(t2, cond=lambda t1, t2: t1["id1"] == t2["id2"], other_columns={"*"})))
    for col in ["id2", "idd2", "n2"]:
        assert col in ret


def test_join_all_and_one_columns(db: gp.Database, t1: gp.Table, t2: gp.Table):
    ret = next(
        iter(
            t1.join(
                t2,
                cond=lambda t1, t2: t1["id1"] == t2["id2"],
                self_columns={"id1"},
                other_columns={"*"},
            )
        )
    )
    for col in ["id2", "idd2", "n2", "id1"]:
        assert col in ret


def test_join_columns_from_list(db: gp.Database, t1: gp.Table, t2: gp.Table):
    ret = next(
        iter(
            t1.join(
                t2,
                cond=lambda t1, t2: t1["id1"] == t2["id2"],
                self_columns=dict.fromkeys(["id1", "n1"]),
                other_columns={"idd2"},
            )
        )
    )
    for col in ["idd2", "n1", "id1"]:
        assert col in ret


# FIXME : Test for no exist target column


def test_join_same_column_using(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t1 = gp.to_table(rows, db=db, column_names=["id"])
    t2 = gp.to_table(rows, db=db, column_names=["id"])
    ret = t1.join(t2, using=["id"], self_columns={"id": "t1_id"}, other_columns={"id": "t2_id"})
    for row in ret:
        assert "t1_id" in row and "t2_id" in row


def test_join_same_column_names(db: gp.Database):
    rows = [(1, 1), (2, 1), (3, 1)]
    t1 = gp.to_table(rows, db=db, column_names=["id", "n1"])
    t2 = gp.to_table(rows, db=db, column_names=["id", "n2"])
    ret = t1.cross_join(
        t2,
        self_columns={"*"},
        other_columns={"*"},
    )
    with pytest.raises(Exception) as e:
        print(ret)
    assert str(e.value) == ("Duplicate column_name(s) found: id")


def test_table_inner_join(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret: gp.Table = zoo_1.join(
        zoo_2,
        using=["animal"],
        self_columns={"animal": "zoo1_animal", "id": "zoo1_id"},
        other_columns={"animal": "zoo2_animal", "id": "zoo2_id"},
    )
    assert len(list(ret)) == 2
    for row in ret:
        assert row["zoo1_animal"] == row["zoo2_animal"]
        assert row["zoo1_animal"] == "Lion" or row["zoo1_animal"] == "Tiger"


def test_table_left_join(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.left_join(
        zoo_2,
        using=["animal"],
        self_columns={"animal": "zoo1_animal", "id": "zoo1_id"},
        other_columns={"animal": "zoo2_animal", "id": "zoo2_id"},
    )
    assert len(list(ret)) == 4
    for row in ret:
        if row["zoo1_animal"] == "Lion" or row["zoo1_animal"] == "Tiger":
            assert row["zoo1_animal"] == row["zoo2_animal"]
        else:
            assert row["zoo2_animal"] is None
            assert row["zoo2_id"] is None


def test_table_right_join(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.right_join(
        zoo_2,
        using=["animal"],
        self_columns={"animal": "zoo1_animal", "id": "zoo1_id"},
        other_columns={"animal": "zoo2_animal", "id": "zoo2_id"},
    )
    assert len(list(ret)) == 4
    for row in ret:
        if row["zoo2_animal"] == "Lion" or row["zoo2_animal"] == "Tiger":
            assert row["zoo1_animal"] == row["zoo2_animal"]
        else:
            assert row["zoo1_animal"] is None
            assert row["zoo1_id"] is None


def test_table_full_join(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.full_join(
        zoo_2,
        using=["animal"],
        self_columns={"animal": "zoo1_animal", "id": "zoo1_id"},
        other_columns={"animal": "zoo2_animal", "id": "zoo2_id"},
    )
    assert len(list(ret)) == 6
    for row in ret:
        if row["zoo2_animal"] == "Lion" or row["zoo2_animal"] == "Tiger":
            assert row["zoo1_animal"] == row["zoo2_animal"]
        else:
            assert (row["zoo1_animal"] is None and row["zoo2_animal"] is not None) or (
                row["zoo1_animal"] is not None and row["zoo2_animal"] is None
            )
            assert (row["zoo1_id"] is None and row["zoo2_id"] is not None) or (
                row["zoo1_id"] is not None and row["zoo2_id"] is None
            )


def test_join_natural(db: gp.Database):
    # fmt: off
    rows1 = [("Smart Phone", 1,), ("Laptop", 2,), ("Tablet", 3,)]
    rows2 = [("iPhone", 1,), ("Samsung Galaxy", 1,), ("HP Elite", 2,),
             ("Lenovo Thinkpad", 2,), ("iPad", 3,), ("Kindle Fire", 3)]
    # fmt: on
    categories = gp.to_table(rows1, db=db, column_names=["category_name", "category_id"])
    products = gp.to_table(rows2, db=db, column_names=["product_name", "category_id"])

    ret = categories.join(
        products,
        using=["category_id"],
        self_columns={"category_name", "category_id"},
        other_columns={"product_name"},
    )
    assert len(list(ret)) == 6
    for col in ["category_id", "category_name", "product_name"]:
        assert col in next(iter(ret))
    for row in ret:
        if row["category_name"] == "Smart Phone":
            assert row["category_id"] == 1
        elif row["category_name"] == "Laptop":
            assert row["category_id"] == 2
        elif row["category_name"] == "Tablet":
            assert row["category_id"] == 3


def test_table_cross_join(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.cross_join(
        zoo_2,
        self_columns={"animal": "zoo1_animal", "id": "zoo1_id"},
        other_columns={"animal": "zoo2_animal", "id": "zoo2_id"},
    )
    assert len(list(ret)) == 16
    ini1_dict = {"Tiger": 0, "Lion": 0, "Wolf": 0, "Fox": 0}
    ini2_dict = {"Tiger": 0, "Lion": 0, "Rhino": 0, "Panther": 0}
    zoo2_cpt = {"Tiger": ini2_dict, "Lion": ini2_dict, "Wolf": ini2_dict, "Fox": ini2_dict}
    zoo1_cpt = {"Tiger": ini1_dict, "Lion": ini1_dict, "Rhino": ini1_dict, "Panther": ini1_dict}
    for row in ret:
        zoo2_cpt[row["zoo1_animal"]][row["zoo2_animal"]] += 1
        zoo1_cpt[row["zoo2_animal"]][row["zoo1_animal"]] += 1
    for _, values in zoo1_cpt.items():
        assert values == {"Tiger": 4, "Lion": 4, "Wolf": 4, "Fox": 4}
    for _, values in zoo2_cpt.items():
        assert values == {"Tiger": 4, "Lion": 4, "Rhino": 4, "Panther": 4}


def test_table_self_join(db: gp.Database, zoo_1: gp.Table):
    zoo_2 = zoo_1.rename("zoo2")
    ret: gp.Table = zoo_1.join(
        zoo_2,
        using=["animal"],
        self_columns={"animal": "zoo1_animal", "id": "zoo1_id"},
        other_columns={"animal": "zoo2_animal", "id": "zoo2_id"},
    )
    assert len(list(ret)) == 4
    for row in ret:
        assert row["zoo1_animal"] == row["zoo2_animal"]


def test_table_join_save(db: gp.Database, zoo_1: gp.Table):
    zoo_2 = zoo_1.rename("zoo2")
    t_join: gp.Table = zoo_1.join(
        zoo_2,
        using=["animal"],
        self_columns={"animal": "zoo1_animal", "id": "zoo1_id"},
        other_columns={"animal": "zoo2_animal", "id": "zoo2_id"},
    )
    t_join.save_as("table_join", temp=True)
    t_join_reload = gp.table("table_join", db=db)
    for col in ["zoo1_id", "zoo1_animal", "zoo2_id", "zoo2_animal"]:
        assert col in next(iter(t_join_reload)).column_names()
    for row in t_join_reload:
        assert row["zoo1_animal"] == row["zoo2_animal"]


def test_table_join_ine(db: gp.Database):
    rows1 = [(1,), (2,), (3,)]
    rows2 = [(2,), (3,), (4,)]
    t1 = gp.to_table(rows1, db=db, column_names=["a"])
    t2 = gp.to_table(rows2, db=db, column_names=["b"])
    ret = t1.join(
        t2, cond=lambda t1, t2: t1["a"] < t2["b"], self_columns={"a"}, other_columns={"b"}
    )
    assert len(list(ret)) == 6
    for row in ret:
        assert row["a"] < row["b"]


def test_table_multiple_self_join(db: gp.Database, zoo_1: gp.Table):
    zoo_2 = zoo_1.rename("zoo2")
    zoo_3 = zoo_2.rename("zoo3")
    t_join = zoo_1.join(
        zoo_2,
        using=["animal"],
        self_columns={"animal": "zoo1_animal", "id": "zoo1_id"},
        other_columns={"animal": "zoo2_animal", "id": "zoo2_id"},
    )
    ret = t_join.join(
        zoo_3,
        cond=lambda s, o: s["zoo1_animal"] == o["animal"],
        self_columns={"*"},
        other_columns={"*"},
    )
    assert len(list(ret)) == 4
    for row in ret:
        assert row["zoo2_animal"] == row["animal"]


# This test case is to guarantee that the CTEs are generated in the reversed
# topological order (i.e. the DFS order) in the lineage graph.
#
# For that, we must ensure there are at least 4 nodes in the graph and the 2
# direct parents of the final node must not be adjacent.
def test_lineage_dfs_order(db: gp.Database):
    rows = [(i,) for i in range(10)]
    numbers = gp.to_table(rows, db=db, column_names=["val"])
    mod = numbers.assign(mod=lambda t: t["val"] % 2)
    mod3 = mod.assign(mod3=lambda t: t["val"] % 3)
    results: gp.Table = mod3.join(numbers, using=["val"], self_columns={"val"})
    assert len(list(results)) == 10
