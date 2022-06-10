import pytest

import greenplumpython as gp


@pytest.fixture
def db() -> gp.Database:
    db = gp.database(host="localhost", dbname="gpadmin")
    yield db
    db.close()


@pytest.fixture
def t1(db: gp.Database):
    # fmt: off
    rows1 = [(1, 0, "'a1'",), (2, 0, "'a2'",), (3, 0, "'a3'",)]
    # fmt: on
    t = gp.values(rows1, db=db).save_as("temp1", temp=True, column_names=["id1", "idd1", "n1"])
    return t


@pytest.fixture
def t2(db: gp.Database):
    # fmt: off
    rows2 = [(1, 0, "'b1'",), (2, 0, "'b2'",), (3, 0, "'b3'",)]
    # fmt: on
    t = gp.values(rows2, db=db).save_as("temp2", temp=True, column_names=["id2", "idd2", "n2"])
    return t


@pytest.fixture
def zoo_1(db: gp.Database):
    return gp.table("zoo1", db)


@pytest.fixture
def zoo_2(db: gp.Database):
    return gp.table("zoo2", db)


def test_join_both_default_targets(db: gp.Database, t1: gp.Table, t2: gp.Table):
    ret = t1._join(
        t2,
        my_targets=["*"],
        other_targets=["*"],
        how="INNER JOIN",
        on_str="ON temp1.id1 = temp2.id2",
    ).fetch()
    assert list(list(ret)[0].keys()) == ["id1", "idd1", "n1", "id2", "idd2", "n2"]


def test_join_both_empty_targets(db: gp.Database, t1: gp.Table, t2: gp.Table):
    ret = t1._join(
        t2, my_targets=[], other_targets=[], how="INNER JOIN", on_str="ON temp1.id1 = temp2.id2"
    ).fetch()
    assert list(list(ret)[0].keys()) == ["id1", "idd1", "n1", "id2", "idd2", "n2"]


def test_join_df_emp_targets(db: gp.Database, t1: gp.Table, t2: gp.Table):
    ret = t1._join(
        t2, my_targets=[], other_targets=["*"], how="INNER JOIN", on_str="ON temp1.id1 = temp2.id2"
    ).fetch()
    assert list(list(ret)[0].keys()) == ["id2", "idd2", "n2"]


def test_join_both_mulp_targets(db: gp.Database, t1: gp.Table, t2: gp.Table):
    ret = t1._join(
        t2,
        my_targets=["id1", "n1"],
        other_targets=["idd2"],
        how="INNER JOIN",
        on_str="ON temp1.id1 = temp2.id2",
    ).fetch()
    assert list(list(ret)[0].keys()) == ["id1", "n1", "idd2"]


def test_join_no_exit_targets(db: gp.Database, t1: gp.Table, t2: gp.Table):
    with pytest.raises(Exception, match=r"not_exist_target column not exists"):
        ret = t1._join(
            t2,
            my_targets=["id1", "n1"],
            other_targets=["not_exist_target"],
            how="INNER JOIN",
            on_str="ON temp1.id1 = temp2.id2",
        ).fetch()


def test_join_same_column_names_alias(db: gp.Database):
    rows = [(1,), (2,), (3,)]
    t1 = gp.values(rows, db=db).save_as("temp1", temp=True, column_names=["id"])
    t2 = gp.values(rows, db=db).save_as("temp2", temp=True, column_names=["id"])
    ret = t1._join(
        t2,
        my_targets=["id AS t1_id"],
        other_targets=["id AS t2_id"],
        how="INNER JOIN",
        on_str="ON temp1.id = temp2.id",
    ).fetch()
    assert list(list(ret)[0].keys()) == ["t1_id", "t2_id"]


def test_join_same_column_names(db: gp.Database):
    rows = [(1, 1), (2, 1), (3, 1)]
    t1 = gp.values(rows, db=db).save_as("temp1", temp=True, column_names=["id", "n1"])
    t2 = gp.values(rows, db=db).save_as("temp2", temp=True, column_names=["id", "n2"])
    ret = t1._join(
        t2,
        my_targets=["*"],
        other_targets=["*"],
        how="INNER JOIN",
        on_str="ON temp1.id = temp2.id",
    ).fetch()
    # FIXME: Add alias automatically when there are same name in selected columns
    assert list(list(ret)[0].keys()) == ["temp1_id", "n1", "temp2_id", "n2"]


def test_table_inner_join_eq(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.inner_join(zoo_2, zoo_1["animal"] == zoo_2["animal"]).fetch()
    assert len(list(ret)) == 2
    for row in list(ret):
        assert row["zoo1_animal"] == row["zoo2_animal"]
        assert row["zoo1_animal"] == "Lion" or row["zoo1_animal"] == "Tiger"


def test_table_left_join_lt(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.left_join(zoo_2, zoo_1["animal"] == zoo_2["animal"]).fetch()
    assert len(list(ret)) == 4
    for row in list(ret):
        if row["zoo1_animal"] == "Lion" or row["zoo1_animal"] == "Tiger":
            assert row["zoo1_animal"] == row["zoo2_animal"]
        else:
            assert row["zoo2_animal"] is None
            assert row["zoo2_id"] is None


def test_table_right_join_lt(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.right_join(zoo_2, zoo_1["animal"] == zoo_2["animal"]).fetch()
    assert len(list(ret)) == 4
    for row in list(ret):
        if row["zoo2_animal"] == "Lion" or row["zoo2_animal"] == "Tiger":
            assert row["zoo1_animal"] == row["zoo2_animal"]
        else:
            assert row["zoo1_animal"] is None
            assert row["zoo1_id"] is None


def test_table_full_join_target(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.full_outer_join(zoo_2, zoo_1["animal"] == zoo_2["animal"]).fetch()
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


def test_table_cross_join(db: gp.Database, zoo_1: gp.Table, zoo_2: gp.Table):
    ret = zoo_1.cross_join(zoo_2).fetch()
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
    zoo_2 = zoo_1.as_name("zoo2")
    ret = zoo_1.inner_join(zoo_2, zoo_1["animal"] == zoo_2["animal"]).fetch()
    assert len(list(ret)) == 4
    for row in list(ret):
        assert row["zoo1_animal"] == row["zoo2_animal"]


def test_table_join_save(db: gp.Database, zoo_1: gp.Table):
    zoo_2 = zoo_1.as_name("zoo2")
    t_join = zoo_1.inner_join(zoo_2, zoo_1["animal"] == zoo_2["animal"])
    t_join.save_as("table_join")
    t_join_reload = gp.table("table_join", db=db)
    ret = t_join_reload.fetch()
    assert len(list(list(ret)[0].keys())) == 4
    assert list(list(ret)[0].keys()) == ["zoo1_id", "zoo1_animal", "zoo2_id", "zoo2_animal"]
    for row in list(ret):
        assert row["zoo1_animal"] == row["zoo2_animal"]


def test_table_join_ine(db: gp.Database):
    rows1 = [(1,), (2,), (3,)]
    rows2 = [(2,), (3,), (4,)]
    t1 = gp.values(rows1, db=db).save_as("temp1", temp=True, column_names=["a"])
    t2 = gp.values(rows2, db=db).save_as("temp2", temp=True, column_names=["b"])
    ret = t1.inner_join(t2, t1["a"] < t2["b"]).fetch()
    assert len(list(ret)) == 6
    for row in list(ret):
        assert row["a"] < row["b"]
