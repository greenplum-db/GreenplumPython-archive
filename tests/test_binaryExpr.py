import pytest
import greenplumpython as gp


@pytest.fixture
def db():
    db = gp.database(host="localhost", dbname="gpadmin")
    yield db
    db.close()


# TODO : Add Query Condition Test
def test_expr_bin_equal_int(db: gp.Database):
    rows = [(1,), (2,), (3,), (2,)]
    t = gp.values(rows, db=db)
    t = t.save_as("temp1", column_names=["id"])
    c = t["id"]
    b1 = gp.expr.BinaryExpr("=", c, 2)
    assert str(b1) == "temp1.id = 2"


# TODO : Add Query Condition Test
def test_expr_bin_equal_str(db: gp.Database):
    rows = [("'a'",), ("'b'",), ("'c'",)]
    t = gp.values(rows, db=db)
    t = t.save_as("temp2", column_names=["id"])
    c = t["id"]
    b2 = gp.expr.BinaryExpr("=", c, "a")
    assert str(b2) == "temp2.id = \"a\""


# TODO : Add Query Condition Test
def test_expr_bin_equal_none(db: gp.Database):
    rows = [("'a'",), ("NULL",), ("'c'",)]
    t = gp.values(rows, db=db)
    t = t.save_as("temp3", column_names=["id"])
    c = t["id"]
    b3 = gp.expr.BinaryExpr("=", c, None)
    assert str(b3) == "temp3.id = NULL"


# TODO : Add Query Condition Test
def test_expr_bin_equal_2expr(db: gp.Database):
    rows = [(1,), (2,), (3,), (2,)]
    t = gp.values(rows, db=db)
    t = t.save_as("temp1", column_names=["id"])
    c = t["id"]
    t2 = gp.values(rows, db=db)
    t2 = t.save_as("temp2", column_names=["id"])
    c2 = t2["id"]
    b4 = gp.expr.BinaryExpr("=", c, c2)
    assert str(b4) == "temp1.id = temp2.id"


# TODO : Add Query Condition Test
def test_expr_bin_equal_bool(db: gp.Database):
    rows = [(0,), (1,), (1,), (0,)]
    t = gp.values(rows, db=db)
    t = t.save_as("temp1", column_names=["id"])
    c = t["id"]
    b5 = gp.expr.BinaryExpr("=", c, True)
    assert str(b5) == "temp1.id = TRUE"
