import psycopg2.errors
import pytest

import greenplumpython as gp
import greenplumpython.pandas.dataframe as pd
from tests import con, db


def test_to_sql(db: gp.Database, con):
    db._execute('DROP TABLE IF EXISTS "test_to_sql"', has_results=False)
    pd_df = pd.read_sql('SELECT unnest(ARRAY[1,2,3]) AS "a",unnest(ARRAY[1,2,3]) AS "b"', con)
    # 1st try: Create Table
    rowcount = pd_df.to_sql(name="test_to_sql", con=con)
    assert rowcount == 3
    # 2nd try: Raise Error if_exists = Fail
    with pytest.raises(Exception) as exc_info:
        pd_df.to_sql(name="test_to_sql", con=con)
    assert 'relation "test_to_sql" already exists' in str(exc_info)
    # 3rd Try: Replace existing table
    pd_df.to_sql(name="test_to_sql", con=con, if_exists="replace")
    df = db.create_dataframe(table_name="test_to_sql")
    assert sorted([tuple(row.values()) for row in df]) == [(1, 1), (2, 2), (3, 3)]
    assert list(next(iter(df)).keys()) == ["a", "b"]
    # 4th try: Insert to exist table
    rowcount = pd_df.to_sql(name="test_to_sql", con=con, if_exists="append")
    assert rowcount == 3
    df = db.create_dataframe(table_name="test_to_sql")
    assert sorted([tuple(row.values()) for row in df]) == [
        (1, 1),
        (1, 1),
        (2, 2),
        (2, 2),
        (3, 3),
        (3, 3),
    ]
    assert list(next(iter(df)).keys()) == ["a", "b"]
    db._execute('DROP TABLE IF EXISTS "test_to_sql"', has_results=False)


def test_pddf_to_df(db: gp.Database, con):
    columns = {"val": [(1,) for _ in range(10)]}
    df = db.create_dataframe(columns=columns)
    pd_df = pd.DataFrame._from_native(df)
    df = pd_df.to_native()
    assert sum(row["val"] for row in df) == 10


def test_sort_values(db: gp.Database, con):
    # fmt: off
    rows = [(1, "Mona Lisa", None), (5, "The Birth of Venus", None),
            (3, "The Scream", 1889, ), (2, "The Starry Night", 1889,),
            (4, "The Night Watch", 1642,)]
    # fmt: on
    df = db.create_dataframe(rows=rows, column_names=["id", "painting", "year"])
    pd_df = pd.DataFrame._from_native(df)
    ret = list(pd_df.sort_values(["year", "id"], ascending=[False, True], na_position="first"))
    assert ret[0]["year"] is None and ret[0]["id"] == 1
    assert ret[1]["year"] is None and ret[1]["id"] == 5
    assert ret[-1]["year"] == 1642


def test_drop_duplicates(db: gp.Database, con):
    rows = [(i, 1) for i in range(10)]
    df = db.create_dataframe(rows=rows, column_names=["i", "j"])
    pd_df = pd.DataFrame._from_native(df)

    result = list(pd_df.drop_duplicates(subset=["i", "j"]))
    assert len(result) == len(rows)
    for row in result:
        assert "i" in row and "j" in row

    result = list(pd_df.drop_duplicates(subset="j"))
    assert len(result) == 1
    for row in result:
        assert "i" in row and "j" in row


def test_merge(db: gp.Database):
    # fmt: off
    rows1 = [(1, "Lion",), (2, "Tiger",), (3, "Wolf",), (4, "Fox")]
    rows2 = [(1, "Tiger",), (2, "Lion",), (3, "Rhino",), (4, "Panther")]
    # fmt: on
    zoo_1_df = db.create_dataframe(rows=rows1, column_names=["zoo1_id", "zoo1_animal"])
    zoo_2_df = db.create_dataframe(rows=rows2, column_names=["zoo2_id", "zoo2_animal"])
    zoo_1_pd_df = pd.DataFrame._from_native(zoo_1_df)
    zoo_2_pd_df = pd.DataFrame._from_native(zoo_2_df)

    ret: pd.DataFrame = zoo_1_pd_df.merge(
        zoo_2_pd_df, how="inner", left_on="zoo1_animal", right_on="zoo2_animal"
    )
    assert len(list(ret)) == 2
    for row in ret:
        assert row["zoo1_animal"] == row["zoo2_animal"]
        assert row["zoo1_animal"] == "Lion" or row["zoo1_animal"] == "Tiger"


def test_merge_same_column_name(db: gp.Database):
    # fmt: off
    rows1 = [(1, "Lion",), (2, "Tiger",), (3, "Wolf",), (4, "Fox")]
    rows2 = [(1, "Tiger",), (2, "Lion",), (3, "Rhino",), (4, "Panther")]
    # fmt: on
    zoo_1_df = db.create_dataframe(rows=rows1, column_names=["zoo1_id", "zoo1_animal"])
    zoo_2_df = db.create_dataframe(rows=rows2, column_names=["zoo2_id", "zoo2_animal"])
    zoo_1_pd_df = pd.DataFrame._from_native(zoo_1_df)
    zoo_2_pd_df = pd.DataFrame._from_native(zoo_2_df)

    with pytest.raises(Exception) as exc_info:
        zoo_1_pd_df.merge(zoo_2_pd_df, how="inner", on="animal")
    assert "Can't support duplicate columns name in both DataFrame" in str(exc_info.value)


def test_read_sql(db: gp.Database, con):
    db._execute("DROP TABLE IF EXISTS test_read_sql", has_results=False)
    columns = {"a": [1, 2, 3], "b": [1, 2, 3]}
    db.create_dataframe(columns=columns).save_as("test_read_sql", column_names=["a", "b"])

    with pytest.raises(Exception) as exc_info:
        pd_df = pd.read_sql("test_read_sql", con)
        list(pd_df)
    assert exc_info.type == psycopg2.errors.SyntaxError

    pd_df = pd.read_sql("SELECT * FROM test_read_sql", con)
    assert sorted([tuple(row.values()) for row in list(pd_df)]) == [(1, 1), (2, 2), (3, 3)]
    db._execute("DROP TABLE IF EXISTS test_read_sql", has_results=False)
