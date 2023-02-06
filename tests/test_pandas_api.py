import pytest
from sqlalchemy import create_engine

import greenplumpython as gp
import greenplumpython.pandas.dataframe as pd
from tests import con, db


def test_df_to_pddf(db: gp.Database):
    rows = [(1,) for _ in range(10)]
    df = db.create_dataframe(rows=rows, column_names=["val"])
    pddf_from_df = pd.create_dataframe(dataframe=df)
    assert sum(row["val"] for row in pddf_from_df) == 10


def test_rows_to_pddf(db: gp.Database):
    rows = [(1,) for _ in range(10)]
    pddf_from_df = pd.create_dataframe(rows=rows, column_names=["val"], db=db)
    assert sum(row["val"] for row in pddf_from_df) == 10


def test_columns_to_pddf(db: gp.Database):
    columns = {"val": [(1,) for _ in range(10)]}
    pddf_from_df = pd.create_dataframe(columns=columns, db=db)
    assert sum(row["val"] for row in pddf_from_df) == 10


def test_to_sql(db: gp.Database, con):
    columns = {"a": [1, 2, 3], "b": [1, 2, 3]}
    df = db.create_dataframe(columns=columns)
    pd_df = pd.DataFrame(df)
    db.execute('DROP TABLE IF EXISTS "test.test_to_sql"', has_results=False)
    assert pd_df.to_sql(name="test_to_sql", con=con, schema="test") == 3
    with pytest.raises(Exception) as exc_info:
        pd_df.to_sql(name="test_to_sql", con=con, schema="test") == 3
    assert 'relation "test.test_to_sql" already exists' in str(exc_info)
    pd_df.to_sql(name="test_to_sql", con=con, schema="test", if_exists="replace") == 3
    df = db.create_dataframe(table_name="test.test_to_sql")
    assert sorted([tuple(row.values()) for row in df]) == [(1, 1), (2, 2), (3, 3)]
    assert list(next(iter(df)).keys()) == ["a", "b"]


def test_sort_values(db: gp.Database):
    # fmt: off
    rows = [(1, "Mona Lisa", None), (5, "The Birth of Venus", None),
            (3, "The Scream", 1889, ), (2, "The Starry Night", 1889,),
            (4, "The Night Watch", 1642,)]
    # fmt: on
    df = db.create_dataframe(rows=rows, column_names=["id", "painting", "year"])
    pd_df = pd.DataFrame(df)
    ret = list(pd_df.sort_values(["year", "id"], ascending=[False, True], na_position="first"))
    assert ret[0]["year"] is None and ret[0]["id"] == 1
    assert ret[1]["year"] is None and ret[1]["id"] == 5
    assert ret[-1]["year"] == 1642


def test_drop_duplicates(db: gp.Database):
    rows = [(i, 1) for i in range(10)]
    df = db.create_dataframe(rows=rows, column_names=["i", "j"])
    pd_df = pd.DataFrame(df)

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
    zoo_1_pd_df = pd.DataFrame(zoo_1_df)
    zoo_2_pd_df = pd.DataFrame(zoo_2_df)

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
    zoo_1_df = db.create_dataframe(rows=rows1, column_names=["zoo1_id", "animal"])
    zoo_2_df = db.create_dataframe(rows=rows2, column_names=["zoo2_id", "animal"])
    zoo_1_pd_df = pd.DataFrame(zoo_1_df)
    zoo_2_pd_df = pd.DataFrame(zoo_2_df)

    with pytest.raises(Exception) as exc_info:
        zoo_1_pd_df.merge(zoo_2_pd_df, how="inner", on="animal")
    assert str(exc_info.value) == "Can't support duplicate columns name in both DataFrame"


def test_read_sql(db: gp.Database, con):
    db.execute("DROP TABLE IF EXISTS test_read_sql", has_results=False)
    columns = {"a": [1, 2, 3], "b": [1, 2, 3]}
    db.create_dataframe(columns=columns).save_as("test_read_sql", column_names=["a", "b"])

    pd_df = pd.read_sql("test_read_sql", con)
    assert sorted([tuple(row.values()) for row in list(pd_df)]) == [(1, 1), (2, 2), (3, 3)]

    pd_df = pd.read_sql("SELECT * FROM test_read_sql", con)
    assert sorted([tuple(row.values()) for row in list(pd_df)]) == [(1, 1), (2, 2), (3, 3)]
