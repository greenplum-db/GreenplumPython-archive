import pytest

import greenplumpython as gp
from tests import db


@pytest.fixture
def top_rated_films(db: gp.Database):
    # fmt: off
    rows = [("The Shawshank Redemption", 1994,), ("The Godfather", 1972,), ("12 Angry Men", 1957,)]
    # fmt: on
    t = gp.values(rows, db=db).save_as(
        "top_rated_films", temp=True, column_names=["title", "release_year"]
    )
    return t


@pytest.fixture
def most_popular_films(db: gp.Database):
    # fmt: off
    rows = [("An American Pickle", 2020,), ("The Godfather", 1972,), ("Greyhound", 2020,)]
    # fmt: on
    t = gp.values(rows, db=db).save_as(
        "most_popular_films", temp=True, column_names=["title", "release_year"]
    )
    return t


def test_union(db: gp.Database, top_rated_films: gp.Table, most_popular_films: gp.Table):
    ret = list(top_rated_films.union(most_popular_films).fetch())
    assert len(ret) == 5


def test_union_all(db: gp.Database, top_rated_films: gp.Table, most_popular_films: gp.Table):
    ret = list(top_rated_films.union(most_popular_films, is_all=True).fetch())
    assert len(ret) == 6
    cpt = 0
    for row in ret:
        if row["title"] == "The Godfather":
            cpt += 1
    assert cpt == 2


def test_union_select(db: gp.Database, top_rated_films: gp.Table, most_popular_films: gp.Table):
    top_rated_films = top_rated_films.select(
        top_rated_films["title"].rename("titltle"), top_rated_films["release_year"]
    )
    ret = list(top_rated_films.union(most_popular_films).fetch())
    assert len(ret) == 5
    assert list(ret[0].keys()) == ["titltle", "release_year"]


def test_union_select_disrespect_order_same_type(db: gp.Database):
    # fmt: off
    rows1 = [("An American Pickle", "2020",)]
    rows2 = [("2021", "An American Pickle",)]
    # fmt: on
    t1 = gp.values(rows1, db=db).save_as("t1", temp=True, column_names=["title", "release_year"])
    t2 = gp.values(rows2, db=db).save_as("t2", temp=True, column_names=["release_year", "title"])
    for row in list(t1.union(t2).fetch()):
        assert (row["title"] == "An American Pickle" and row["release_year"] == "2020") or (
            row["title"] == "2021" and row["release_year"] == "An American Pickle"
        )


def test_union_select_disrespect_order_diff_type(
    db: gp.Database, top_rated_films: gp.Table, most_popular_films: gp.Table
):
    top_rated_films = top_rated_films.select(
        top_rated_films["release_year"], top_rated_films["title"]
    )
    # NOTE: UNION consider the order of columns rather than the name
    # It will reveal errors if order of columns is not identical
    with pytest.raises(Exception) as exc_info:
        top_rated_films.union(most_popular_films).fetch()
    assert str(exc_info.value).startswith("UNION types integer and text cannot be matched\n")
