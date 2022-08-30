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
