import pytest

import greenplumpython as gp


@pytest.fixture
def db() -> gp.Database:
    db = gp.database(host="localhost", dbname="gpadmin")
    yield db
    db.close()


def test_plain_func(db: gp.Database):
    version = gp.function("version", db)
    for row in version().to_table().fetch():
        assert "Greenplum" in row["version"]


def test_set_returning_func(db: gp.Database):
    generate_series = gp.function("generate_series", db)
    results = generate_series(0, 9, as_name="id").to_table().fetch()
    assert sorted([row["id"] for row in results]) == list(range(10))
