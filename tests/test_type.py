import pytest

import greenplumpython as gp
from greenplumpython import type


@pytest.fixture
def db():
    db = gp.database(host="localhost", dbname="gpadmin")
    yield db
    db.close()


def test_type_create(db: gp.Database):
    class Person:
        _first_name: str
        _last_name: str

    type.create_type(Person, "Person", db)
    with pytest.raises(Exception) as exc_info:
        type.create_type(Person, "Person", db)
    assert 'type "person" already exists\n' in str(exc_info.value)


def test_type_drop(db: gp.Database):
    class Person:
        _first_name: str
        _last_name: str

    type.create_type(Person, "Person", db)
    type.drop_type("Person", db)
    type.create_type(Person, "Person", db)
