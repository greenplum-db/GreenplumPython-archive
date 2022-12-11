from os import environ

import pytest

import greenplumpython as gp


@pytest.fixture()
def db():
    # for the connection both work for GitHub Actions and concourse
    db = gp.database(
        host="35.241.130.38",
        dbname="dev",
        user="gpadmin",
        password="79kq97lCaWrFn",
    )
    yield db
    db.close()
