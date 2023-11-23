from os import environ

import pytest

import greenplumpython as gp


# NOTE: This UDF must **not** depend on picklers, such as dill.
@gp.create_function
def pip_install(requirements: str) -> str:
    import subprocess as sp
    import sys

    assert sys.executable, "Python executable is required to install packages."
    cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--requirement",
        "/dev/stdin",
    ]
    try:
        output = sp.check_output(cmd, text=True, stderr=sp.STDOUT, input=requirements)
        return output
    except sp.CalledProcessError as e:
        raise Exception(e.stdout)


@pytest.fixture(scope="session")
def db(server_use_pickler: bool, server_has_pgvector: bool):
    # for the connection both work for GitHub Actions and concourse
    db = gp.database(
        params={
            "host": environ.get("PGHOST", "localhost"),
            "dbname": environ.get("TESTDB", environ.get("USER")),
            "user": environ.get("PGUSER", environ.get("USER")),
            "password": environ.get("PGPASSWORD"),
        }
    )
    db._execute(
        """
        CREATE EXTENSION IF NOT EXISTS plpython3u;
        DROP SCHEMA IF EXISTS test CASCADE;
        CREATE SCHEMA test;
        """
        + ("CREATE EXTENSION IF NOT EXISTS vector;" if server_has_pgvector else ""),
        has_results=False,
    )
    if server_use_pickler:
        print(db.apply(lambda: pip_install("dill==0.3.6")))
    yield db
    db.close()


@pytest.fixture()
def con():
    host = "localhost"
    dbname = environ.get("TESTDB", environ.get("USER"))
    con = f"postgresql://{host}/{dbname}"
    yield con


gp.config.print_sql = True
