import pytest


def pytest_addoption(parser: pytest.Parser):
    parser.addini(
        "server_use_pickler",
        type="bool",
        default=True,
        help="Use pickler to deserialize UDFs on server.",
    )


@pytest.fixture(scope="session")
def server_use_pickler(pytestconfig: pytest.Config) -> bool:
    val: bool = pytestconfig.getini("server_use_pickler")
    return val
