import pytest


def pytest_addoption(parser: pytest.Parser):
    parser.addini(
        "server_use_pickler",
        type="bool",
        default=True,
        help="Use pickler to deserialize UDFs on server.",
    )
    parser.addini(
        "server_has_pgvector",
        type="bool",
        default=True,
        help="pgvector is available on server.",
    )


@pytest.fixture(scope="session")
def server_use_pickler(pytestconfig: pytest.Config) -> bool:
    val: bool = pytestconfig.getini("server_use_pickler")
    return val


@pytest.fixture(scope="session")
def server_has_pgvector(pytestconfig: pytest.Config) -> bool:
    val: bool = pytestconfig.getini("server_has_pgvector")
    return val


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]):
    server_has_pgvector: bool = config.getini("server_has_pgvector")
    server_use_pickler: bool = config.getini("server_use_pickler")
    xfail_requires_pgvector = pytest.mark.xfail(reason="requires pgvector on server to run")
    xfail_requires_pickler_on_server = pytest.mark.xfail(
        reason="requires pickler (e.g. dill) on server to run"
    )
    for item in items:
        if "requires_pgvector" in item.keywords and not server_has_pgvector:
            item.add_marker(xfail_requires_pgvector)
        if "requires_pickler_on_server" in item.keywords and not server_use_pickler:
            item.add_marker(xfail_requires_pickler_on_server)
