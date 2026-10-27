import socket

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--storage-integration",
        action="store_true",
        default=False,
        help="Use explicit runtime credentials; creates isolated test resources",
    )


@pytest.fixture(autouse=True)
def offline_environment(monkeypatch, request):
    """No ambient credentials and no outbound sockets in the test suite."""
    if request.node.get_closest_marker("storage"):
        if not request.config.getoption("--storage-integration"):
            pytest.skip("Storage integration requires --storage-integration in private runtime")
        return
    monkeypatch.setattr("os.environ", {})

    def denied(*args, **kwargs):
        raise AssertionError("Network forbidden in offline tests")

    monkeypatch.setattr(socket.socket, "connect", denied)
    monkeypatch.setattr(socket.socket, "connect_ex", denied)
    monkeypatch.setattr(socket, "create_connection", denied)
