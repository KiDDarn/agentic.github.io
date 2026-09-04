from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from core.api_integration import (
    APIEndpoint,
    APIRegistry,
    APIResponse,
    APIStackManager,
    GraphQLAPIClient,
    HTTPAPIClient,
    RateLimiter,
    WebSocketAPIClient,
)


def test_api_endpoint_and_response_defaults():
    endpoint = APIEndpoint(name="get_users", url="/users")
    assert endpoint.name == "get_users"
    assert endpoint.method == "GET"
    assert endpoint.headers == {}
    assert endpoint.timeout == 30.0

    now = datetime.now(timezone.utc)
    response = APIResponse(
        status_code=200,
        data={"ok": True},
        headers={"content-type": "application/json"},
        endpoint="get_users",
        timestamp=now,
    )
    assert response.status_code == 200
    assert response.error is None
    assert response.timestamp == now


@pytest.mark.asyncio
async def test_rate_limiter():
    limiter = RateLimiter(max_requests=2, time_window=1)
    # First 2 requests acquire immediately
    await limiter.acquire()
    await limiter.acquire()
    assert len(limiter.requests) == 2

    # Third request waits for window to elapse
    start = datetime.now(timezone.utc).timestamp()
    await limiter.acquire()
    elapsed = datetime.now(timezone.utc).timestamp() - start
    assert elapsed >= 0.5


@pytest.mark.asyncio
async def test_http_api_client_calls_and_errors():
    client = HTTPAPIClient(base_url="https://api.example.com", name="test_http", api_key="test-key-123")
    await client.initialize()

    assert "Authorization" in client.session.headers
    assert client.session.headers["Authorization"] == "Bearer test-key-123"

    endpoint = APIEndpoint(name="data", url="/data", method="POST")
    client.register_endpoint(endpoint)

    # Mock successful JSON response
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.headers = {"Content-Type": "application/json"}
    mock_resp.json.return_value = {"id": 1, "name": "sample"}

    client.session.request = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp)))

    res = await client.call_endpoint("data", data={"name": "sample"})
    assert res.status_code == 200
    assert res.data == {"id": 1, "name": "sample"}
    assert res.error is None

    # Mock error response (404)
    mock_resp.status = 404
    mock_resp.json.return_value = {"message": "Not Found"}
    res_err = await client.call_endpoint("data")
    assert res_err.status_code == 404
    assert "Not Found" in res_err.error

    # Mock network exception
    client.session.request = MagicMock(side_effect=RuntimeError("Connection refused"))
    res_net_err = await client.call_endpoint("data")
    assert res_net_err.status_code == 0
    assert "Connection refused" in res_net_err.error

    # Calling unknown endpoint
    with pytest.raises(ValueError, match="Unknown endpoint: unknown"):
        await client.call_endpoint("unknown")

    await client.cleanup()


@pytest.mark.asyncio
async def test_graphql_api_client():
    client = GraphQLAPIClient(base_url="https://api.example.com", name="test_gql", api_key="gql-key")
    await client.initialize()

    client.register_endpoint(APIEndpoint(name="graphql", url="/graphql", method="POST"))

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.headers = {}
    mock_resp.json.return_value = {"data": {"viewer": {"login": "user1"}}}

    client.session.request = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp)))

    res = await client.query("query { viewer { login } }", variables={"var": 1})
    assert res.status_code == 200
    assert res.data["data"]["viewer"]["login"] == "user1"

    await client.cleanup()


@pytest.mark.asyncio
async def test_api_registry_and_stack_manager():
    registry = APIRegistry()
    client = HTTPAPIClient(base_url="https://test", name="cli1")
    client.register_endpoint(APIEndpoint("ping", "/ping", "GET"))

    await registry.register_client(client)
    assert registry.get_client("cli1") == client

    # Mock response
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.headers = {}
    mock_resp.json.return_value = {"pong": True}
    client.session.request = MagicMock(return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_resp)))

    resp = await registry.call_api("cli1", "ping")
    assert resp.data == {"pong": True}

    with pytest.raises(ValueError, match="Unknown API client"):
        await registry.call_api("nonexistent", "ping")

    await registry.unregister_client("cli1")
    assert registry.get_client("cli1") is None

    # Test APIStackManager
    stack = APIStackManager()
    await stack.setup_common_apis()
    reg = stack.get_registry()
    assert reg.get_client("example_rest") is not None
    assert reg.get_client("github") is not None

    await stack.add_custom_api("custom", "https://custom.api", endpoints=[APIEndpoint("check", "/check")])
    assert reg.get_client("custom") is not None

    await reg.cleanup_all()
    assert len(reg.clients) == 0


@pytest.mark.asyncio
async def test_websocket_api_client_send_and_disconnect():
    ws_client = WebSocketAPIClient(url="ws://mock.example.com", name="test_ws")

    mock_ws = AsyncMock()
    ws_client.websocket = mock_ws

    await ws_client.send_message({"type": "ping"})
    mock_ws.send.assert_called_once_with('{"type": "ping"}')

    await ws_client.disconnect()
    mock_ws.close.assert_called_once()
    assert ws_client.websocket is None
