"""Inbound/outbound resource and serialization boundaries before data mutation."""

import json

import httpx
import pytest
import pytest_asyncio

from signaldesk.app import create_app
from signaldesk.config import Settings
from signaldesk.http_limits import ResponseBodyError, read_bounded_body


class BodyStream(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.reads = 0
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            self.reads += 1
            yield chunk

    async def aclose(self):
        self.closed = True


@pytest.mark.parametrize(
    "headers,code",
    [
        ({"content-length": "1025"}, "response_too_large"),
        ({"content-length": "9" * 5000}, "response_too_large"),
        ({"content-length": "-1"}, "invalid_content_length"),
        ({"content-length": "10,10"}, "invalid_content_length"),
        ({"content-encoding": "gzip"}, "unsupported_content_encoding"),
        ({"content-encoding": "br"}, "unsupported_content_encoding"),
        ({"content-encoding": "identity, gzip"}, "unsupported_content_encoding"),
    ],
)
async def test_outbound_rejects_before_any_response_bytes_are_read(headers, code):
    body = BodyStream([b"private-response"])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, headers=headers, stream=body))
    ) as client:
        async with client.stream("GET", "https://fixture.example") as response:
            with pytest.raises(ResponseBodyError) as caught:
                await read_bounded_body(response, 1024)
            assert caught.value.code == code
        assert body.reads == 0 and body.closed


@pytest.mark.parametrize("headers", [{}, {"content-length": "1"}])
async def test_outbound_counts_actual_chunks_and_closes_after_overflow(headers):
    body = BodyStream([b"a" * 600, b"b" * 425, b"must-not-be-read"])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, headers=headers, stream=body))
    ) as client:
        with pytest.raises(ResponseBodyError, match="response_too_large"):
            async with client.stream("GET", "https://fixture.example") as response:
                await read_bounded_body(response, 1024)
    assert body.reads == 2 and body.closed


async def test_outbound_supports_exact_boundary_and_already_loaded_injected_responses():
    body = BodyStream([b"a" * 500, b"b" * 524])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, stream=body))
    ) as client:
        async with client.stream("GET", "https://fixture.example") as response:
            assert await read_bounded_body(response, 1024) == b"a" * 500 + b"b" * 524
    assert body.closed
    assert await read_bounded_body(httpx.Response(200, content=b"ok"), 1024) == b"ok"
    with pytest.raises(ResponseBodyError):
        await read_bounded_body(httpx.Response(200, content=b"a" * 1025), 1024)


@pytest_asyncio.fixture
async def api(tmp_path):
    settings = Settings(
        _env_file=None,
        database_url=f"sqlite+aiosqlite:///{tmp_path}/api.db",
        checkpoint_url=f"sqlite:///{tmp_path}/cp.db",
        memory_url=f"sqlite:///{tmp_path}/memory.db",
        model="",
        openai_api_key="",
        enable_a2a=True,
    )
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://fixture",
        ) as client:
            yield client


@pytest.mark.parametrize(
    "body",
    [
        {"name": "\ud800", "sources": [{"name": "Source", "url": "https://example.com"}]},
        {"name": "Normal", "sources": [{"name": "\udfff", "url": "https://example.com"}]},
        {
            "name": "Normal",
            "sources": [{"name": "Source", "url": "https://example.com"}],
            "extra": {"\ud800": "value"},
        },
        {"name": float("nan"), "sources": [{"name": "Source", "url": "https://example.com"}]},
    ],
)
async def test_bad_unicode_and_nonfinite_json_are_422_before_project_writes(api, body):
    response = await api.post(
        "/api/projects", content=json.dumps(body), headers={"Content-Type": "application/json"}
    )
    assert response.status_code == 422 and response.json()["code"] == "invalid_json_body"
    assert (await api.get("/api/projects")).json() == []


@pytest.mark.parametrize(
    "content",
    [
        b'{"value":1e999}',
        b'{"value":-Infinity}',
        b'{"value":',
        b'{"value":"\xff"}',
    ],
    ids=["overflow", "infinity", "truncated", "invalid-utf8"],
)
async def test_malformed_json_is_bounded_serializable_and_never_echoes_request(api, content):
    response = await api.put(
        "/api/projects/missing/memory/key",
        content=content,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422 and response.json()["code"] == "invalid_json_body"
    assert "1e999" not in response.text


async def test_deep_valid_json_receives_serializable_validation_error(api):
    # Decoder recursion thresholds differ by Python version. A deeply nested
    # array is valid JSON, but it is not the object accepted by this endpoint.
    response = await api.put(
        "/api/projects/missing/memory/key",
        content=b"[" * 1200 + b"0" + b"]" * 1200,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert "detail" in response.json()


async def test_normal_unicode_pairs_and_literal_escapes_remain_valid(api):
    for name in ("中文 😀", r"literal \ud800"):
        response = await api.post(
            "/api/projects",
            json={"name": name, "sources": [{"name": "来源", "url": "https://example.com"}]},
        )
        assert response.status_code == 201 and response.json()["name"] == name


async def test_framework_validation_does_not_serialize_raw_binary_or_private_input(api):
    response = await api.post(
        "/api/projects",
        content=b"private-fixture\xff",
        headers={"Content-Type": "application/octet-stream"},
    )
    assert response.status_code == 422
    errors = response.json()["detail"]
    assert errors and all(set(e) == {"loc", "msg", "type"} for e in errors)
    assert "private-fixture" not in response.text


async def test_a2a_malformed_json_keeps_its_protocol_error_path(api):
    response = await api.post(
        "/a2a/", content=b'{"jsonrpc":', headers={"Content-Type": "application/json"}
    )
    assert response.status_code < 500
    assert response.json() == {
        "jsonrpc": "2.0",
        "id": None,
        "error": {"code": -32700, "message": "Parse error"},
    }


@pytest.mark.parametrize(
    "body",
    [
        {"jsonrpc": "2.0", "id": "\ud800", "method": "UnknownMethod", "params": {}},
        {"jsonrpc": "2.0", "id": 1, "method": "UnknownMethod", "params": {"\udfff": 1}},
        {"jsonrpc": "2.0", "id": float("nan"), "method": "UnknownMethod", "params": {}},
    ],
    ids=["unsafe-id", "unsafe-params-key", "nonfinite-id"],
)
@pytest.mark.parametrize("content_type", ["application/json", "application/octet-stream"])
async def test_a2a_unsafe_values_use_null_id_protocol_error(api, body, content_type):
    response = await api.post(
        "/a2a/", content=json.dumps(body), headers={"Content-Type": content_type}
    )
    assert response.status_code == 200
    assert response.json() == {
        "jsonrpc": "2.0",
        "id": None,
        "error": {"code": -32600, "message": "Invalid Request"},
    }
