import json

import httpx
import pytest
from fastapi import Depends, FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse

from signaldesk.request_limits import RequestBodyLimitMiddleware


async def exchange(app, *, method="POST", path="/api/example", headers=(), chunks=(b"{}",)):
    messages = [
        {"type": "http.request", "body": chunk, "more_body": index < len(chunks) - 1}
        for index, chunk in enumerate(chunks)
    ]
    sent, calls = [], {"receive": 0}

    async def receive():
        calls["receive"] += 1
        return messages.pop(0) if messages else {"type": "http.disconnect"}

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": list(headers),
        "scheme": "http",
        "server": ("test", 80),
        "client": ("test", 123),
    }
    await app(scope, receive, send)
    return sent, calls


def response(sent):
    status = next(message["status"] for message in sent if message["type"] == "http.response.start")
    body = b"".join(
        message.get("body", b"") for message in sent if message["type"] == "http.response.body"
    )
    return status, json.loads(body)


async def test_declared_size_rejected_without_reading_or_parsing_body():
    called = False

    async def inner(scope, receive, send):
        nonlocal called
        called = True

    limited = RequestBodyLimitMiddleware(inner, max_bytes=1024)
    sent, calls = await exchange(limited, headers=[(b"content-length", b"1025")])
    assert response(sent) == (
        413,
        {"detail": "请求体超过本实例的字节上限。", "code": "request_body_too_large"},
    )
    assert calls["receive"] == 0 and not called


@pytest.mark.parametrize(
    "headers", [[], [(b"content-length", b"2")], [(b"transfer-encoding", b"chunked")]]
)
async def test_actual_chunks_are_counted_when_length_absent_or_too_small(headers):
    async def forbidden(scope, receive, send):
        raise AssertionError("Oversized bodies must not reach a JSON parser")

    sent, calls = await exchange(
        RequestBodyLimitMiddleware(forbidden, max_bytes=1024),
        headers=headers,
        chunks=[b"a" * 600, b"b" * 425],
    )
    assert response(sent)[0] == 413 and calls["receive"] == 2


@pytest.mark.parametrize(
    "method,path",
    [("POST", "/api"), ("PUT", "/api/projects/one"), ("PATCH", "/a2a/"), ("POST", "/a2a/nested")],
)
async def test_utf8_wire_limit_and_bounded_replay_reach_inner_json(method, path):
    value = {"text": "情报😀", "escaped": '\\quote"\n'}
    wire = json.dumps(value, ensure_ascii=False).encode()

    async def inner(scope, receive, send):
        request = Request(scope, receive)
        await JSONResponse(await request.json())(scope, receive, send)

    sent, calls = await exchange(
        RequestBodyLimitMiddleware(inner, max_bytes=len(wire)),
        method=method,
        path=path,
        headers=[(b"content-encoding", b"IDENTITY")],
        chunks=[wire[:13], wire[13:]],
    )
    assert response(sent) == (200, value) and calls["receive"] == 2
    sent, _ = await exchange(
        RequestBodyLimitMiddleware(inner, max_bytes=len(wire) - 1),
        method=method,
        path=path,
        chunks=[wire],
    )
    assert response(sent)[0] == 413


@pytest.mark.parametrize("encoding", [b"gzip", b"br", b"identity,gzip", b""])
async def test_nonidentity_encoding_is_rejected_before_read(encoding):
    async def forbidden(scope, receive, send):
        raise AssertionError("Encoded request must not reach the inner app")

    sent, calls = await exchange(
        RequestBodyLimitMiddleware(forbidden, max_bytes=1024),
        headers=[(b"content-encoding", encoding)],
    )
    assert response(sent)[0] == 415
    assert response(sent)[1]["code"] == "unsupported_content_encoding"
    assert calls["receive"] == 0


@pytest.mark.parametrize(
    "headers",
    [
        [(b"content-length", b"-1")],
        [(b"content-length", b"x")],
        [(b"content-length", b"1"), (b"content-length", b"2")],
    ],
)
async def test_invalid_or_conflicting_length_is_a_small_400(headers):
    sent, calls = await exchange(RequestBodyLimitMiddleware(None, max_bytes=1024), headers=headers)
    assert response(sent)[0] == 400 and calls["receive"] == 0


async def test_extremely_large_declared_integer_is_rejected_without_int_conversion():
    sent, calls = await exchange(
        RequestBodyLimitMiddleware(None, max_bytes=1024),
        headers=[(b"content-length", b"9" * 5000)],
    )
    assert response(sent)[0] == 413 and calls["receive"] == 0


@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/api/runs/id/ag-ui"),
        ("HEAD", "/api/health"),
        ("OPTIONS", "/api/projects"),
        ("POST", "/outside"),
        ("POST", "/apiculture"),
    ],
)
async def test_reads_and_other_paths_pass_through_without_buffering_streams(method, path):
    async def streaming(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/event-stream")],
            }
        )
        await send({"type": "http.response.body", "body": b"data: first\n\n", "more_body": True})
        await send({"type": "http.response.body", "body": b"data: last\n\n", "more_body": False})

    sent, calls = await exchange(
        RequestBodyLimitMiddleware(streaming, max_bytes=1024),
        method=method,
        path=path,
        headers=[(b"content-length", b"999999"), (b"content-encoding", b"gzip")],
    )
    assert calls["receive"] == 0
    assert [message.get("body") for message in sent[1:]] == [b"data: first\n\n", b"data: last\n\n"]
    assert sent[1]["more_body"] is True


async def test_disconnected_partial_request_does_not_enter_inner_app():
    messages = iter(
        [{"type": "http.request", "body": b"{", "more_body": True}, {"type": "http.disconnect"}]
    )
    sent = []

    async def receive():
        return next(messages)

    async def send(message):
        sent.append(message)

    async def forbidden(scope, receive, send):
        raise AssertionError("Disconnected partial body must not reach the parser")

    await RequestBodyLimitMiddleware(forbidden, max_bytes=1024)(
        {"type": "http", "path": "/api/projects", "method": "POST", "headers": []},
        receive,
        send,
    )
    assert sent == []


async def test_middleware_precedes_fastapi_auth_and_mounted_a2a_json_parsing(monkeypatch):
    parsed, authenticated = [], []
    original_json = Request.json

    async def observed_json(self):
        parsed.append(self.url.path)
        return await original_json(self)

    monkeypatch.setattr(Request, "json", observed_json)
    app = FastAPI()

    async def authentication():
        authenticated.append(True)
        raise HTTPException(401, "fixture token required")

    @app.post("/api/projects/one/sandbox/executions", dependencies=[Depends(authentication)])
    async def sandbox(payload: dict):
        return payload

    remote = FastAPI()

    @remote.post("/")
    async def rpc(payload: dict):
        return payload

    app.mount("/a2a", remote)
    app.add_middleware(RequestBodyLimitMiddleware, max_bytes=1024)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for path in ["/api/projects/one/sandbox/executions", "/a2a/"]:
            result = await client.post(path, content=b"not json" * 200)
            assert result.status_code == 413
        assert parsed == [] and authenticated == []
        assert (await client.post("/a2a/", json={"jsonrpc": "2.0"})).json() == {"jsonrpc": "2.0"}
        assert (
            await client.post("/api/projects/one/sandbox/executions", json={})
        ).status_code == 401
    assert parsed == ["/a2a/", "/api/projects/one/sandbox/executions"]
    assert authenticated == [True]


async def test_non_http_asgi_lifespan_is_passed_through():
    observed = []

    async def inner(scope, receive, send):
        observed.append(scope["type"])
        message = await receive()
        await send({"type": message["type"] + ".complete"})

    async def receive():
        return {"type": "lifespan.startup"}

    async def send(message):
        observed.append(message["type"])

    await RequestBodyLimitMiddleware(inner, max_bytes=1024)({"type": "lifespan"}, receive, send)
    assert observed == ["lifespan", "lifespan.startup.complete"]
