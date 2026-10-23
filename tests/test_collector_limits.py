import asyncio
import gzip
from types import SimpleNamespace

import httpx
import pytest

from signaldesk.collector import CollectionError, fetch_page


class TrackingStream(httpx.AsyncByteStream):
    def __init__(self, chunks=(), *, delay=0, wait=False):
        self.chunks = chunks
        self.delay = delay
        self.wait = wait
        self.started = asyncio.Event()
        self.closed = False
        self.reads = 0

    async def __aiter__(self):
        self.started.set()
        if self.wait:
            await asyncio.Event().wait()
        for chunk in self.chunks:
            if self.delay:
                await asyncio.sleep(self.delay)
            self.reads += 1
            yield chunk

    async def aclose(self):
        self.closed = True


def limits(*, timeout=1, max_bytes=1000):
    return SimpleNamespace(
        request_timeout_seconds=timeout,
        max_source_bytes=max_bytes,
        max_source_chars=1000,
    )


async def allow(_url):
    pass


async def test_compressed_stream_is_rejected_before_read_or_decode(monkeypatch):
    from httpx._decoders import GZipDecoder

    payload = gzip.compress(b"x" * 500_000)
    assert len(payload) < 1000
    stream = TrackingStream([payload])

    def unexpected_decode(*args):
        pytest.fail("a compressed source must be rejected before decoding")

    monkeypatch.setattr(GZipDecoder, "decode", unexpected_decode)
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(
            200,
            headers={"Content-Type": "text/plain", "Content-Encoding": "gzip"},
            stream=stream,
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), headers={"Accept-Encoding": "gzip, br"}
    ) as client:
        with pytest.raises(CollectionError) as error:
            await fetch_page("https://public.test/", limits(), client=client, validator=allow)
        assert not client.is_closed
    assert error.value.code == "source_unsupported_content_encoding"
    assert not error.value.retryable
    assert requests[0].headers["accept-encoding"] == "identity"
    assert stream.reads == 0
    assert stream.closed


@pytest.mark.parametrize(
    ("header", "chunks", "code", "reads"),
    [
        ("1001", [b"ignored"], "source_too_large", 0),
        ("-1", [b"ignored"], "source_invalid_content_length", 0),
        ("one", [b"ignored"], "source_invalid_content_length", 0),
        ("1, 1", [b"ignored"], "source_invalid_content_length", 0),
        (None, [b"x" * 700, b"x" * 400, b"unread"], "source_too_large", 2),
        ("1", [b"x" * 1001, b"unread"], "source_too_large", 1),
    ],
)
async def test_header_and_actual_raw_body_are_bounded(header, chunks, code, reads):
    stream = TrackingStream(chunks)
    headers = {"Content-Type": "text/plain"}
    if header is not None:
        headers["Content-Length"] = header
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, headers=headers, stream=stream))
    ) as client:
        with pytest.raises(CollectionError) as error:
            await fetch_page("https://public.test/", limits(), client=client, validator=allow)
    assert error.value.code == code
    assert stream.reads == reads
    assert stream.closed


@pytest.mark.parametrize(
    ("content_type", "body", "title", "text"),
    [
        (
            "Text/HTML; charset=UTF-8",
            "<title>价格</title><p>套餐39元</p>".encode(),
            "价格",
            "套餐39元",
        ),
        (
            "text/html",
            '<meta charset="gb18030"><title>价格</title><p>套餐39元</p>'.encode("gb18030"),
            "价格",
            "套餐39元",
        ),
        (
            "text/html",
            "<title>价格</title><p>套餐39元</p>".encode("utf-16"),
            "价格",
            "套餐39元",
        ),
        (
            'application/xhtml+xml; charset="gb18030"',
            "<title>价格</title><p>套餐39元</p>".encode("gb18030"),
            "价格",
            "套餐39元",
        ),
        (
            "Text/Plain; charset=gb18030",
            "套餐39元".encode("gb18030"),
            "public.test",
            "套餐39元",
        ),
        ("text/plain", b"\xef\xbb\xbfplain text", "public.test", "plain text"),
    ],
)
async def test_html_bom_meta_http_charset_and_case_insensitive_mime(
    content_type, body, title, text
):
    stream = TrackingStream([body])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, headers={"Content-Type": content_type}, stream=stream)
        )
    ) as client:
        actual_title, actual_text = await fetch_page(
            "https://public.test/", limits(), client=client, validator=allow
        )
    assert actual_title == title
    assert text in actual_text
    assert stream.closed


@pytest.mark.parametrize(
    "charset", ["unknown-charset", "base64_codec", "hex_codec", "rot_13", "idna"]
)
@pytest.mark.parametrize("media_type", ["text/html", "text/plain"])
async def test_invalid_http_charsets_fail_with_stable_public_error(charset, media_type):
    stream = TrackingStream([b"<p>body</p>"])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200, headers={"Content-Type": f"{media_type}; charset={charset}"}, stream=stream
            )
        )
    ) as client:
        with pytest.raises(CollectionError) as error:
            await fetch_page("https://public.test/", limits(), client=client, validator=allow)
    assert error.value.code == "source_invalid_charset"
    assert not error.value.retryable
    assert charset not in str(error.value)
    assert stream.reads == 0
    assert stream.closed


async def test_slow_body_has_one_deadline_and_closes_response():
    stream = TrackingStream([b"x"] * 100, delay=0.01)
    loop = asyncio.get_running_loop()
    started = loop.time()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, headers={"Content-Type": "text/plain"}, stream=stream)
        )
    ) as client:
        with pytest.raises(CollectionError) as error:
            await fetch_page(
                "https://public.test/", limits(timeout=0.07), client=client, validator=allow
            )
        assert not client.is_closed
    assert error.value.code == "source_timeout"
    assert error.value.retryable
    assert stream.started.is_set()
    assert stream.reads < 100
    assert loop.time() - started < 0.7
    assert stream.closed


async def test_validation_and_redirects_share_one_deadline():
    visited = []
    validated = []
    streams = []

    async def validator(url):
        validated.append(url)
        await asyncio.sleep(0.02)

    async def respond(request):
        visited.append(str(request.url))
        await asyncio.sleep(0.02)
        stream = TrackingStream()
        streams.append(stream)
        return httpx.Response(302, headers={"Location": f"/hop/{len(visited)}"}, stream=stream)

    started = asyncio.get_running_loop().time()
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(CollectionError) as error:
            await fetch_page(
                "https://public.test/", limits(timeout=0.09), client=client, validator=validator
            )
    assert error.value.code == "source_timeout"
    assert 1 <= len(visited) < 5
    assert len(visited) <= len(validated) < 5
    assert asyncio.get_running_loop().time() - started < 0.7
    assert all(stream.closed for stream in streams)


async def test_deadline_includes_validation_before_first_request():
    called = False

    async def validator(_):
        await asyncio.Event().wait()

    def respond(_):
        nonlocal called
        called = True
        pytest.fail("validation did not finish")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(CollectionError) as error:
            await fetch_page(
                "https://public.test/", limits(timeout=0.02), client=client, validator=validator
            )
    assert error.value.code == "source_timeout"
    assert not called


async def test_injected_client_cannot_follow_redirect_past_validator():
    visited = []

    async def validator(url):
        if "127.0.0.1" in url:
            raise CollectionError("blocked", code="source_address_blocked")

    def respond(request):
        visited.append(str(request.url))
        return httpx.Response(302, headers={"Location": "http://127.0.0.1/private"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), follow_redirects=True
    ) as client:
        with pytest.raises(CollectionError) as error:
            await fetch_page("https://public.test/", limits(), client=client, validator=validator)
    assert error.value.code == "source_address_blocked"
    assert visited == ["https://public.test/"]


@pytest.mark.parametrize("owns_client", [False, True])
async def test_external_cancellation_propagates_and_closes_owned_resources(
    monkeypatch, owns_client
):
    stream = TrackingStream(wait=True)
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, headers={"Content-Type": "text/plain"}, stream=stream)
        )
    )
    if owns_client:
        monkeypatch.setattr("signaldesk.collector.httpx.AsyncClient", lambda **_: client)
    task = asyncio.create_task(
        fetch_page(
            "https://public.test/",
            limits(),
            client=None if owns_client else client,
            validator=allow,
        )
    )
    try:
        await asyncio.wait_for(stream.started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed
        assert client.is_closed is owns_client
    finally:
        await client.aclose()


async def test_httpx_timeout_has_same_safe_timeout_code():
    def respond(_):
        raise httpx.ReadTimeout("private transport details")

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(CollectionError) as error:
            await fetch_page("https://public.test/", limits(), client=client, validator=allow)
    assert error.value.code == "source_timeout"
    assert "private" not in str(error.value)
