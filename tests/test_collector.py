import socket

import httpx
import pytest

from signaldesk.collector import CollectionError, extract_text, fetch_page, validate_public_url
from signaldesk.config import Settings


def test_extract_text_removes_scripts_and_boilerplate():
    title, body = extract_text(
        "<title>Pricing</title><nav>Menu</nav><script>bad()</script><main><h1>Pro</h1><p>$39 per month</p></main>",
        1000,
    )
    assert title == "Pricing"
    assert "bad" not in body and "Menu" not in body
    assert "$39 per month" in body


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/admin",
        "http://127.0.0.1",
        "https://x.local/",
        "file:///etc/passwd",
        "https://user:pass@site.com",
        "https://site.com:8443",
    ],
)
async def test_reject_non_public_targets(url, monkeypatch):
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *args: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))],
    )
    with pytest.raises(CollectionError):
        await validate_public_url(url)


async def test_redirect_is_checked_before_next_request():
    visited = []

    async def validator(url):
        if "127.0.0.1" in url:
            raise CollectionError("blocked")

    def response(request):
        visited.append(str(request.url))
        return httpx.Response(302, headers={"Location": "http://127.0.0.1/private"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        with pytest.raises(CollectionError, match="blocked"):
            await fetch_page(
                "https://public.test/", Settings(_env_file=None), client=client, validator=validator
            )
    assert visited == ["https://public.test/"]


async def test_reject_oversized_response():
    async def allow(url):
        return None

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers={"Content-Type": "text/html"},
                content=b"a" * 2000,
            )
        )
    ) as client:
        with pytest.raises(CollectionError, match="大小限制"):
            await fetch_page(
                "https://public.test/",
                Settings(_env_file=None, max_source_bytes=1000),
                client=client,
                validator=allow,
            )
