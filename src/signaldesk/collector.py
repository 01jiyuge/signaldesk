"""Bounded public-page collection. Demo fixtures never use the network."""

import asyncio
import codecs
import hashlib
import ipaddress
import socket
from collections.abc import Awaitable, Callable
from urllib.parse import urljoin, urlsplit

import httpx
from bs4 import BeautifulSoup

from signaldesk.config import Settings
from signaldesk.errors import PublicTaskError
from signaldesk.http_limits import ResponseBodyError, read_bounded_body


class CollectionError(PublicTaskError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "collection_failed",
        retryable: bool = True,
        suggestion: str = "检查来源是否公开可访问；修改来源后请新建任务。",
    ):
        super().__init__(message, code=code, retryable=retryable, suggestion=suggestion)


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


async def validate_public_url(url: str) -> None:
    """Reject private targets on initial requests AND every redirect.

    Deployment must additionally enforce outbound network policy to protect against
    DNS rebinding; application-layer DNS checks alone are not an egress firewall.
    """
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise CollectionError("仅支持 HTTP(S) 公网来源。")
    try:
        port = parts.port
    except ValueError as exc:
        raise CollectionError("来源端口无效。", code="invalid_source", retryable=False) from exc
    if parts.username or parts.password or port not in (None, 80, 443):
        raise CollectionError("来源不可包含凭据或非标准端口。")
    hostname = parts.hostname.rstrip(".").lower()
    if hostname in {"localhost", "localhost.localdomain"} or hostname.endswith(
        (".local", ".internal", ".localhost", ".example")
    ):
        raise CollectionError("真实模式仅允许可解析的公网来源；.example 仅用于演示。")
    try:
        addresses = await asyncio.to_thread(socket.getaddrinfo, hostname, parts.port or 443)
    except OSError as exc:
        raise CollectionError("来源域名无法解析。", code="source_dns_failed") from exc
    if not addresses:
        raise CollectionError("来源域名未返回地址。")
    for address in addresses:
        ip = ipaddress.ip_address(address[4][0])
        if not ip.is_global or getattr(ip, "ipv4_mapped", None):
            raise CollectionError(
                "来源解析到非公网地址，已拒绝访问。",
                code="source_address_blocked",
                retryable=False,
                suggestion="检查域名和代理 DNS。若使用 fake-IP 模式，请改用真实公网解析后新建任务；保留内网访问限制。",
            )


def extract_text(
    html: str | bytes, max_chars: int, *, encoding: str | None = None
) -> tuple[str, str]:
    # Keep bytes intact until the HTML parser can inspect its BOM and meta tags.
    soup = BeautifulSoup(
        html, "html.parser", from_encoding=encoding if isinstance(html, bytes) else None
    )
    title = soup.title.get_text(" ", strip=True) if soup.title else "Untitled source"
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "svg"]):
        tag.decompose()
    lines = [" ".join(line.split()) for line in soup.get_text("\n").splitlines()]
    text = "\n".join(line for line in lines if line)[:max_chars]
    if not text:
        raise CollectionError("来源未包含可读取的正文；动态页面需后续浏览器采集器。")
    return title[:300], text


def _http_charset(response: httpx.Response) -> str | None:
    declared = response.charset_encoding
    if declared is None:
        return None
    try:
        codec = codecs.lookup(declared)
        if not codec._is_text_encoding:
            raise LookupError("not a text encoding")
        codec.decode(b"", "replace")
    except (LookupError, ValueError) as exc:
        raise CollectionError(
            "来源声明了无效或不支持的文本字符编码。", code="source_invalid_charset", retryable=False
        ) from exc
    return codec.name


def _body_error(exc: ResponseBodyError) -> CollectionError:
    messages = {
        "response_too_large": ("来源超过单页大小限制。", "source_too_large"),
        "unsupported_content_encoding": (
            "来源未遵守未压缩响应请求；目前不支持压缩来源。",
            "source_unsupported_content_encoding",
        ),
        "invalid_content_length": ("来源声明了无效的正文长度。", "source_invalid_content_length"),
    }
    message, code = messages[exc.code]
    return CollectionError(message, code=code, retryable=False)


async def fetch_page(
    url: str,
    settings: Settings,
    *,
    client: httpx.AsyncClient | None = None,
    validator: Callable[[str], Awaitable[None]] = validate_public_url,
) -> tuple[str, str]:
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(
            timeout=settings.request_timeout_seconds,
            follow_redirects=False,
            trust_env=False,
            headers={"User-Agent": "SignalDesk/0.1 (competitive-intelligence research)"},
        )
    try:
        # HTTPX's per-operation timeouts do not bound a slow drip, validation or
        # multiple redirects. One deadline covers the complete collection flow.
        async with asyncio.timeout(settings.request_timeout_seconds):
            for _ in range(5):
                await validator(url)
                async with client.stream(
                    "GET", url, headers={"Accept-Encoding": "identity"}, follow_redirects=False
                ) as response:
                    if response.status_code in {301, 302, 303, 307, 308}:
                        location = response.headers.get("location")
                        if not location:
                            raise CollectionError("来源重定向缺少目标地址。")
                        url = urljoin(url, location)
                        continue
                    if response.status_code != 200:
                        raise CollectionError(
                            f"来源返回 HTTP {response.status_code}。",
                            code="source_http_error",
                            retryable=response.status_code in {408, 429}
                            or response.status_code >= 500,
                        )
                    media_type = (
                        response.headers.get("content-type", "").split(";")[0].strip().lower()
                    )
                    if media_type not in {"text/html", "application/xhtml+xml", "text/plain"}:
                        raise CollectionError(
                            "目前仅支持 HTML 或纯文本来源。",
                            code="unsupported_source",
                            retryable=False,
                        )
                    charset = _http_charset(response)
                    try:
                        data = await read_bounded_body(response, settings.max_source_bytes)
                    except ResponseBodyError as exc:
                        raise _body_error(exc) from exc
                    if media_type == "text/plain":
                        try:
                            value = data.decode(charset or "utf-8-sig", errors="replace").strip()
                        except (LookupError, UnicodeError) as exc:
                            raise CollectionError(
                                "来源文本字符编码无法解码。",
                                code="source_invalid_charset",
                                retryable=False,
                            ) from exc
                        value = value[: settings.max_source_chars]
                        if not value:
                            raise CollectionError("来源正文为空。")
                        return urlsplit(url).hostname or "Source", value
                    return extract_text(data, settings.max_source_chars, encoding=charset)
            raise CollectionError("来源重定向次数超过限制。")
    except (TimeoutError, httpx.TimeoutException) as exc:
        raise CollectionError("来源采集超过时间限制，请稍后重试。", code="source_timeout") from exc
    except httpx.HTTPError as exc:
        raise CollectionError("来源访问失败，请稍后重试。", code="source_access_failed") from exc
    finally:
        if owns_client:
            await client.aclose()


def demo_page(url: str, revision: int) -> tuple[str, str]:
    parts = urlsplit(url)
    if parts.hostname == "northstar.example" and parts.path.rstrip("/") == "/pricing":
        if revision == 1:
            return "Northstar Pricing（演示夹具 v1）", (
                "Northstar Pro\nPrice: $29 / month\nSeats: 5\nFeatures: shared workspace, CSV export"
            )
        return "Northstar Pricing（演示夹具 v2）", (
            "Northstar Pro\nPrice: $39 / month\nSeats: 10\nFeatures: shared workspace, CSV export, API access"
        )
    if parts.hostname == "orbit.example" and parts.path.rstrip("/") == "/changelog":
        if revision == 1:
            return "Orbit Changelog（演示夹具 v1）", (
                "Orbit product updates\n2026-10-01: Added CSV export.\nSSO: unavailable"
            )
        return "Orbit Changelog（演示夹具 v2）", (
            "Orbit product updates\n2026-10-01: Added CSV export.\n2026-10-15: Added SSO for Enterprise.\nSSO: Enterprise only"
        )
    raise CollectionError(
        "演示模式仅支持 Northstar /pricing 和 Orbit /changelog 示例来源。",
        code="unsupported_demo_source",
        retryable=False,
        suggestion="使用一键演示项目，或为公开真实来源配置模型并选择真实模式。",
    )
