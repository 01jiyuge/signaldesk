"""Bound request wire bytes before API frameworks parse or validate JSON.

These limits are independent of the decoded Sandbox code/input limits. Only
non-streaming write endpoints are buffered; SSE, other reads and ASGI lifespan
messages retain their original receive/send behavior.
"""

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


class RequestBodyLimitMiddleware:
    def __init__(self, app: ASGIApp, *, max_bytes: int):
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = scope.get("path", "")
        protected = any(
            path == prefix or path.startswith(prefix + "/") for prefix in ("/api", "/a2a")
        )
        if (
            scope["type"] != "http"
            or scope.get("method") not in {"POST", "PUT", "PATCH"}
            or not protected
        ):
            await self.app(scope, receive, send)
            return

        async def reject(status: int, code: str, detail: str):
            await JSONResponse({"detail": detail, "code": code}, status_code=status)(
                scope, receive, send
            )

        lengths = set()
        for name, value in scope.get("headers", []):
            name = name.lower()
            if name == b"content-encoding":
                if any(encoding.strip().lower() != b"identity" for encoding in value.split(b",")):
                    await reject(
                        415,
                        "unsupported_content_encoding",
                        "请求仅支持 identity Content-Encoding。",
                    )
                    return
            elif name == b"content-length":
                value = value.strip()
                if not value.isdigit():
                    await reject(400, "invalid_content_length", "Content-Length 格式无效。")
                    return
                # Avoid parsing an unbounded integer supplied as a header.
                significant = value.lstrip(b"0") or b"0"
                if len(significant) > len(str(self.max_bytes)) or int(significant) > self.max_bytes:
                    await reject(413, "request_body_too_large", "请求体超过本实例的字节上限。")
                    return
                lengths.add(int(significant))
        if len(lengths) > 1:
            await reject(400, "invalid_content_length", "Content-Length 不可冲突。")
            return

        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            if len(body) + len(chunk) > self.max_bytes:
                await reject(413, "request_body_too_large", "请求体超过本实例的字节上限。")
                return
            body.extend(chunk)
            if not message.get("more_body", False):
                break

        # A bounded pre-read gives a reliable 413 before inner exception handlers
        # or JSON parsers run, including requests with no/truncated Content-Length.
        buffered = bytes(body)
        body.clear()
        replayed = False

        async def replay():
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": buffered, "more_body": False}
            return await receive()

        await self.app(scope, replay, send)
