"""Bound identity response bytes before HTTPX's automatic decoding or JSON parsing."""

import httpx


class ResponseBodyError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


async def read_bounded_body(response: httpx.Response, max_bytes: int) -> bytes:
    """Caller owns the stream and deadline; reject unsupported encoding before reading.

    Real network callers must use client.stream(), not eager get()/post(). An
    already-consumed injected response can only be checked after its owner loaded
    it; this branch never invokes a decoder or copies an oversized body.
    """
    if max_bytes < 1:
        raise ValueError("max_bytes must be positive")
    encoding = response.headers.get("content-encoding", "identity")
    if any(part.strip().lower() != "identity" for part in encoding.split(",")):
        raise ResponseBodyError("unsupported_content_encoding")
    declared = response.headers.get("content-length")
    if declared is not None:
        declared = declared.strip()
        if not declared.isascii() or not declared.isdecimal():
            raise ResponseBodyError("invalid_content_length")
        significant = declared.lstrip("0") or "0"
        if len(significant) > len(str(max_bytes)) or int(significant) > max_bytes:
            raise ResponseBodyError("response_too_large")
    if response.is_stream_consumed:
        content = response.content
        if len(content) > max_bytes:
            raise ResponseBodyError("response_too_large")
        return content
    body = bytearray()
    async for chunk in response.aiter_raw():
        if len(body) + len(chunk) > max_bytes:
            raise ResponseBodyError("response_too_large")
        body.extend(chunk)
    return bytes(body)
