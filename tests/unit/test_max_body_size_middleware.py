"""Unit tests for app.middleware.MaxBodySizeMiddleware, exercised
directly at the ASGI level (no HTTP client, no real Redis/Postgres) so
the byte-counting safeguard can be tested independently of whatever a
real client's Content-Length header happens to say.
"""

from __future__ import annotations

from app.middleware import MaxBodySizeMiddleware


class _RecordingApp:
    """A minimal ASGI app that drains the body it's handed and echoes
    a 200 — standing in for FastAPI/Starlette without pulling in a real
    route or dependency."""

    def __init__(self) -> None:
        self.received_bodies: list[bytes] = []

    async def __call__(self, scope, receive, send) -> None:
        while True:
            message = await receive()
            self.received_bodies.append(message.get("body", b""))
            if not message.get("more_body", False):
                break
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})


def _http_scope(headers: list[tuple[bytes, bytes]] | None = None) -> dict:
    return {"type": "http", "method": "POST", "path": "/x", "headers": headers or []}


def _chunked_receive(chunks: list[bytes]):
    """An ASGI receive callable that hands back the given chunks one at
    a time as separate http.request messages, the way a real server
    delivers a streamed/chunked body."""
    remaining = list(chunks)

    async def receive():
        if not remaining:
            return {"type": "http.request", "body": b"", "more_body": False}
        chunk = remaining.pop(0)
        return {
            "type": "http.request",
            "body": chunk,
            "more_body": bool(remaining),
        }

    return receive


async def _collect_sent(app, scope, receive) -> list[dict]:
    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    await app(scope, receive, send)
    return sent


async def test_oversized_content_length_is_rejected_before_any_body_is_read():
    inner = _RecordingApp()
    middleware = MaxBodySizeMiddleware(inner, max_bytes=10)
    scope = _http_scope(headers=[(b"content-length", b"999")])

    async def receive():
        raise AssertionError("body should never be read once Content-Length exceeds the limit")

    sent = await _collect_sent(middleware, scope, receive)

    assert sent[0]["status"] == 413
    assert b"payload_too_large" in sent[1]["body"]
    assert inner.received_bodies == []  # the wrapped app was never even invoked


async def test_chunked_body_without_content_length_is_capped_by_actual_byte_count():
    # The bug this closes: nothing here declares Content-Length at all
    # (a client can omit it, or a proxy can strip it) — the only thing
    # standing between this and unbounded buffering is counting bytes
    # as they actually arrive.
    inner = _RecordingApp()
    middleware = MaxBodySizeMiddleware(inner, max_bytes=10)
    chunks = [b"12345", b"67890", b"ABCDE"]  # 15 bytes total, over the 10-byte limit
    scope = _http_scope()  # no content-length header

    sent = await _collect_sent(middleware, scope, _chunked_receive(chunks))

    assert sent[0]["status"] == 413
    assert b"payload_too_large" in sent[1]["body"]


async def test_body_within_the_limit_reaches_the_wrapped_app_unchanged():
    inner = _RecordingApp()
    middleware = MaxBodySizeMiddleware(inner, max_bytes=10)
    scope = _http_scope(headers=[(b"content-length", b"5")])

    sent = await _collect_sent(middleware, scope, _chunked_receive([b"12345"]))

    assert sent[0]["status"] == 200
    assert sent[1]["body"] == b"ok"
    assert inner.received_bodies == [b"12345"]


async def test_non_http_scope_passes_through_untouched():
    # A lifespan or websocket scope has no body to police — must not be
    # confused for an http request missing a body.
    inner = _RecordingApp()
    middleware = MaxBodySizeMiddleware(inner, max_bytes=10)

    class _NonHttpApp:
        called = False

        async def __call__(self, scope, receive, send):
            self.called = True

    passthrough = _NonHttpApp()
    middleware.app = passthrough

    async def receive():
        return {"type": "lifespan.startup"}

    async def send(message):
        pass

    await middleware({"type": "lifespan"}, receive, send)
    assert passthrough.called
