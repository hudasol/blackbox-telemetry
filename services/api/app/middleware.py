"""ASGI middleware enforcing a maximum request body size.

POST /api/v1/telemetry is the one public write endpoint, and
TelemetryEvent's extra="allow" (telemetry_contract/models.py) means the
model itself has no opinion on how large or deeply-keyed a body is —
that's exactly what "allow" means. With nothing else in front of it,
FastAPI/Starlette fully buffers the request body into memory and hands
it to Pydantic before any of that validation gets a chance to reject
it, so a single oversized payload — or enough of them at once — is a
straightforward memory-exhaustion vector against the process, with no
application code ever in a position to say no.

Enforced here instead, at the ASGI layer, before Starlette builds a
Request or reads a body at all:

  - A Content-Length that already exceeds the limit is rejected
    immediately, with zero bytes of body ever read off the socket.
  - A body delivered as chunks (no Content-Length, or one the client
    can't be trusted to have set honestly) is capped by counting bytes
    as they actually arrive and aborting the moment the running total
    crosses the limit — so a client can't bypass the header check
    simply by omitting or lying about Content-Length.

This is deliberately scoped to size: a payload that's small in bytes
but pathologically deep (many thousands of nested arrays) isn't a
memory-exhaustion vector in the same way, and isn't what this guards
against.
"""

from __future__ import annotations

import json

from starlette.types import ASGIApp, Message, Receive, Scope, Send


class _BodyTooLarge(Exception):
    def __init__(self, received: int) -> None:
        self.received = received


class MaxBodySizeMiddleware:
    """Rejects any request whose body exceeds ``max_bytes`` with a
    413, before the wrapped application (FastAPI) ever sees the body.
    """

    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        content_length = _get_header(scope, b"content-length")
        if content_length is not None:
            try:
                declared = int(content_length)
            except ValueError:
                declared = None  # malformed header: fall through to the byte count below
            if declared is not None and declared > self.max_bytes:
                await _send_413(send, self.max_bytes)
                return

        received = 0

        async def limited_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise _BodyTooLarge(received)
            return message

        try:
            await self.app(scope, limited_receive, send)
        except _BodyTooLarge:
            # Raised from inside limited_receive, part way through the
            # wrapped app reading the body — before it has produced any
            # response of its own (every route here reads the full body
            # up front, none streams a response while still consuming
            # one), so no "http.response.start" has been sent yet and
            # it's still safe to send our own here.
            await _send_413(send, self.max_bytes)


def _get_header(scope: Scope, name: bytes) -> bytes | None:
    for key, value in scope.get("headers") or []:
        if key == name:
            return value
    return None


async def _send_413(send: Send, max_bytes: int) -> None:
    body = json.dumps(
        {
            "error": "payload_too_large",
            "detail": f"request body exceeds the {max_bytes}-byte limit",
        }
    ).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": body})
