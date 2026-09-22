"""Structured (JSON) logging.

One JSON object per log line, so a single telemetry event can be traced
across acceptance / rejection / enqueue just by grepping its asset_id and
seq — see docs/PLAN.md, problem framing ("structured, traceable logs").
No external dependency: a small Formatter is all this needs.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

_RESERVED_LOG_RECORD_KEYS = set(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__.keys()
) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Fields passed via logger.info(..., extra={...}) ride along.
        for key, value in record.__dict__.items():
            if key not in _RESERVED_LOG_RECORD_KEYS:
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
