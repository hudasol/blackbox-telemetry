"""Unit test for the worker's retry backoff schedule — pure function, no
Redis/Postgres needed."""

from __future__ import annotations

from worker.processing import (
    BACKOFF_INITIAL_SECONDS,
    BACKOFF_MAX_SECONDS,
    next_backoff_seconds,
)


def test_first_attempt_uses_initial_delay():
    assert next_backoff_seconds(0) == BACKOFF_INITIAL_SECONDS


def test_delay_doubles_each_attempt():
    assert next_backoff_seconds(1) == BACKOFF_INITIAL_SECONDS * 2
    assert next_backoff_seconds(2) == BACKOFF_INITIAL_SECONDS * 4
    assert next_backoff_seconds(3) == BACKOFF_INITIAL_SECONDS * 8


def test_delay_is_capped():
    assert next_backoff_seconds(20) == BACKOFF_MAX_SECONDS
    assert next_backoff_seconds(1000) == BACKOFF_MAX_SECONDS


def test_delay_never_exceeds_cap_at_any_attempt():
    for attempt in range(0, 50):
        assert next_backoff_seconds(attempt) <= BACKOFF_MAX_SECONDS
