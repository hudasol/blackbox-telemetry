"""Shared fixtures for integration tests: a real Postgres pool and a real
Redis connection, both pointed at throwaway test instances — see
docs/PROCESS_LOG.md for why these are real services, not mocks.
"""

from __future__ import annotations

import os

import asyncpg
import pytest_asyncio
from redis.asyncio import Redis
from worker.db import SCHEMA_SQL

POSTGRES_TEST_DSN = os.environ.get(
    "POSTGRES_TEST_DSN", "postgresql://blackbox:blackbox@localhost:5432/blackbox_test"
)
REDIS_TEST_URL = os.environ.get("REDIS_TEST_URL", "redis://localhost:16379/0")


@pytest_asyncio.fixture()
async def pg_pool():
    pool = await asyncpg.create_pool(POSTGRES_TEST_DSN, min_size=1, max_size=5)
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
        await conn.execute("TRUNCATE telemetry_events, asset_state")
    yield pool
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE telemetry_events, asset_state")
    await pool.close()


@pytest_asyncio.fixture()
async def redis_client():
    client = Redis.from_url(REDIS_TEST_URL, decode_responses=True)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()
