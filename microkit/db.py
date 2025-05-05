import asyncpg
from typing import Optional

from microkit.config import settings

__pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """
    Returns a singleton asyncpg pool.
    Usage:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("…")
    """
    global __pool
    if __pool is None:
        __pool = await asyncpg.create_pool(
            dsn=settings.postgres_dsn,
            min_size=settings.db_min_size,
            max_size=settings.db_max_size,
            command_timeout=60,
        )
    return __pool
