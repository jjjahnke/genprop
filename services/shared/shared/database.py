"""
Database connection utilities using asyncpg.

This module provides:
- Connection pool management with singleton pattern
- Database helper functions
- Connection lifecycle management
"""

import asyncpg
import os
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Global connection pool (singleton)
_db_pool: Optional[asyncpg.Pool] = None


async def get_db_pool() -> asyncpg.Pool:
    """
    Get or create the asyncpg connection pool.

    Uses a singleton pattern to ensure only one pool exists per process.
    The pool is automatically created on first access with configuration
    from environment variables.

    Returns:
        asyncpg.Pool: The database connection pool

    Example:
        ```python
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            result = await conn.fetchrow("SELECT * FROM import_batches LIMIT 1")
        ```
    """
    global _db_pool

    if _db_pool is None:
        database_url = os.getenv(
            "DATABASE_URL",
            "postgresql://realestate:devpassword@localhost:5432/realestate"
        )

        min_size = int(os.getenv("DB_POOL_MIN_SIZE", "5"))
        max_size = int(os.getenv("DB_POOL_MAX_SIZE", "20"))

        logger.info(f"Creating database connection pool (min={min_size}, max={max_size})")

        _db_pool = await asyncpg.create_pool(
            database_url,
            min_size=min_size,
            max_size=max_size,
            command_timeout=60,
        )

        logger.info("Database connection pool created successfully")

    return _db_pool


async def close_db_pool() -> None:
    """
    Close the database connection pool.

    Should be called on application shutdown to gracefully close all
    database connections.

    Example:
        ```python
        # In FastAPI shutdown event
        @app.on_event("shutdown")
        async def shutdown():
            await close_db_pool()
        ```
    """
    global _db_pool

    if _db_pool is not None:
        logger.info("Closing database connection pool")
        await _db_pool.close()
        _db_pool = None
        logger.info("Database connection pool closed")


async def check_db_health() -> bool:
    """
    Check if the database connection is healthy.

    Attempts a simple query to verify the database is reachable and responding.

    Returns:
        bool: True if database is healthy, False otherwise

    Example:
        ```python
        if await check_db_health():
            print("Database is healthy")
        else:
            print("Database is unhealthy")
        ```
    """
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False


__all__ = [
    "get_db_pool",
    "close_db_pool",
    "check_db_health",
]
