"""
Unit tests for database connection utilities.

Tests the asyncpg connection pool management:
- get_db_pool() singleton pattern
- close_db_pool() cleanup
- check_db_health() health checks
"""

import pytest
import asyncpg
from unittest.mock import AsyncMock, MagicMock, patch

from shared.database import get_db_pool, close_db_pool, check_db_health


@pytest.fixture(autouse=True)
def reset_pool():
    """Reset the global connection pool before each test."""
    import shared.database
    shared.database._db_pool = None
    yield
    shared.database._db_pool = None


class TestGetDbPool:
    """Tests for get_db_pool() function."""

    @pytest.mark.asyncio
    async def test_creates_pool_on_first_call(self):
        """Test that get_db_pool creates a new pool on first call."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)

        with patch('asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool) as mock_create:
            pool = await get_db_pool()

            assert pool is mock_pool
            mock_create.assert_called_once()
            # Verify connection parameters
            call_args = mock_create.call_args
            assert 'min_size' in call_args.kwargs
            assert 'max_size' in call_args.kwargs
            assert 'command_timeout' in call_args.kwargs

    @pytest.mark.asyncio
    async def test_returns_same_pool_on_subsequent_calls(self):
        """Test singleton pattern - same pool returned on multiple calls."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)

        with patch('asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool) as mock_create:
            pool1 = await get_db_pool()
            pool2 = await get_db_pool()
            pool3 = await get_db_pool()

            assert pool1 is pool2
            assert pool2 is pool3
            mock_create.assert_called_once()  # Only created once

    @pytest.mark.asyncio
    async def test_uses_environment_variables(self):
        """Test that pool configuration comes from environment variables."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)

        env_vars = {
            'DATABASE_URL': 'postgresql://testuser:testpass@testhost:5432/testdb',
            'DB_POOL_MIN_SIZE': '10',
            'DB_POOL_MAX_SIZE': '50'
        }

        with patch('asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool) as mock_create, \
             patch.dict('os.environ', env_vars):

            await get_db_pool()

            call_args = mock_create.call_args
            assert call_args.args[0] == 'postgresql://testuser:testpass@testhost:5432/testdb'
            assert call_args.kwargs['min_size'] == 10
            assert call_args.kwargs['max_size'] == 50

    @pytest.mark.asyncio
    async def test_uses_default_values_when_env_missing(self):
        """Test default values when environment variables are not set."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)

        with patch('asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool) as mock_create, \
             patch.dict('os.environ', {}, clear=True):

            await get_db_pool()

            call_args = mock_create.call_args
            # Check default DATABASE_URL is used
            assert 'postgresql://' in call_args.args[0]
            # Check default pool sizes
            assert call_args.kwargs['min_size'] == 5
            assert call_args.kwargs['max_size'] == 20

    @pytest.mark.asyncio
    async def test_sets_command_timeout(self):
        """Test that command timeout is configured."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)

        with patch('asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool) as mock_create:
            await get_db_pool()

            call_args = mock_create.call_args
            assert call_args.kwargs['command_timeout'] == 60


class TestCloseDbPool:
    """Tests for close_db_pool() function."""

    @pytest.mark.asyncio
    async def test_closes_existing_pool(self):
        """Test that close_db_pool closes an active pool."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)

        with patch('asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            # Create a pool first
            await get_db_pool()

            # Close it
            await close_db_pool()

            mock_pool.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_handles_no_pool_gracefully(self):
        """Test that closing when no pool exists doesn't error."""
        # Should not raise an exception
        await close_db_pool()

    @pytest.mark.asyncio
    async def test_allows_recreation_after_close(self):
        """Test that pool can be recreated after closing."""
        mock_pool1 = AsyncMock(spec=asyncpg.Pool)
        mock_pool2 = AsyncMock(spec=asyncpg.Pool)

        async def create_pool_side_effect(*args, **kwargs):
            # Return the next pool in sequence
            if not hasattr(create_pool_side_effect, 'call_count'):
                create_pool_side_effect.call_count = 0
            create_pool_side_effect.call_count += 1
            return mock_pool1 if create_pool_side_effect.call_count == 1 else mock_pool2

        with patch('asyncpg.create_pool', side_effect=create_pool_side_effect):
            # Create and close first pool
            pool1 = await get_db_pool()
            await close_db_pool()

            # Create second pool
            pool2 = await get_db_pool()

            assert pool1 is mock_pool1
            assert pool2 is mock_pool2
            assert pool1 is not pool2

    @pytest.mark.asyncio
    async def test_sets_pool_to_none_after_close(self):
        """Test that global pool variable is reset to None."""
        import shared.database

        mock_pool = AsyncMock(spec=asyncpg.Pool)

        with patch('asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            await get_db_pool()
            assert shared.database._db_pool is not None

            await close_db_pool()
            assert shared.database._db_pool is None


class TestCheckDbHealth:
    """Tests for check_db_health() function."""

    @pytest.mark.asyncio
    async def test_returns_true_when_query_succeeds(self):
        """Test health check returns True when database is accessible."""
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=1)

        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        with patch('asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            result = await check_db_health()

            assert result is True
            mock_conn.fetchval.assert_called_once_with("SELECT 1")

    @pytest.mark.asyncio
    async def test_returns_false_on_connection_error(self):
        """Test health check returns False when connection fails."""
        async def failing_create_pool(*args, **kwargs):
            raise Exception("Connection failed")

        with patch('asyncpg.create_pool', side_effect=failing_create_pool):
            result = await check_db_health()

            assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_on_query_error(self):
        """Test health check returns False when query fails."""
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(side_effect=asyncpg.PostgresError("Query failed"))

        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        with patch('asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            result = await check_db_health()

            assert result is False

    @pytest.mark.asyncio
    async def test_executes_simple_query(self):
        """Test that health check uses SELECT 1 query."""
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=1)

        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        with patch('asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            await check_db_health()

            # Verify the exact query used
            mock_conn.fetchval.assert_called_once_with("SELECT 1")


class TestConnectionPoolIntegration:
    """Integration-style tests for connection pool behavior."""

    @pytest.mark.asyncio
    async def test_pool_lifecycle(self):
        """Test complete pool lifecycle: create, use, close."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        with patch('asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool):
            # Create pool
            pool = await get_db_pool()
            assert pool is not None

            # Use pool (simulated)
            async with pool.acquire() as conn:
                assert conn is mock_conn

            # Close pool
            await close_db_pool()
            mock_pool.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_acquisitions_use_same_pool(self):
        """Test that multiple connection acquisitions use the same pool."""
        mock_pool = AsyncMock(spec=asyncpg.Pool)

        with patch('asyncpg.create_pool', new_callable=AsyncMock, return_value=mock_pool) as mock_create:
            pool1 = await get_db_pool()
            pool2 = await get_db_pool()
            pool3 = await get_db_pool()

            # All should be the same pool object
            assert pool1 is pool2 is pool3
            # Pool created only once
            assert mock_create.call_count == 1
