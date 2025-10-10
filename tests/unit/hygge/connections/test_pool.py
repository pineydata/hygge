"""
Unit tests for ConnectionPool.
"""
import asyncio

import pytest

from hygge.connections import ConnectionPool
from hygge.connections.base import BaseConnection


class MockConnection(BaseConnection):
    """Mock connection for testing."""

    connection_count = 0

    def __init__(self, server: str, database: str, options=None):
        super().__init__(server, database, options)
        self.closed = False

    async def get_connection(self):
        """Create a mock connection."""
        MockConnection.connection_count += 1
        return f"mock_connection_{MockConnection.connection_count}"

    async def close_connection(self, conn):
        """Close a mock connection."""
        self.closed = True

    async def is_connection_alive(self, conn):
        """Check if connection is alive."""
        return not self.closed


@pytest.fixture
def mock_factory():
    """Create a mock connection factory."""
    MockConnection.connection_count = 0
    return MockConnection(server="test.database.windows.net", database="testdb")


@pytest.mark.asyncio
async def test_pool_initialization(mock_factory):
    """Test pool initializes with correct number of connections."""
    pool = ConnectionPool(
        name="test_pool", connection_factory=mock_factory, pool_size=3
    )

    await pool.initialize()

    assert pool.size == 3
    assert pool.available == 3
    assert MockConnection.connection_count == 3

    await pool.close()


@pytest.mark.asyncio
async def test_pool_acquire_release(mock_factory):
    """Test acquiring and releasing connections."""
    pool = ConnectionPool(
        name="test_pool", connection_factory=mock_factory, pool_size=2
    )

    await pool.initialize()

    # Acquire connection
    conn1 = await pool.acquire()
    assert conn1 == "mock_connection_1"
    assert pool.available == 1

    # Acquire second connection
    conn2 = await pool.acquire()
    assert conn2 == "mock_connection_2"
    assert pool.available == 0

    # Release first connection
    await pool.release(conn1)
    assert pool.available == 1

    # Release second connection
    await pool.release(conn2)
    assert pool.available == 2

    await pool.close()


@pytest.mark.asyncio
async def test_pool_acquire_blocks_when_empty(mock_factory):
    """Test that acquire blocks when pool is exhausted."""
    pool = ConnectionPool(
        name="test_pool", connection_factory=mock_factory, pool_size=1
    )

    await pool.initialize()

    # Acquire only connection
    _conn1 = await pool.acquire()
    assert pool.available == 0

    # Try to acquire with timeout - should block
    acquired = False

    async def try_acquire():
        nonlocal acquired
        await pool.acquire()
        acquired = True

    # Start acquisition in background
    task = asyncio.create_task(try_acquire())

    # Give it a moment to try
    await asyncio.sleep(0.1)
    assert not acquired  # Should still be blocked

    # Release connection
    await pool.release(_conn1)

    # Wait for acquisition to complete
    await asyncio.wait_for(task, timeout=1.0)
    assert acquired  # Should have acquired after release

    await pool.close()


@pytest.mark.asyncio
async def test_pool_raises_error_if_not_initialized(mock_factory):
    """Test pool raises error if used before initialization."""
    pool = ConnectionPool(
        name="test_pool", connection_factory=mock_factory, pool_size=2
    )

    # Don't initialize

    with pytest.raises(RuntimeError, match="not initialized"):
        await pool.acquire()


@pytest.mark.asyncio
async def test_pool_close_releases_all_connections(mock_factory):
    """Test closing pool releases all connections."""
    pool = ConnectionPool(
        name="test_pool", connection_factory=mock_factory, pool_size=3
    )

    await pool.initialize()

    # Acquire one connection
    _conn = await pool.acquire()
    assert pool.available == 2

    # Close pool (with one connection still out)
    await pool.close()

    # Factory's close method should have been called for available connections
    assert mock_factory.closed


@pytest.mark.asyncio
async def test_pool_properties(mock_factory):
    """Test pool size and available properties."""
    pool = ConnectionPool(
        name="test_pool", connection_factory=mock_factory, pool_size=5
    )

    assert pool.size == 5
    assert pool.available == 0  # Not initialized yet

    await pool.initialize()

    assert pool.size == 5
    assert pool.available == 5

    # Acquire some connections
    conn1 = await pool.acquire()
    _conn2 = await pool.acquire()

    assert pool.size == 5  # Size doesn't change
    assert pool.available == 3  # Available decreases

    await pool.release(conn1)
    assert pool.available == 4

    await pool.close()


@pytest.mark.asyncio
async def test_pool_concurrent_access(mock_factory):
    """Test pool handles concurrent access correctly."""
    pool = ConnectionPool(
        name="test_pool", connection_factory=mock_factory, pool_size=3
    )

    await pool.initialize()

    acquired_connections = []

    async def worker(worker_id: int):
        """Simulate a worker acquiring and releasing a connection."""
        conn = await pool.acquire()
        acquired_connections.append((worker_id, conn))
        await asyncio.sleep(0.01)  # Simulate work
        await pool.release(conn)

    # Run 10 workers concurrently (more than pool size)
    tasks = [worker(i) for i in range(10)]
    await asyncio.gather(*tasks)

    # All workers should have completed
    assert len(acquired_connections) == 10

    # All connections should be back in pool
    assert pool.available == 3

    await pool.close()
