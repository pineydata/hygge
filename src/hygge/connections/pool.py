"""
Connection pool implementation using asyncio.Queue.

Provides efficient connection reuse across multiple concurrent
operations to avoid connection overhead and hitting database
connection limits.
"""
import asyncio
from typing import Any, Optional

from hygge.utility.logger import get_logger

from .base import BaseConnection


class ConnectionPool:
    """
    Async connection pool using asyncio.Queue for simple, efficient pooling.

    Pre-creates N connections at startup and manages their lifecycle.
    Connections are reused across multiple operations to reduce overhead.

    Design:
    - Uses asyncio.Queue for async-friendly blocking when pool is exhausted
    - Simple queue-based approach (no complex health checking in v0.1.x)
    - Connections are validated on acquire (basic check)
    - Health monitoring can be added in future versions

    Example:
        ```python
        pool = ConnectionPool(
            name="salesforce_db",
            connection_factory=MssqlConnection(...),
            pool_size=8
        )
        await pool.initialize()

        # Use connection
        conn = await pool.acquire()
        try:
            # Do work with connection
            ...
        finally:
            await pool.release(conn)

        # Cleanup
        await pool.close()
        ```
    """

    def __init__(
        self,
        name: str,
        connection_factory: BaseConnection,
        pool_size: int = 5,
    ):
        """
        Initialize connection pool.

        Args:
            name: Name of this pool (for logging)
            connection_factory: Factory to create connections
            pool_size: Number of connections to pre-create
        """
        self.name = name
        self.connection_factory = connection_factory
        self.pool_size = pool_size

        # Connection queue (populated during initialize())
        self._connections: Optional[asyncio.Queue] = None

        # Track pool state
        self._initialized = False
        self._closed = False

        # Logging
        self.logger = get_logger(f"hygge.connections.pool.{name}")

    async def initialize(self) -> None:
        """
        Initialize the pool by pre-creating connections.

        Must be called before using the pool. Creates pool_size
        connections and adds them to the queue.
        """
        if self._initialized:
            self.logger.warning(f"Pool {self.name} already initialized")
            return

        self.logger.info(
            f"Initializing connection pool '{self.name}' "
            f"with {self.pool_size} connections"
        )

        # Create queue
        self._connections = asyncio.Queue(maxsize=self.pool_size)

        # Pre-create connections
        for i in range(self.pool_size):
            try:
                conn = await self.connection_factory.get_connection()
                await self._connections.put(conn)
                self.logger.debug(f"Created connection {i+1}/{self.pool_size}")
            except Exception as e:
                self.logger.error(f"Failed to create connection {i+1}: {str(e)}")
                # Clean up any connections we did create
                await self._cleanup_partial_initialization()
                raise

        self._initialized = True
        self.logger.success(
            f"Pool '{self.name}' initialized with {self.pool_size} connections"
        )

    async def acquire(self) -> Any:
        """
        Acquire a connection from the pool.

        Blocks if no connections are available until one is released.
        Validates connection health before returning.

        Returns:
            Database connection object

        Raises:
            RuntimeError: If pool is not initialized or is closed
        """
        if not self._initialized:
            raise RuntimeError(
                f"Pool {self.name} not initialized. Call initialize() first."
            )

        if self._closed:
            raise RuntimeError(f"Pool {self.name} is closed")

        # Get connection from queue (blocks if empty)
        conn = await self._connections.get()

        # Basic health check - recreate if dead
        try:
            is_alive = await self.connection_factory.is_connection_alive(conn)
            if not is_alive:
                self.logger.warning("Connection from pool was dead, creating new one")
                # Close dead connection
                try:
                    await self.connection_factory.close_connection(conn)
                except Exception:
                    pass  # Already dead, ignore
                # Create new connection
                conn = await self.connection_factory.get_connection()
        except Exception as e:
            # If health check fails, try to create new connection
            self.logger.warning(
                f"Health check failed: {str(e)}, creating new connection"
            )
            try:
                await self.connection_factory.close_connection(conn)
            except Exception:
                pass
            conn = await self.connection_factory.get_connection()

        self.logger.debug(
            f"Acquired connection from pool '{self.name}' "
            f"(queue size: {self._connections.qsize()})"
        )
        return conn

    async def release(self, conn: Any) -> None:
        """
        Release a connection back to the pool.

        Args:
            conn: Connection to release

        Raises:
            RuntimeError: If pool is not initialized or is closed
        """
        if not self._initialized:
            raise RuntimeError(f"Pool {self.name} not initialized")

        if self._closed:
            # Pool closed, just close the connection
            try:
                await self.connection_factory.close_connection(conn)
            except Exception as e:
                self.logger.warning(
                    f"Error closing connection from closed pool: {str(e)}"
                )
            return

        # Return connection to queue
        await self._connections.put(conn)
        self.logger.debug(
            f"Released connection to pool '{self.name}' "
            f"(queue size: {self._connections.qsize()})"
        )

    async def close(self) -> None:
        """
        Close all connections in the pool and shut down.

        Should be called during cleanup to properly release resources.
        """
        if self._closed:
            return

        self.logger.info(f"Closing connection pool '{self.name}'")
        self._closed = True

        if not self._initialized or self._connections is None:
            return

        # Close all connections in the queue
        closed_count = 0
        while not self._connections.empty():
            try:
                conn = self._connections.get_nowait()
                await self.connection_factory.close_connection(conn)
                closed_count += 1
            except asyncio.QueueEmpty:
                break
            except Exception as e:
                self.logger.warning(f"Error closing connection: {str(e)}")

        self.logger.success(f"Pool '{self.name}' closed ({closed_count} connections)")

    async def _cleanup_partial_initialization(self) -> None:
        """Clean up connections if initialization fails partway through."""
        if self._connections is None:
            return

        while not self._connections.empty():
            try:
                conn = self._connections.get_nowait()
                await self.connection_factory.close_connection(conn)
            except Exception:
                pass  # Best effort cleanup

    @property
    def size(self) -> int:
        """Get the configured pool size."""
        return self.pool_size

    @property
    def available(self) -> int:
        """Get the number of available connections in the pool."""
        if self._connections is None:
            return 0
        return self._connections.qsize()
