"""
Base connection interface for comfortable, reliable database connections.

Defines the contract that all database-specific connection factories must
implement, ensuring consistent, reliable database access across different
database types.

Following hygge's philosophy, connections prioritize:
- **Comfort**: Simple interface, automatic connection pooling
- **Reliability**: Health checks, proper cleanup, error handling
- **Natural flow**: Connections work smoothly without manual management
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseConnection(ABC):
    """
    Abstract base class for comfortable, reliable database connection factories.

    Connection factories make it easy to work with databases by handling all
    the complexity of connection management, authentication, and configuration.
    Each database type (MSSQL, Postgres, etc.) implements this interface to
    provide its specific connection logic.

    Following hygge's philosophy, connections prioritize:
    - **Comfort**: Simple interface, automatic connection pooling
    - **Reliability**: Health checks, proper cleanup, error handling
    - **Natural flow**: Connections work smoothly without manual management

    Connection factories are responsible for creating and managing database
    connections with database-specific authentication, configuration, and
    connection string building. The Coordinator uses connection pools to
    manage connections efficiently across multiple flows.

    Example:
        ```python
        class MssqlConnection(BaseConnection):
            async def get_connection(self) -> Any:
                # MSSQL-specific connection logic with Azure AD
                ...

            async def close_connection(self, conn: Any) -> None:
                # Clean up MSSQL connection
                ...
        ```
    """

    def __init__(
        self, server: str, database: str, options: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize base connection.

        Args:
            server: Database server address
            database: Database name
            options: Additional database-specific options
        """
        self.server = server
        self.database = database
        self.options = options or {}

    @abstractmethod
    async def get_connection(self) -> Any:
        """
        Create and return a new database connection.

        This method must be implemented by subclasses to provide
        database-specific connection logic.

        Should wrap all blocking I/O operations in asyncio.to_thread()
        to prevent blocking the event loop.

        Returns:
            Database connection object (type varies by database)
        """
        pass

    @abstractmethod
    async def close_connection(self, conn: Any) -> None:
        """
        Close a database connection.

        This method must be implemented by subclasses to provide
        database-specific cleanup logic.

        Should wrap all blocking I/O operations in asyncio.to_thread()
        to prevent blocking the event loop.

        Args:
            conn: Database connection to close
        """
        pass

    async def is_connection_alive(self, conn: Any) -> bool:
        """
        Check if a connection is still alive and usable.

        Default implementation returns True. Override in subclasses
        to provide database-specific health checks.

        Args:
            conn: Database connection to check

        Returns:
            True if connection is alive, False otherwise
        """
        return True
