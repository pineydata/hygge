"""
Connection management for hygge.

Provides connection pooling and database-specific connection factories
for efficient data movement from SQL sources.

Key components:
- BaseConnection: Interface for database connection factories
- ConnectionPool: Async connection pool using asyncio.Queue
- MssqlConnection: MS SQL Server connection factory (Azure AD support)
"""
from .base import BaseConnection
from .constants import get_mssql_batching_defaults, get_mssql_defaults
from .mssql import MssqlConnection
from .pool import ConnectionPool

__all__ = [
    "BaseConnection",
    "ConnectionPool",
    "MssqlConnection",
    "get_mssql_defaults",
    "get_mssql_batching_defaults",
]
