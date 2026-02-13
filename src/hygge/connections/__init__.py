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
from .constants import (
    MSSQL_BATCHING_DEFAULTS,
    MSSQL_CONNECTION_DEFAULTS,
    MSSQL_HOME_BATCHING_DEFAULTS,
    MSSQL_STORE_BATCHING_DEFAULTS,
    get_mssql_batching_defaults,
    get_mssql_defaults,
    get_mssql_home_defaults,
    get_mssql_store_defaults,
)
from .execution import ThreadPoolEngine, get_engine, register_engine
from .mssql import MssqlConnection
from .pool import ConnectionPool

__all__ = [
    "BaseConnection",
    "ConnectionPool",
    "MssqlConnection",
    "ThreadPoolEngine",
    "get_engine",
    "register_engine",
    "MSSQL_CONNECTION_DEFAULTS",
    "MSSQL_BATCHING_DEFAULTS",
    "MSSQL_HOME_BATCHING_DEFAULTS",
    "MSSQL_STORE_BATCHING_DEFAULTS",
    "get_mssql_defaults",
    "get_mssql_batching_defaults",
    "get_mssql_home_defaults",
    "get_mssql_store_defaults",
]
