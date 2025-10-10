"""
MS SQL Server home implementation.

Provides data reading from MS SQL Server databases with:
- Azure AD authentication
- Connection pooling
- Efficient batching with Polars
- Entity support for multiple tables
"""
from .home import MssqlHome, MssqlHomeConfig

__all__ = [
    "MssqlHome",
    "MssqlHomeConfig",
]
