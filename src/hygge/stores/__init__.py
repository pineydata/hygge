"""
Collection of data stores.
"""

from hygge.core.store import Store

from .adls import ADLSStore, ADLSStoreConfig
from .local import LocalStore, LocalStoreConfig
from .mssql import MssqlStore, MssqlStoreConfig
from .onelake import OneLakeStore, OneLakeStoreConfig
from .openmirroring import OpenMirroringStore, OpenMirroringStoreConfig
from .parquet import ParquetStore, ParquetStoreConfig
from .sqlite import SqliteStore, SqliteStoreConfig

# Backward compatibility: type "parquet" uses LocalStore with format=parquet
Store._registry["parquet"] = LocalStore

__all__ = [
    "ADLSStore",
    "ADLSStoreConfig",
    "LocalStore",
    "LocalStoreConfig",
    "MssqlStore",
    "MssqlStoreConfig",
    "OneLakeStore",
    "OneLakeStoreConfig",
    "OpenMirroringStore",
    "OpenMirroringStoreConfig",
    "ParquetStore",
    "ParquetStoreConfig",
    "SqliteStore",
    "SqliteStoreConfig",
]
