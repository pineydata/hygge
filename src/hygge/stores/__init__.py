"""
Collection of data stores.
"""
from .adls import ADLSStore, ADLSStoreConfig
from .mssql import MssqlStore, MssqlStoreConfig
from .onelake import OneLakeStore, OneLakeStoreConfig
from .openmirroring import OpenMirroringStore, OpenMirroringStoreConfig
from .parquet import ParquetStore, ParquetStoreConfig
from .sqlite import SqliteStore, SqliteStoreConfig

__all__ = [
    "ADLSStore",
    "ADLSStoreConfig",
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
