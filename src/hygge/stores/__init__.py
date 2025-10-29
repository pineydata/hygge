"""
Collection of data stores.
"""
from .adls import ADLSStore, ADLSStoreConfig
from .mssql import MssqlStore, MssqlStoreConfig
from .onelake import OneLakeStore, OneLakeStoreConfig
from .parquet import ParquetStore, ParquetStoreConfig

__all__ = [
    "ADLSStore",
    "ADLSStoreConfig",
    "MssqlStore",
    "MssqlStoreConfig",
    "OneLakeStore",
    "OneLakeStoreConfig",
    "ParquetStore",
    "ParquetStoreConfig",
]
