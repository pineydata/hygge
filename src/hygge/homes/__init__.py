"""
Collection of data homes.
"""

from hygge.core.home import Home

from .local import LocalHome, LocalHomeConfig
from .mssql import MssqlHome, MssqlHomeConfig
from .parquet import ParquetHome, ParquetHomeConfig

# Backward compatibility: type "parquet" uses LocalHome with format=parquet
Home._registry["parquet"] = LocalHome

__all__ = [
    "LocalHome",
    "LocalHomeConfig",
    "ParquetHome",
    "ParquetHomeConfig",
    "MssqlHome",
    "MssqlHomeConfig",
]
