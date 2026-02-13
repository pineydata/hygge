"""
hygge - A cozy, comfortable data movement framework.

hygge (pronounced "hoo-ga") makes data movement feel natural and reliable.
It follows the philosophy of comfort over complexity, convention over
configuration, and reliability over speed.

hygge is built on Polars + PyArrow for efficient data movement, with a
simple, intuitive API that makes it easy to move data from any source
(Home) to any destination (Store) with confidence.
"""

from .core import Coordinator, Flow, Home, HomeConfig, Store, StoreConfig

# IMPORTANT: Registry Initialization
# All home and store implementations must be imported here to register themselves
# with the registry pattern. This ensures they are available when hygge is imported.
#
# When adding new home/store implementations:
# 1. Create your implementation (e.g., SqlHome, S3Store)
# 2. Add the import below to register it
# 3. The registry pattern will automatically make it available via
#    Home.create() and Store.create()
#
# Example:
#   from .homes.sql import SqlHome, SqlHomeConfig  # noqa: F401
#   from .stores.s3 import S3Store, S3StoreConfig  # noqa: F401
# Import implementations to register them
from .homes import ParquetHome, ParquetHomeConfig  # noqa: F401
from .homes.mssql import MssqlHome, MssqlHomeConfig  # noqa: F401
from .stores import (  # noqa: F401
    ADLSStore,
    ADLSStoreConfig,
    MssqlStore,
    MssqlStoreConfig,
    OneLakeStore,
    OneLakeStoreConfig,
    ParquetStore,
    ParquetStoreConfig,
    SqliteStore,
    SqliteStoreConfig,
)

__all__ = [
    # Core components
    "Flow",
    "Home",
    "Store",
    "Coordinator",
    # Config classes
    "HomeConfig",
    "StoreConfig",
]
