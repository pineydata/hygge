"""
A cozy, comfortable data movement framework.
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
from .stores import ParquetStore, ParquetStoreConfig  # noqa: F401

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
