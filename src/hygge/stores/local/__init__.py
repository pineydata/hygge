"""
Local file store: writes to local paths using the format layer (parquet, csv, ndjson).
"""

from .store import LocalStore, LocalStoreConfig

__all__ = ["LocalStore", "LocalStoreConfig"]
