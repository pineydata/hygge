"""
Tests for LocalStore and LocalStoreConfig (type: local, format).
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from hygge.core.store import Store
from hygge.stores import LocalStore, LocalStoreConfig


class TestLocalStoreConfig:
    def test_default_format_parquet(self):
        config = LocalStoreConfig(path="/data")
        assert config.format == "parquet"

    def test_format_validation(self):
        with pytest.raises(ValueError, match="Format must be one of"):
            LocalStoreConfig(path="/data", format="xml")


class TestLocalStoreWrite:
    @pytest.mark.asyncio
    async def test_write_parquet_creates_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = LocalStoreConfig(path=tmp, format="parquet")
            store = LocalStore("test", config)
            df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
            await store.write(df)
            await store.finish()
            files = list(Path(tmp).glob("*.parquet"))
            assert len(files) == 1
            back = pl.read_parquet(files[0])
            assert back.equals(df)

    @pytest.mark.asyncio
    async def test_write_csv_creates_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = LocalStoreConfig(path=tmp, format="csv")
            store = LocalStore("test", config)
            df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
            await store.write(df)
            await store.finish()
            files = list(Path(tmp).glob("*.csv"))
            assert len(files) == 1
            back = pl.read_csv(files[0])
            assert back.equals(df)


class TestParquetAliasUsesLocalStore:
    """type: parquet in config should create LocalStore (backward compat)."""

    def test_store_create_parquet_config_returns_local_store(self):
        from hygge.stores import ParquetStoreConfig

        with tempfile.TemporaryDirectory() as tmp:
            config = ParquetStoreConfig(path=tmp)
            store = Store.create("flow_store", config)
            assert type(store).__name__ == "LocalStore"
            assert store._format == "parquet"
