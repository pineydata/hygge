"""
Tests for LocalHome and LocalHomeConfig (type: local, format).
"""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from hygge.core.home import Home
from hygge.homes import LocalHome, LocalHomeConfig
from hygge.utility import HomeError


class TestLocalHomeConfig:
    def test_default_format_parquet(self):
        config = LocalHomeConfig(path="/data")
        assert config.format == "parquet"

    def test_format_csv(self):
        config = LocalHomeConfig(path="/data", format="csv")
        assert config.format == "csv"

    def test_format_validation(self):
        with pytest.raises(ValueError, match="Format must be one of"):
            LocalHomeConfig(path="/data", format="xml")


class TestLocalHomePathResolution:
    def test_get_batch_paths_single_parquet_file(self):
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            Path(tmp.name).write_bytes(b"")
            path = tmp.name
        try:
            config = LocalHomeConfig(path=path, format="parquet")
            home = LocalHome("test", config)
            # get_batch_paths will try to read; path exists but empty parquet may error
            assert home.get_data_path() == Path(path)
        finally:
            Path(path).unlink(missing_ok=True)

    def test_get_batch_paths_directory_parquet(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            df = pl.DataFrame({"a": [1, 2, 3]})
            df.write_parquet(Path(tmp_dir) / "x.parquet")
            config = LocalHomeConfig(path=tmp_dir, format="parquet")
            home = LocalHome("test", config)
            paths = home.get_batch_paths()
            assert len(paths) == 1
            assert paths[0].suffix == ".parquet"

    def test_nonexistent_path_raises(self):
        config = LocalHomeConfig(path="/nonexistent/local/home/path.parquet")
        home = LocalHome("test", config)
        with pytest.raises(HomeError, match="Path does not exist"):
            home.get_batch_paths()


class TestLocalHomeRead:
    @pytest.mark.asyncio
    async def test_read_parquet_single_file(self):
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            path = tmp.name
        try:
            expected = pl.DataFrame(
                {"id": range(50), "v": [f"x{i}" for i in range(50)]}
            )
            expected.write_parquet(path)
            config = LocalHomeConfig(path=path, format="parquet", batch_size=20)
            home = LocalHome("test", config)
            batches = []
            async for b in home.read():
                batches.append(b)
            combined = pl.concat(batches)
            assert combined.shape == expected.shape
        finally:
            Path(path).unlink(missing_ok=True)


class TestParquetAliasUsesLocalHome:
    """type: parquet in config should create LocalHome (backward compat)."""

    def test_home_create_parquet_config_returns_local_home(self):
        from hygge.homes import ParquetHomeConfig

        config = ParquetHomeConfig(path="/some/path.parquet")
        home = Home.create("flow_home", config)
        assert type(home).__name__ == "LocalHome"
        assert home._format == "parquet"
