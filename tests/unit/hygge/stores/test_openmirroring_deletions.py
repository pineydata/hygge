"""
Unit tests for OpenMirroringStore deletion detection module.

Tests the deletions.py module functions for query-based deletion detection.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from hygge.core.home import Home
from hygge.stores.openmirroring.deletions import (
    cleanup_temp_keys_file,
    find_deletions,
    find_deletions_batched,
    find_source_keys,
    stage_target_keys_in_tmp,
)
from hygge.utility.exceptions import StoreError


class MockOpenMirroringStore:
    """Mock OpenMirroringStore for testing deletions module."""

    def __init__(self):
        self.key_columns = ["id"]
        self.entity_name = "users"
        self.compression = "snappy"
        self.config = MagicMock()
        self.config.deletion_source = {
            "server": "test.database.windows.net",
            "database": "testdb",
            "schema": "dbo",
            "table": "users",
        }
        self.logger = MagicMock()
        self._deletion_metrics = {
            "column_based_deletions": 0,
            "query_based_deletions": 0,
        }

    def get_staging_directory(self):
        """Mock staging directory."""
        from pathlib import Path

        return Path("/tmp/staging")

    def _get_adls_ops(self):
        """Mock ADLS operations."""
        adls_ops = MagicMock()
        adls_ops.upload_bytes = AsyncMock(
            return_value="/tmp/staging/_deletion_check_keys.parquet"
        )
        adls_ops.read_file_bytes = AsyncMock(return_value=b"parquet_data")
        adls_ops.delete_file = AsyncMock()
        return adls_ops

    async def write(self, df):
        """Mock write method."""
        pass

    async def _flush_buffer(self):
        """Mock flush buffer."""
        pass


class TestFindDeletions:
    """Test find_deletions() orchestration function."""

    @pytest.mark.asyncio
    async def test_find_deletions_raises_error_when_key_columns_missing(self):
        """Test that find_deletions() raises error when key_columns is not set."""
        store = MockOpenMirroringStore()
        store.key_columns = None

        with pytest.raises(StoreError, match="key_columns is required"):
            await find_deletions(store, None)

    @pytest.mark.asyncio
    async def test_find_deletions_skips_when_no_source_keys(self):
        """Test that find_deletions() skips when no source keys found."""
        store = MockOpenMirroringStore()

        with patch(
            "hygge.stores.openmirroring.deletions.stage_target_keys_in_tmp",
            new_callable=AsyncMock,
        ) as mock_stage, patch(
            "hygge.stores.openmirroring.deletions.find_source_keys",
            new_callable=AsyncMock,
        ) as mock_find_source:
            mock_stage.return_value = "/tmp/staging/keys.parquet"
            mock_find_source.return_value = None  # No source keys

            await find_deletions(store, None)

            # Should not raise error, just skip
            assert mock_stage.called

    @pytest.mark.asyncio
    async def test_find_deletions_writes_delete_markers(self):
        """Test that find_deletions() writes delete markers for deleted rows."""
        store = MockOpenMirroringStore()

        # Target has keys [1, 2, 3], source has keys [1, 2]
        # So key 3 should be marked for deletion
        target_keys = pl.DataFrame({"id": [1, 2, 3]})
        source_keys = pl.DataFrame({"id": [1, 2]})

        with patch(
            "hygge.stores.openmirroring.deletions.stage_target_keys_in_tmp",
            new_callable=AsyncMock,
        ) as mock_stage, patch(
            "hygge.stores.openmirroring.deletions.find_source_keys",
            new_callable=AsyncMock,
        ) as mock_find_source, patch(
            "hygge.stores.openmirroring.deletions.find_deletions_batched",
            new_callable=AsyncMock,
        ) as mock_find_batched, patch(
            "polars.read_parquet"
        ) as mock_read_parquet, patch.object(
            store, "write", new_callable=AsyncMock
        ) as mock_write:
            mock_stage.return_value = "/tmp/staging/keys.parquet"
            mock_find_source.return_value = source_keys
            mock_read_parquet.return_value = target_keys
            mock_find_batched.return_value = pl.DataFrame({"id": [3]})

            await find_deletions(store, None)

            # Verify delete markers were written
            assert mock_write.called
            write_df = mock_write.call_args[0][0]
            assert "__rowMarker__" in write_df.columns
            assert all(write_df["__rowMarker__"] == 2)  # Delete marker
            assert store._deletion_metrics["query_based_deletions"] == 1


class TestFindDeletionsBatched:
    """Test find_deletions_batched() function."""

    @pytest.mark.asyncio
    async def test_find_deletions_batched_small_table(self):
        """Test batched anti-join for small tables (< 100K keys)."""
        store = MockOpenMirroringStore()

        target_keys = pl.DataFrame({"id": [1, 2, 3, 4, 5]})
        source_keys = pl.DataFrame({"id": [1, 2, 3]})

        result = await find_deletions_batched(store, target_keys, source_keys)

        # Should find keys 4 and 5 as deletions
        assert len(result) == 2
        assert set(result["id"].to_list()) == {4, 5}

    @pytest.mark.asyncio
    async def test_find_deletions_batched_large_table(self):
        """Test batched anti-join for large tables (>= 100K keys)."""
        store = MockOpenMirroringStore()

        # Create large datasets
        target_keys = pl.DataFrame({"id": range(200_000)})
        source_keys = pl.DataFrame({"id": range(150_000)})  # Missing last 50K

        result = await find_deletions_batched(store, target_keys, source_keys)

        # Should find 50K deletions
        assert len(result) == 50_000
        assert result["id"].min() == 150_000
        assert result["id"].max() == 199_999

    @pytest.mark.asyncio
    async def test_find_deletions_batched_no_deletions(self):
        """Test batched anti-join when no deletions found."""
        store = MockOpenMirroringStore()

        target_keys = pl.DataFrame({"id": [1, 2, 3]})
        source_keys = pl.DataFrame(
            {"id": [1, 2, 3, 4, 5]}
        )  # Source has all target keys

        result = await find_deletions_batched(store, target_keys, source_keys)

        # Should return empty DataFrame with correct schema
        assert len(result) == 0
        assert list(result.columns) == ["id"]


class TestFindSourceKeys:
    """Test find_source_keys() function."""

    @pytest.mark.asyncio
    async def test_find_source_keys_raises_error_when_home_is_none(self):
        """Test that find_source_keys() raises StoreError when home is None."""
        from hygge.utility.exceptions import StoreError

        store = MockOpenMirroringStore()

        with pytest.raises(StoreError, match="Home not set"):
            await find_source_keys(store, None, ["id"])

    @pytest.mark.asyncio
    async def test_find_source_keys_raises_error_when_home_not_supported(self):
        """Test find_source_keys() raises StoreError when Home doesn't support key finding."""  # noqa: E501
        from hygge.utility.exceptions import StoreError

        store = MockOpenMirroringStore()

        class UnsupportedHome(Home, home_type="unsupported"):
            async def _get_batches(self):
                yield None

        home = UnsupportedHome("test", {})
        with pytest.raises(StoreError, match="does not support find_keys"):
            await find_source_keys(store, home, ["id"])

    @pytest.mark.asyncio
    async def test_find_source_keys_calls_home_find_keys(self):
        """Test that find_source_keys() calls home.find_keys()."""
        store = MockOpenMirroringStore()

        class SupportedHome(Home, home_type="supported"):
            async def _get_batches(self):
                yield None

            async def find_keys(self, key_columns):
                return pl.DataFrame({"id": [1, 2, 3]})

            def supports_key_finding(self):
                return True

        home = SupportedHome("test", {})
        result = await find_source_keys(store, home, ["id"])

        assert result is not None
        assert len(result) == 3
        assert list(result["id"]) == [1, 2, 3]


class TestStageTargetKeysInTmp:
    """Test stage_target_keys_in_tmp() function."""

    @pytest.mark.asyncio
    async def test_stage_target_keys_raises_error_when_deletion_source_missing(self):
        """Test stage_target_keys_in_tmp() raises error when deletion_source not configured."""  # noqa: E501
        store = MockOpenMirroringStore()
        store.config.deletion_source = None

        with pytest.raises(
            StoreError, match="deletion_source configuration is required"
        ):
            await stage_target_keys_in_tmp(store)

    @pytest.mark.asyncio
    async def test_stage_target_keys_raises_error_when_server_database_missing(self):
        """Test stage_target_keys_in_tmp() raises error when server/database missing."""
        store = MockOpenMirroringStore()
        store.config.deletion_source = {"server": "test.server"}  # Missing database

        with pytest.raises(StoreError, match="must contain 'server' and 'database'"):
            await stage_target_keys_in_tmp(store)

    @pytest.mark.asyncio
    async def test_stage_target_keys_stages_keys_to_tmp(self):
        """Test that stage_target_keys_in_tmp() stages keys to _tmp folder."""
        store = MockOpenMirroringStore()

        target_keys = pl.DataFrame({"id": [1, 2, 3]})

        with patch(
            "hygge.stores.openmirroring.deletions.MssqlHome"
        ) as mock_mssql_home_class:
            mock_home = MagicMock()
            mock_home.find_keys = AsyncMock(return_value=target_keys)
            mock_mssql_home_class.return_value = mock_home

            result = await stage_target_keys_in_tmp(store)

            # Verify MssqlHome was created with correct config
            assert mock_mssql_home_class.called
            config = mock_mssql_home_class.call_args[0][1]
            assert config.server == "test.database.windows.net"
            assert config.database == "testdb"

            # Verify keys were staged
            assert result is not None
            assert "_deletion_check_keys.parquet" in result


class TestCleanupTempKeysFile:
    """Test cleanup_temp_keys_file() function."""

    @pytest.mark.asyncio
    async def test_cleanup_temp_keys_file_deletes_file(self):
        """Test that cleanup_temp_keys_file() deletes temp file."""
        store = MockOpenMirroringStore()
        keys_path = "/tmp/staging/_deletion_check_keys.parquet"

        # Mock _get_adls_ops to return a persistent mock
        adls_ops = MagicMock()
        adls_ops.delete_file = AsyncMock()
        store._get_adls_ops = MagicMock(return_value=adls_ops)

        await cleanup_temp_keys_file(store, keys_path)

        # Verify delete_file was called
        adls_ops.delete_file.assert_called_once_with(keys_path)

    @pytest.mark.asyncio
    async def test_cleanup_temp_keys_file_handles_errors_gracefully(self):
        """Test that cleanup_temp_keys_file() handles errors gracefully."""
        store = MockOpenMirroringStore()
        keys_path = "/tmp/staging/_deletion_check_keys.parquet"

        # Set up ADLS ops with error before calling function
        adls_ops = MagicMock()
        adls_ops.delete_file = AsyncMock(side_effect=Exception("Delete failed"))
        store._get_adls_ops = MagicMock(return_value=adls_ops)

        # Should not raise error, just log warning
        await cleanup_temp_keys_file(store, keys_path)

        # Verify warning was logged
        assert store.logger.warning.called
