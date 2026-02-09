"""
Additional comprehensive tests for Open Mirroring Store to improve coverage.

Focuses on error handling, edge cases, and complex methods that are currently
under-tested. This file supplements test_openmirroring_store.py.
"""
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from hygge.stores.openmirroring import (
    OpenMirroringStore,
    OpenMirroringStoreConfig,
)
from hygge.utility.exceptions import StoreError


class TestOpenMirroringStoreErrorHandling:
    """Test error handling paths in Open Mirroring Store."""

    @pytest.mark.asyncio
    async def test_write_metadata_json_handles_directory_creation_failure(self):
        """Test metadata write handles directory creation failures gracefully."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")

        mock_adls = AsyncMock()
        mock_adls.read_json = AsyncMock(return_value=None)  # File doesn't exist
        mock_adls.file_system_client.get_file_client = MagicMock()
        mock_file_client = MagicMock()
        mock_adls.file_system_client.get_file_client.return_value = mock_file_client
        mock_adls.file_system_client.get_directory_client = MagicMock()
        mock_dir_client = MagicMock()
        mock_dir_client.exists.return_value = False
        mock_dir_client.create_directory.side_effect = Exception(
            "Directory creation failed"
        )
        mock_adls.file_system_client.get_directory_client.return_value = mock_dir_client

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            # Should handle directory creation failure gracefully
            await store._write_metadata_json()

        # Should still attempt to write (directory might already exist)
        assert store._metadata_written is True

    @pytest.mark.asyncio
    async def test_write_metadata_json_validates_key_columns_mismatch(self):
        """Test metadata write validates keyColumns mismatch."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id", "user_id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")

        # Mock existing metadata with different keyColumns
        mock_adls = AsyncMock()
        mock_adls.read_json = AsyncMock(
            return_value={"keyColumns": ["id"]}  # Different from config
        )

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            with pytest.raises(StoreError, match="different keyColumns"):
                await store._write_metadata_json()

    @pytest.mark.asyncio
    async def test_write_metadata_json_handles_tmp_path_construction_failure(self):
        """Test metadata write handles _tmp path construction failure."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        # Set base_path to something without LandingZone AFTER initialization
        # This simulates a misconfigured path (must NOT contain "LandingZone" anywhere)
        store.base_path = "invalid/path/without/anything"

        # Error should be raised at line 761-766 before _get_adls_ops() is called
        # The error check is: if self.base_path and "LandingZone" in self.base_path
        # Since base_path doesn't contain "LandingZone", it should raise
        # Mock _get_adls_ops to prevent real Azure calls if error isn't raised
        # Mock _write_schema_json to prevent it from being called
        mock_adls = AsyncMock()
        mock_adls.read_json = AsyncMock(return_value=None)
        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            with patch.object(store, "_write_schema_json", new_callable=AsyncMock):
                with pytest.raises(StoreError, match="Cannot construct _tmp path"):
                    await store._write_metadata_json(to_tmp=True)

    @pytest.mark.asyncio
    async def test_write_schema_json_handles_tmp_path_failure(self):
        """Test schema write handles _tmp path construction failure."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        # Set base_path to something without LandingZone AFTER initialization
        # This simulates a misconfigured path (must NOT contain "LandingZone" anywhere)
        store.base_path = "invalid/path/without/anything"

        # Error should be raised at line 856-859 before _get_adls_ops() is called
        # The error check is: if self.base_path and "LandingZone" in self.base_path
        # Since base_path doesn't contain "LandingZone", it should raise
        # Mock _get_adls_ops to prevent real Azure calls if error isn't raised
        mock_adls = AsyncMock()
        mock_file_client = MagicMock()
        mock_file_client.upload_data = MagicMock()
        mock_dir_client = MagicMock()
        mock_dir_client.exists.return_value = True
        mock_adls.file_system_client = MagicMock()
        mock_adls.file_system_client.get_file_client.return_value = mock_file_client
        mock_adls.file_system_client.get_directory_client.return_value = mock_dir_client
        mock_adls.timeout = 30
        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            with pytest.raises(
                StoreError, match="Cannot construct _tmp path for schema"
            ):
                await store._write_schema_json(to_tmp=True)

    @pytest.mark.asyncio
    async def test_delete_table_folder_raises_after_retries_exhausted(self):
        """Test folder deletion raises StoreError after all retry attempts fail."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.base_path = "Files/LandingZone/users"

        # Mock directory client: exists but delete always fails (lock)
        mock_dir_client = MagicMock()
        mock_dir_client.exists.return_value = True
        mock_dir_client.delete_directory.side_effect = Exception("DirectoryInUse")

        mock_fs_client = MagicMock()
        mock_fs_client.get_directory_client.return_value = mock_dir_client

        with patch.object(
            store, "_get_file_system_client", return_value=mock_fs_client
        ):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                with pytest.raises(
                    StoreError, match="Could not delete the LandingZone folder"
                ):
                    await store._delete_table_folder()

        # Should have retried 3 times
        assert mock_dir_client.delete_directory.call_count == 3

    @pytest.mark.asyncio
    async def test_delete_table_folder_succeeds_on_retry(self):
        """Test folder deletion succeeds after a transient lock clears."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            folder_deletion_wait_seconds=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.base_path = "Files/LandingZone/users"

        # First call fails (lock), second call succeeds
        mock_dir_client = MagicMock()
        mock_dir_client.exists.return_value = True
        mock_dir_client.delete_directory.side_effect = [
            Exception("ConflictError"),
            None,  # Success on second attempt
        ]

        mock_fs_client = MagicMock()
        mock_fs_client.get_directory_client.return_value = mock_dir_client

        with patch.object(
            store, "_get_file_system_client", return_value=mock_fs_client
        ):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await store._delete_table_folder()

        # Should have tried twice
        assert mock_dir_client.delete_directory.call_count == 2

    @pytest.mark.asyncio
    async def test_delete_table_folder_skips_when_not_found(self):
        """Test folder deletion handles already-deleted folder gracefully."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            folder_deletion_wait_seconds=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.base_path = "Files/LandingZone/users"

        # Directory doesn't exist
        mock_dir_client = MagicMock()
        mock_dir_client.exists.return_value = False

        mock_fs_client = MagicMock()
        mock_fs_client.get_directory_client.return_value = mock_dir_client

        with patch.object(
            store, "_get_file_system_client", return_value=mock_fs_client
        ):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await store._delete_table_folder()

        # Should not have tried to delete
        mock_dir_client.delete_directory.assert_not_called()

    @pytest.mark.asyncio
    async def test_finish_handles_move_errors_in_full_drop_mode(self):
        """Test finish() handles file move errors in full_drop mode."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.configure_for_run("full_drop")
        store.saved_paths = ["Files/_tmp/users/file1.parquet"]
        store._metadata_tmp_path = "Files/_tmp/users/_metadata.json"
        store._partner_events_tmp_path = "Files/_tmp/_partnerEvents.json"
        store._schema_tmp_path = "Files/_tmp/users/_schema.json"

        mock_adls = AsyncMock()
        mock_adls.move_file = AsyncMock(side_effect=Exception("Move failed"))

        with patch.object(store, "_delete_table_folder", new_callable=AsyncMock):
            with patch.object(store, "_get_adls_ops", return_value=mock_adls):
                with pytest.raises(
                    StoreError, match="full_drop atomic operation partially failed"
                ):
                    await store.finish()

    def test_validate_key_columns_with_polish_normalization(self):
        """Test key column validation with polish column normalization."""
        from hygge.core.polish import PolishConfig

        polish_config = PolishConfig(columns={"case": "pascal"})
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["ISTARDesignationHashId"],  # Will be normalized
            row_marker=0,
            polish=polish_config,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")

        # DataFrame with normalized column name
        df = pl.DataFrame(
            {
                "IstarDesignationHashId": [1, 2, 3],  # Pascal case normalized
                "other_col": ["a", "b", "c"],
            }
        )

        # Should not raise error (key column normalized to match DataFrame)
        store._validate_key_columns(df)

    def test_validate_key_columns_missing_after_polish(self):
        """Test key column validation fails when column missing after polish."""
        from hygge.core.polish import PolishConfig

        polish_config = PolishConfig(columns={"case": "pascal"})
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["MissingColumn"],
            row_marker=0,
            polish=polish_config,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")

        df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        with pytest.raises(StoreError, match="missing required key columns"):
            store._validate_key_columns(df)

    def test_validate_row_marker_invalid_values(self):
        """Test row marker validation catches invalid values."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "__rowMarker__": [0, 1, 99],  # Invalid value 99
            }
        )

        with pytest.raises(StoreError, match="Invalid __rowMarker__ values"):
            store._validate_row_marker(df)

    def test_validate_update_rows_with_update_marker(self):
        """Test update rows validation with __rowMarker__ = 1."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")

        df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["a", "b"],
                "__rowMarker__": [0, 1],  # One update row
            }
        )

        # Should not raise error (validation is permissive)
        store._validate_update_rows(df)

    @pytest.mark.asyncio
    async def test_initialize_sequence_counter_handles_directory_not_found(self):
        """Test sequence counter initialization handles missing directory."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            file_detection="timestamp",
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.base_path = "Files/LandingZone/users"

        mock_fs_client = MagicMock()
        mock_fs_client.get_paths = MagicMock(
            side_effect=Exception("NotFound: Directory does not exist")
        )

        with patch.object(
            store, "_get_file_system_client", return_value=mock_fs_client
        ):
            await store._initialize_sequence_counter()

        # Should fallback to starting_sequence - 1
        assert store.sequence_counter == 0  # starting_sequence (1) - 1

    @pytest.mark.asyncio
    async def test_initialize_sequence_counter_handles_other_errors(self):
        """Test sequence counter initialization handles other errors."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            file_detection="sequential",
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.base_path = "Files/LandingZone/users"

        mock_fs_client = MagicMock()
        mock_fs_client.get_paths = MagicMock(side_effect=Exception("Unexpected error"))

        with patch.object(
            store, "_get_file_system_client", return_value=mock_fs_client
        ):
            await store._initialize_sequence_counter()

        # Should fallback to starting_sequence - 1
        assert store.sequence_counter == 0

    @pytest.mark.asyncio
    async def test_get_next_filename_timestamp_mode(self):
        """Test filename generation in timestamp mode."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            file_detection="timestamp",
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.sequence_counter = 5  # Pre-initialized

        filename = await store.get_next_filename()

        # Should have timestamp format
        assert filename.endswith(".parquet")
        assert "_" in filename
        # Should increment sequence
        assert store.sequence_counter == 6

    @pytest.mark.asyncio
    async def test_get_next_filename_sequential_mode(self):
        """Test filename generation in sequential mode."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            file_detection="sequential",
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.sequence_counter = 10  # Pre-initialized

        filename = await store.get_next_filename()

        # Should be 20-digit sequential
        assert filename == "00000000000000000011.parquet"
        assert store.sequence_counter == 11

    @pytest.mark.asyncio
    async def test_reset_retry_sensitive_state(self):
        """Test resetting retry-sensitive state."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.sequence_counter = 100

        await store.reset_retry_sensitive_state()

        assert store.sequence_counter is None

    @pytest.mark.asyncio
    async def test_prepare_table_folder_full_drop_mode(self):
        """Test table folder preparation in full_drop mode."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.configure_for_run("full_drop")

        mock_adls = AsyncMock()
        mock_adls.read_json = AsyncMock(return_value=None)
        mock_adls.file_system_client.get_file_client = MagicMock()
        mock_file_client = MagicMock()
        mock_adls.file_system_client.get_file_client.return_value = mock_file_client
        mock_adls.file_system_client.get_directory_client = MagicMock()
        mock_dir_client = MagicMock()
        mock_dir_client.exists.return_value = True
        mock_adls.file_system_client.get_directory_client.return_value = mock_dir_client

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            with patch.object(
                store, "_write_metadata_json", new_callable=AsyncMock
            ) as mock_meta:
                with patch.object(
                    store, "_write_partner_events_json", new_callable=AsyncMock
                ) as mock_partner:
                    await store._prepare_table_folder()

        # Should write to _tmp in full_drop mode
        mock_meta.assert_awaited_once_with(to_tmp=True)
        mock_partner.assert_awaited_once_with(to_tmp=True)
        assert store._table_folder_prepared is True

    @pytest.mark.asyncio
    async def test_prepare_table_folder_incremental_mode(self):
        """Test table folder preparation in incremental mode."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.configure_for_run("incremental")

        mock_adls = AsyncMock()
        mock_adls.read_json = AsyncMock(return_value=None)
        mock_adls.file_system_client.get_file_client = MagicMock()
        mock_file_client = MagicMock()
        mock_adls.file_system_client.get_file_client.return_value = mock_file_client
        mock_adls.file_system_client.get_directory_client = MagicMock()
        mock_dir_client = MagicMock()
        mock_dir_client.exists.return_value = True
        mock_adls.file_system_client.get_directory_client.return_value = mock_dir_client

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            with patch.object(
                store, "_write_metadata_json", new_callable=AsyncMock
            ) as mock_meta:
                with patch.object(
                    store, "_write_partner_events_json", new_callable=AsyncMock
                ) as mock_partner:
                    await store._prepare_table_folder()

        # Should write to production in incremental mode (default to_tmp=False)
        mock_meta.assert_awaited_once()
        # Verify it was called without to_tmp or with to_tmp=False
        call_args = mock_meta.await_args
        assert call_args is not None
        assert call_args.kwargs.get("to_tmp", False) is False
        mock_partner.assert_awaited_once()
        partner_call_args = mock_partner.await_args
        assert partner_call_args is not None
        assert partner_call_args.kwargs.get("to_tmp", False) is False

    def test_map_polars_dtype_to_fabric(self):
        """Test Polars dtype to Fabric type mapping."""
        # Test various dtype mappings
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Utf8) == "string"
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Int64) == "long"
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Float64) == "double"
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Datetime) == "datetime"
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Date) == "datetime"
        # Boolean is now mapped explicitly instead of falling back to string
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Boolean) == "boolean"

    @pytest.mark.asyncio
    async def test_write_handles_table_folder_preparation(self):
        """Test write() ensures table folder is prepared."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")

        with patch.object(
            store, "_prepare_table_folder", new_callable=AsyncMock
        ) as mock_prep:
            with patch.object(store, "write", wraps=store.write):
                # Call parent write() which should call _prepare_table_folder
                df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
                await store.write(df)

        # Should prepare table folder before writing
        mock_prep.assert_awaited_once()

    def test_ensure_row_marker_last_reorders_columns(self):
        """Test that __rowMarker__ is moved to last position."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")

        df = pl.DataFrame(
            {
                "__rowMarker__": [0, 1, 2],
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
            }
        )

        result = store._ensure_row_marker_last(df)

        # __rowMarker__ should be last
        assert result.columns[-1] == "__rowMarker__"
        # __LastLoadedAt__ should be second-to-last if present
        if "__LastLoadedAt__" in result.columns:
            assert result.columns[-2] == "__LastLoadedAt__"

    def test_ensure_row_marker_last_with_last_loaded_at(self):
        """Test column reordering with __LastLoadedAt__ present."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )
        store = OpenMirroringStore("test_store", config, entity_name="users")

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "__LastLoadedAt__": [datetime.now() for _ in range(3)],
                "__rowMarker__": [0, 0, 0],
            }
        )

        result = store._ensure_row_marker_last(df)

        # Column order: [id, __LastLoadedAt__, __rowMarker__]
        assert result.columns[0] == "id"
        assert result.columns[-2] == "__LastLoadedAt__"
        assert result.columns[-1] == "__rowMarker__"
