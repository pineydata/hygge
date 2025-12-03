"""
Additional comprehensive tests for ADLS Store to improve coverage.

Focuses on error handling, edge cases, and complex methods that are currently
under-tested. This file supplements test_adls_store_unit.py.
"""
from unittest.mock import AsyncMock, patch

import polars as pl
import pytest

from hygge.stores.adls import ADLSStore, ADLSStoreConfig
from hygge.utility.exceptions import StoreError


class TestADLSStoreErrorHandling:
    """Test error handling paths in ADLS Store."""

    @pytest.mark.asyncio
    async def test_save_handles_upload_failure(self):
        """Test _save() handles upload failures."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")

        mock_adls = AsyncMock()
        mock_adls.upload_bytes = AsyncMock(side_effect=Exception("Upload failed"))

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
            with pytest.raises(StoreError, match="Failed to upload"):
                await store._save(df)

    @pytest.mark.asyncio
    async def test_save_skips_empty_dataframe(self):
        """Test _save() skips empty DataFrames."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")

        mock_adls = AsyncMock()
        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            empty_df = pl.DataFrame({"id": [], "name": []})
            await store._save(empty_df)

        # Should not attempt upload
        mock_adls.upload_bytes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cleanup_staging_handles_not_found(self):
        """Test cleanup_staging() handles directory not found."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")

        mock_adls = AsyncMock()
        mock_adls.delete_directory = AsyncMock(
            side_effect=Exception("NotFound: Directory does not exist")
        )

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            # Should handle gracefully
            await store.cleanup_staging()

        # Should have attempted deletion
        mock_adls.delete_directory.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cleanup_staging_handles_other_errors(self):
        """Test cleanup_staging() handles other errors with warning."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")

        mock_adls = AsyncMock()
        mock_adls.delete_directory = AsyncMock(
            side_effect=Exception("Permission denied")
        )

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            # Should handle gracefully (logs warning)
            await store.cleanup_staging()

    @pytest.mark.asyncio
    async def test_move_to_final_handles_missing_staging_path(self):
        """Test _move_to_final() handles missing staging path."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")
        store.saved_paths = []  # No saved paths

        with pytest.raises(StoreError, match="Cloud staging path not found"):
            await store._move_to_final(
                "_tmp/users/file.parquet", "data/users/file.parquet"
            )

    @pytest.mark.asyncio
    async def test_move_to_final_handles_move_failure(self):
        """Test _move_to_final() handles move operation failure."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")
        store.saved_paths = ["Files/_tmp/users/file.parquet"]

        mock_adls = AsyncMock()
        mock_adls.move_file = AsyncMock(side_effect=Exception("Move failed"))

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            with pytest.raises(StoreError, match="Failed to finalize transfer"):
                await store._move_to_final(
                    "_tmp/users/file.parquet", "data/users/file.parquet"
                )

    @pytest.mark.asyncio
    async def test_truncate_destination_handles_failure(self):
        """Test _truncate_destination() handles deletion failure."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")

        mock_adls = AsyncMock()
        mock_adls.delete_directory = AsyncMock(side_effect=Exception("Deletion failed"))

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            with pytest.raises(StoreError, match="Failed to truncate destination"):
                await store._truncate_destination(mock_adls)

    @pytest.mark.asyncio
    async def test_truncate_destination_handles_empty_path(self):
        """Test _truncate_destination() handles empty path."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")
        # Mock get_final_directory to return a path that normalizes to empty
        with patch.object(store, "get_final_directory", return_value=None):
            with pytest.raises(StoreError, match="Cannot truncate destination"):
                await store._truncate_destination(AsyncMock())

    @pytest.mark.asyncio
    async def test_move_staged_files_to_final_handles_no_saved_paths(self):
        """Test _move_staged_files_to_final() handles no saved paths."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")
        store.saved_paths = []  # No paths to move

        mock_adls = AsyncMock()
        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            # Should return early without error
            await store._move_staged_files_to_final()

    @pytest.mark.asyncio
    async def test_move_staged_files_to_final_handles_missing_final_dir(self):
        """Test _move_staged_files_to_final() handles missing final directory."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")
        store.saved_paths = ["Files/_tmp/users/file.parquet"]
        # Mock get_final_directory to return None
        with patch.object(store, "get_final_directory", return_value=None):
            with pytest.raises(StoreError, match="destination directory unknown"):
                await store._move_staged_files_to_final()

    @pytest.mark.asyncio
    async def test_reset_retry_sensitive_state(self):
        """Test resetting retry-sensitive state."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")
        store.sequence_counter = 100
        store.uploaded_files = ["file1.parquet", "file2.parquet"]
        store.saved_paths = ["path1", "path2"]

        await store.reset_retry_sensitive_state()

        assert store.sequence_counter == 0
        assert len(store.uploaded_files) == 0
        assert len(store.saved_paths) == 0

    @pytest.mark.asyncio
    async def test_close_calls_finish(self):
        """Test close() calls finish()."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")

        with patch.object(store, "finish", new_callable=AsyncMock) as mock_finish:
            await store.close()

        mock_finish.assert_awaited_once()


class TestADLSStoreAuthentication:
    """Test authentication edge cases."""

    def test_get_credential_service_principal_missing_params(self):
        """Test service principal credential requires all params."""
        # Pydantic validation happens at config creation, not at _get_credential()
        with pytest.raises(
            Exception, match="Service principal authentication requires"
        ):
            ADLSStoreConfig(
                account_url="https://mystorage.dfs.core.windows.net",
                filesystem="mycontainer",
                path="data/test/",
                credential="service_principal",
                tenant_id="test-tenant",
                # Missing client_id and client_secret
            )

    def test_get_credential_storage_key_missing_key(self):
        """Test storage key credential requires key."""
        # Pydantic validation happens at config creation, not at _get_credential()
        with pytest.raises(Exception, match="Storage key authentication requires"):
            ADLSStoreConfig(
                account_url="https://mystorage.dfs.core.windows.net",
                filesystem="mycontainer",
                path="data/test/",
                credential="storage_key",
                # Missing storage_account_key
            )

    @patch("hygge.stores.adls.store.DefaultAzureCredential")
    def test_get_credential_default_fallback(self, mock_default):
        """Test default credential fallback."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/test/",
            credential="default",
        )
        store = ADLSStore("test_store", config)

        store._get_credential()
        mock_default.assert_called_once()

    def test_get_service_client_handles_import_error(self):
        """Test service client creation handles import errors."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/test/",
            credential="managed_identity",
        )
        store = ADLSStore("test_store", config)

        with patch(
            "hygge.stores.adls.store.DataLakeServiceClient",
            side_effect=ImportError("Missing package"),
        ):
            with pytest.raises(
                StoreError, match="azure-storage-filedatalake package required"
            ):
                store._get_service_client()

    def test_get_service_client_storage_key_extracts_account_name(self):
        """Test storage key extracts account name from URL."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/test/",
            credential="storage_key",
            storage_account_key="test-key",
        )
        store = ADLSStore("test_store", config)

        with patch("hygge.stores.adls.store.AzureNamedKeyCredential") as mock_named_key:
            with patch("hygge.stores.adls.store.DataLakeServiceClient"):
                store._get_service_client()

        # Should extract "mystorage" from URL
        mock_named_key.assert_called_once()
        call_args = mock_named_key.call_args
        assert call_args[1]["name"] == "mystorage"
        assert call_args[1]["key"] == "test-key"

    def test_get_service_client_fabric_url_support(self):
        """Test service client handles Fabric URLs."""
        config = ADLSStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="data/test/",
            credential="storage_key",
            storage_account_key="test-key",
        )
        store = ADLSStore("test_store", config)

        with patch("hygge.stores.adls.store.AzureNamedKeyCredential") as mock_named_key:
            with patch("hygge.stores.adls.store.DataLakeServiceClient"):
                store._get_service_client()

        # Should handle Fabric URL format
        call_args = mock_named_key.call_args
        # Account name extraction should work for Fabric URLs too
        assert call_args[1]["name"] == "onelake"


class TestADLSStorePathHandling:
    """Test path handling edge cases."""

    def test_build_adls_path_with_trailing_slash(self):
        """Test path building handles trailing slashes."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/users/",  # Trailing slash
        )
        store = ADLSStore("test_store", config, entity_name="users")

        filename = "file.parquet"
        path = store._build_adls_path(filename)

        assert path.endswith(filename)
        assert "data/users" in path

    def test_build_adls_path_without_trailing_slash(self):
        """Test path building handles missing trailing slashes."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/users",  # No trailing slash
        )
        store = ADLSStore("test_store", config, entity_name="users")

        filename = "file.parquet"
        path = store._build_adls_path(filename)

        assert path.endswith(filename)
        assert "data/users" in path

    @pytest.mark.asyncio
    async def test_get_next_filename_with_pattern(self):
        """Test filename generation with custom pattern."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
            file_pattern="{timestamp}_{sequence:020d}_{name}.parquet",
        )
        store = ADLSStore("test_store", config, entity_name="users")
        store.sequence_counter = 5

        filename = await store.get_next_filename()

        assert filename.endswith(".parquet")
        assert "test_store" in filename  # {name} substitution
        assert "00000000000000000006" in filename  # {sequence} substitution

    @pytest.mark.asyncio
    async def test_get_next_filename_with_flow_name(self):
        """Test filename generation with flow_name in pattern."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
            file_pattern="{flow_name}_{sequence:020d}.parquet",
        )
        store = ADLSStore(
            "test_store", config, flow_name="users_flow", entity_name="users"
        )
        store.sequence_counter = 10

        filename = await store.get_next_filename()

        assert "users_flow" in filename
        assert "00000000000000000011" in filename


class TestADLSStoreFullDropMode:
    """Test full_drop mode behavior."""

    @pytest.mark.asyncio
    async def test_move_staged_files_to_final_truncates_in_full_drop(self):
        """Test full_drop mode truncates before moving files."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")
        store.configure_for_run("full_drop")
        store.saved_paths = ["Files/_tmp/users/file.parquet"]

        mock_adls = AsyncMock()
        mock_move = AsyncMock()

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            with patch.object(store, "_move_to_final", mock_move):
                await store._move_staged_files_to_final()

        # Should truncate destination first
        mock_adls.delete_directory.assert_awaited_once_with(
            "data/users", recursive=True
        )
        # Then move files
        mock_move.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_move_staged_files_to_final_skips_truncate_in_incremental(self):
        """Test incremental mode skips truncation."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )
        store = ADLSStore("test_store", config, entity_name="users")
        store.configure_for_run("incremental")
        store.saved_paths = ["Files/_tmp/users/file.parquet"]

        mock_adls = AsyncMock()
        mock_move = AsyncMock()

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            with patch.object(store, "_move_to_final", mock_move):
                await store._move_staged_files_to_final()

        # Should not truncate in incremental mode
        mock_adls.delete_directory.assert_not_awaited()
        # But should still move files
        mock_move.assert_awaited_once()
