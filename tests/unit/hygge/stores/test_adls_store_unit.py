"""
Unit tests for ADLS Gen2 store implementation.

Tests configuration, path management, and authentication setup without
requiring an actual ADLS account. Integration tests are in tests/integration/.
"""
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from hygge.stores.adls import ADLSStore, ADLSStoreConfig


class TestADLSStoreConfig:
    """Test ADLS store configuration validation."""

    def test_config_with_minimal_required_fields(self):
        """Test config creation with minimal required fields."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )

        assert config.type == "adls"
        assert config.account_url == "https://mystorage.dfs.core.windows.net"
        assert config.filesystem == "mycontainer"
        assert config.path == "data/{entity}/"
        assert config.credential == "managed_identity"  # Default
        assert config.compression == "snappy"  # Default
        assert config.batch_size == 100_000  # Default
        assert config.incremental is None

    def test_config_with_all_fields(self):
        """Test config creation with all optional fields."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
            credential="service_principal",
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
            compression="gzip",
            file_pattern="{timestamp}_{sequence:020d}.parquet",
            batch_size=50_000,
            incremental=False,
        )

        assert config.credential == "service_principal"
        assert config.tenant_id == "test-tenant"
        assert config.compression == "gzip"
        assert config.batch_size == 50_000
        assert config.incremental is False

    def test_config_validates_compression(self):
        """Test that compression must be valid."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/test/",
            compression="snappy",
        )
        assert config.compression == "snappy"

        with pytest.raises(ValidationError):
            ADLSStoreConfig(
                account_url="https://mystorage.dfs.core.windows.net",
                filesystem="mycontainer",
                path="data/test/",
                compression="invalid",
            )

    def test_config_storage_key_authentication(self):
        """Test storage key authentication."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/test/",
            credential="storage_key",
            storage_account_key="test-key",
        )

        assert config.credential == "storage_key"
        assert config.storage_account_key == "test-key"


class TestADLSStoreInitialization:
    """Test ADLS store initialization without credentials."""

    def test_adls_store_initialization(self):
        """Test ADLS store initializes correctly."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )

        store = ADLSStore("test_store", config)

        assert store.name == "test_store"
        assert store.config == config
        assert store.base_path == "data/{entity}/"
        assert store.file_pattern == "{sequence:020d}.parquet"  # Default
        assert store.compression == "snappy"  # Default
        assert store.sequence_counter == 0
        assert store.uploaded_files == []
        assert store.incremental_override is None

    def test_configure_for_run_resets_state(self):
        """Run-level configuration resets counters and flags."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )

        store = ADLSStore("test_store", config, entity_name="users")
        store.saved_paths = ["Files/_tmp/users/file.parquet"]
        store.uploaded_files = ["file.parquet"]
        store.sequence_counter = 7
        store.full_drop_mode = True

        store.configure_for_run("incremental")

        assert store.full_drop_mode is False
        assert store.saved_paths == []
        assert store.uploaded_files == []
        assert store.sequence_counter == 0

        store.configure_for_run("full_drop")
        assert store.full_drop_mode is True

    def test_configure_for_run_respects_incremental_flag(self):
        """Stores with incremental disabled should always truncate."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
            incremental=False,
        )

        store = ADLSStore("test_store", config, entity_name="users")
        store.configure_for_run("incremental")

        assert store.full_drop_mode is True
        assert store.incremental_override is False

    def test_adls_store_with_entity_name(self):
        """Test ADLS store with entity name substitution."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
        )

        store = ADLSStore("test_store", config, entity_name="users")

        assert store.base_path == "data/users/"  # Entity substituted
        assert store.entity_name == "users"

    def test_adls_store_path_management(self):
        """Test ADLS store path management."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/test/",
        )

        store = ADLSStore("test_store", config)

        # Staging directory should be _tmp at base level
        staging_dir = store.get_staging_directory()
        assert staging_dir is not None
        assert isinstance(staging_dir, Path)
        assert staging_dir == Path("_tmp")

        # Final directory should be the ADLS path
        final_dir = store.get_final_directory()
        assert final_dir == Path("data/test/")

    def test_adls_store_with_custom_options(self):
        """Test ADLS store with custom options."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/{entity}/",
            compression="gzip",
            file_pattern="{timestamp}_{sequence:020d}.parquet",
            batch_size=50_000,
        )

        store = ADLSStore("test_store", config)

        assert store.compression == "gzip"
        assert store.file_pattern == "{timestamp}_{sequence:020d}.parquet"
        assert store.batch_size == 50_000


class TestADLSStoreAuthentication:
    """Test ADLS store authentication setup (mocked)."""

    @patch("hygge.stores.adls.store.ManagedIdentityCredential")
    def test_managed_identity_authentication(self, mock_credential):
        """Test managed identity credential initialization."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/test/",
            credential="managed_identity",
        )

        store = ADLSStore("test_store", config)

        # Should initialize credential when needed
        store._get_credential()
        mock_credential.assert_called_once()

    @patch("hygge.stores.adls.store.ClientSecretCredential")
    def test_service_principal_authentication(self, mock_credential):
        """Test service principal credential initialization."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/test/",
            credential="service_principal",
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
        )

        store = ADLSStore("test_store", config)

        # Should initialize credential when needed
        store._get_credential()
        mock_credential.assert_called_once_with(
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
        )


@pytest.mark.asyncio
async def test_move_staged_files_to_final_full_drop_truncates_destination():
    """full_drop runs delete destination data before publishing new files."""
    config = ADLSStoreConfig(
        account_url="https://mystorage.dfs.core.windows.net",
        filesystem="mycontainer",
        path="data/{entity}/",
    )

    store = ADLSStore("test_store", config, entity_name="users")
    store.full_drop_mode = True
    store.saved_paths = ["Files/_tmp/users/00000000000000000001.parquet"]

    adls_ops = AsyncMock()
    move_mock = AsyncMock()

    with patch.object(store, "_get_adls_ops", return_value=adls_ops):
        with patch.object(store, "_move_to_final", move_mock):
            await store._move_staged_files_to_final()

    adls_ops.delete_directory.assert_awaited_once_with("data/users", recursive=True)
    move_mock.assert_awaited_once()
    staging_arg, final_arg = move_mock.await_args.args
    assert str(staging_arg).endswith("00000000000000000001.parquet")
    assert str(final_arg).endswith("00000000000000000001.parquet")

    @patch("hygge.stores.adls.store.AzureNamedKeyCredential")
    def test_storage_key_authentication(self, mock_named_key):
        """Test storage key credential initialization."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/test/",
            credential="storage_key",
            storage_account_key="test-key",
        )

        store = ADLSStore("test_store", config)

        # Should initialize credential when needed
        store._get_credential()
        # Credential is None for storage_key, but AzureNamedKeyCredential
        # is created in _get_service_client()
        assert store._credential is None

        # Call _get_service_client() which uses the credential
        store._get_service_client()
        # AzureNamedKeyCredential should be called with account name and key
        mock_named_key.assert_called_with(name="mystorage", key="test-key")


class TestADLSStorePathBuilding:
    """Test ADLS store path building."""

    def test_build_adls_path_simple(self):
        """Test building ADLS path with simple structure."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/users/",
        )

        store = ADLSStore("test_store", config, entity_name="users")

        filename = "00000000000000000001.parquet"
        adls_path = store._build_adls_path(f"_tmp/users/{filename}")

        # Should include the base path
        assert "data/users" in adls_path
        assert filename in adls_path

    def test_build_adls_path_with_entity_template(self):
        """Test building ADLS path with entity template."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="Files/{entity}/",
        )

        store = ADLSStore("test_store", config, entity_name="orders")

        filename = "00000000000000000001.parquet"
        adls_path = store._build_adls_path(f"_tmp/orders/{filename}")

        # Should include the entity path
        assert "Files/orders" in adls_path
        assert filename in adls_path

    def test_get_staging_directory_centralized(self):
        """Test that staging directory is centralized under _tmp."""
        config = ADLSStoreConfig(
            account_url="https://mystorage.dfs.core.windows.net",
            filesystem="mycontainer",
            path="data/test/",
        )

        store_with_entity = ADLSStore("test_store", config, entity_name="users")
        assert store_with_entity.get_staging_directory() == Path("_tmp")

        store_without_entity = ADLSStore("test_store", config)
        assert store_without_entity.get_staging_directory() == Path("_tmp")

    def test_cloud_staging_path_with_entity(self):
        """Test that cloud staging path is Files/_tmp/entity/filename."""
        from hygge.utility.path_helper import PathHelper

        config = ADLSStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="Files/{entity}/",
        )

        store = ADLSStore("test_store", config, entity_name="Account")

        # Test the internal _save logic would build correct path
        # staging_path from get_staging_directory would be "_tmp/filename.parquet"
        filename = "00000000000000000001.parquet"

        # Use PathHelper to build staging path (same as _save does)
        cloud_staging_path = PathHelper.build_staging_path(
            store.base_path, store.entity_name, filename
        )

        # Should be Files/_tmp/Account/filename.parquet
        assert cloud_staging_path == "Files/_tmp/Account/00000000000000000001.parquet"
