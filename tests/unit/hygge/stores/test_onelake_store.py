"""
Unit tests for OneLake Store implementation.

Tests configuration, path management, and authentication setup.
Note: These are unit tests - integration tests with actual OneLake
should be in the integration test directory.
"""
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from hygge.stores.onelake import OneLakeStore, OneLakeStoreConfig


class TestOneLakeStoreConfig:
    """Test OneLake store configuration validation."""

    def test_config_with_minimal_required_fields(self):
        """Test config creation with minimal required fields."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/test/",
        )

        assert config.type == "onelake"
        assert config.account_url == "https://onelake.dfs.fabric.microsoft.com"
        assert config.filesystem == "MyLake"
        assert config.path == "landing/test/"
        assert config.credential == "managed_identity"  # Default
        assert config.incremental is None

    def test_config_with_all_fields(self):
        """Test config creation with all optional fields."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/{entity}/",
            credential="service_principal",
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
            compression="gzip",
            file_pattern="{timestamp}_{sequence:020d}.parquet",
            batch_size=50000,
            incremental=False,
        )

        assert config.credential == "service_principal"
        assert config.tenant_id == "test-tenant"
        assert config.compression == "gzip"
        assert config.incremental is False

    def test_config_path_optional_with_auto_build(self):
        """Test that path is optional and auto-built if not provided."""
        # Path is now optional - will be auto-built by build_lakehouse_path
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            # path not provided - should default to Files/{entity}/
        )

        # Should auto-build path to Files/{entity}/
        assert config.path == "Files/{entity}/"

    def test_config_validates_compression(self):
        """Test that compression must be valid."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/test/",
            compression="snappy",
        )
        assert config.compression == "snappy"

        with pytest.raises(ValidationError):
            OneLakeStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                path="landing/test/",
                compression="invalid",
            )

    def test_config_get_merged_options(self):
        """Test that get_merged_options returns all options."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/test/",
            compression="gzip",
        )

        options = config.get_merged_options()
        assert options["compression"] == "gzip"
        assert options["batch_size"] == 100_000  # Default


class TestOneLakeStoreInitialization:
    """Test OneLake store initialization."""

    def test_onelake_store_initialization(self):
        """Test OneLake store initializes correctly."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/{entity}/",
        )

        store = OneLakeStore("test_store", config)

        assert store.name == "test_store"
        assert store.config == config
        assert store.base_path == "landing/{entity}"  # PathHelper normalizes trailing slash
        assert store.file_pattern == "{sequence:020d}.parquet"  # Default
        assert store.incremental_override is None

    def test_onelake_configure_for_run_uses_full_drop_flag(self):
        """Inherited ADLS behaviour toggles truncate mode per run."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="Files/{entity}/",
        )

        store = OneLakeStore("test_store", config, entity_name="users")
        store.saved_paths = ["Files/_tmp/users/file.parquet"]
        store.uploaded_files = ["file.parquet"]
        store.sequence_counter = 9
        store.full_drop_mode = True

        store.configure_for_run("incremental")
        assert store.full_drop_mode is False
        assert store.saved_paths == []
        assert store.uploaded_files == []
        assert store.sequence_counter == 0

        store.configure_for_run("full_drop")
        assert store.full_drop_mode is True

    def test_onelake_incremental_disabled_forces_full_drop(self):
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="Files/{entity}/",
            incremental=False,
        )

        store = OneLakeStore("test_store", config, entity_name="users")
        store.configure_for_run("incremental")
        assert store.full_drop_mode is True

    def test_onelake_store_with_entity_name(self):
        """Test OneLake store with entity name substitution."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/{entity}/",
        )

        store = OneLakeStore("test_store", config, entity_name="users")

        assert store.base_path == "landing/users"  # Entity substituted, PathHelper normalizes

    def test_onelake_store_with_options(self):
        """Test OneLake store with custom options."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/{entity}/",
            compression="gzip",
            file_pattern="{timestamp}_{sequence:020d}.parquet",
        )

        store = OneLakeStore("test_store", config)

        assert store.compression == "gzip"
        assert store.file_pattern == "{timestamp}_{sequence:020d}.parquet"

    def test_onelake_store_path_management(self):
        """Test OneLake store path management."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/test/",
        )

        store = OneLakeStore("test_store", config)

        # Staging directory should be _tmp at base level
        staging_dir = store.get_staging_directory()
        assert staging_dir is not None
        assert isinstance(staging_dir, Path)
        assert staging_dir == Path("_tmp")

        # Final directory should be the OneLake path
        final_dir = store.get_final_directory()
        assert final_dir == Path("landing/test/")


class TestOneLakeStoreAuthentication:
    """Test OneLake store authentication setup."""

    @patch("hygge.stores.adls.store.ManagedIdentityCredential")
    def test_managed_identity_authentication(self, mock_credential):
        """Test managed identity credential initialization."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/test/",
            credential="managed_identity",
        )

        store = OneLakeStore("test_store", config)

        # Should initialize credential when needed
        store._get_credential()
        mock_credential.assert_called_once()

    def test_service_principal_requires_credentials(self):
        """Test that service principal requires tenant_id, client_id, client_secret."""
        # Should raise validation error during config creation
        with pytest.raises(ValidationError):
            OneLakeStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                path="landing/test/",
                credential="service_principal",
                # Missing credentials
            )

    def test_service_principal_with_credentials(self):
        """Test service principal credential with all required fields."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/test/",
            credential="service_principal",
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
        )

        store = OneLakeStore("test_store", config)
        # Should not raise error
        assert store.config.tenant_id == "test-tenant"


class TestOneLakeStorePathBuilding:
    """Test OneLake store path building (uses ADLS path building)."""

    def test_build_adls_path(self):
        """Test building ADLS path (OneLake extends ADLS)."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/users/",
        )

        store = OneLakeStore("test_store", config, entity_name="users")

        filename = "00000000000000000001.parquet"
        adls_path = store._build_adls_path(f"_tmp/users/{filename}")

        # Should include the base path
        assert "landing/users" in adls_path
        assert filename in adls_path

    def test_build_adls_path_with_entity(self):
        """Test building ADLS path with entity name."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="Files/{entity}/",
        )

        store = OneLakeStore("test_store", config, entity_name="orders")

        filename = "00000000000000000001.parquet"
        adls_path = store._build_adls_path(f"_tmp/orders/{filename}")

        # Should include the entity path
        assert "Files/orders" in adls_path
        assert filename in adls_path


class TestOneLakeStoreErrorHandling:
    """Test OneLake store error handling."""

    def test_empty_path_allowed(self):
        """Test that empty path is allowed (will be auto-built from mirror_name)."""
        # Empty path should be allowed - build_lakehouse_path will handle it
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            # path not specified - will be auto-built
        )

        # Should default to Files/{entity}/ for Lakehouse
        assert config.path == "Files/{entity}/"

    def test_invalid_compression_raises_validation_error(self):
        """Test that invalid compression raises ValidationError."""
        with pytest.raises(ValidationError):
            OneLakeStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                path="landing/test/",
                compression="invalid_compression",
            )
