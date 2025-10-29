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
from hygge.utility import StoreError


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
            partition_by_date=True,
            batch_size=50000,
        )

        assert config.credential == "service_principal"
        assert config.tenant_id == "test-tenant"
        assert config.compression == "gzip"

    def test_config_validates_path_required(self):
        """Test that path is required for OneLake stores."""
        with pytest.raises(ValidationError):
            OneLakeStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                # path missing
            )

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
            partition_by_date=False,
        )

        options = config.get_merged_options()
        assert options["compression"] == "gzip"
        assert options["partition_by_date"] is False
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
        assert store.base_path == "landing/{entity}/"
        assert store.file_pattern == "{sequence:020d}.parquet"  # Default

    def test_onelake_store_with_entity_name(self):
        """Test OneLake store with entity name substitution."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/{entity}/",
        )

        store = OneLakeStore("test_store", config, entity_name="users")

        assert store.base_path == "landing/users/"  # Entity substituted

    def test_onelake_store_with_options(self):
        """Test OneLake store with custom options."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/{entity}/",
            compression="gzip",
            file_pattern="{timestamp}_{sequence:020d}.parquet",
            partition_by_date=True,
        )

        store = OneLakeStore("test_store", config)

        assert store.compression == "gzip"
        assert store.file_pattern == "{timestamp}_{sequence:020d}.parquet"
        assert store.partition_by_date is True

    def test_onelake_store_path_management(self):
        """Test OneLake store path management."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/test/",
        )

        store = OneLakeStore("test_store", config)

        # Staging directory should be local temp
        staging_dir = store.get_staging_directory()
        assert staging_dir is not None
        assert isinstance(staging_dir, Path)
        assert "hygge" in str(staging_dir)
        assert "test_store" in str(staging_dir)

        # Final directory should be the OneLake path
        final_dir = store.get_final_directory()
        assert final_dir == Path("landing/test/")


class TestOneLakeStoreAuthentication:
    """Test OneLake store authentication setup."""

    @patch("hygge.stores.onelake.ManagedIdentityCredential")
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
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/test/",
            credential="service_principal",
            # Missing credentials
        )

        store = OneLakeStore("test_store", config)

        # Should raise error when trying to get credential
        with pytest.raises(StoreError) as exc_info:
            store._get_credential()

        assert "requires tenant_id" in str(exc_info.value).lower()

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
    """Test OneLake store path building."""

    @pytest.mark.asyncio
    async def test_build_onelake_path_with_date_partitioning(self):
        """Test building OneLake path with date partitioning."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/users/",
            partition_by_date=True,
        )

        store = OneLakeStore("test_store", config)

        filename = "00000000000000000001.parquet"
        onelake_path = store._build_onelake_path(filename)

        # Should include date partition: YYYY/MM/DD/
        assert "2025/" in onelake_path or "2024/" in onelake_path
        assert filename in onelake_path

    @pytest.mark.asyncio
    async def test_build_onelake_path_without_date_partitioning(self):
        """Test building OneLake path without date partitioning."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/users/",
            partition_by_date=False,
        )

        store = OneLakeStore("test_store", config)

        filename = "00000000000000000001.parquet"
        onelake_path = store._build_onelake_path(filename)

        # Should not include date partition
        assert onelake_path == "landing/users/00000000000000000001.parquet"

    @pytest.mark.asyncio
    async def test_get_next_filename_with_patterns(self):
        """Test filename generation with different patterns."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/users/",
            file_pattern="{name}_{timestamp}_{sequence:020d}.parquet",
        )

        store = OneLakeStore("test_store", config)

        filename = await store.get_next_filename()
        assert "test_store" in filename
        assert filename.endswith(".parquet")

    @pytest.mark.asyncio
    async def test_get_next_filename_sequence_increment(self):
        """Test that sequence counter increments."""
        config = OneLakeStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            path="landing/users/",
            file_pattern="{sequence:020d}.parquet",
        )

        store = OneLakeStore("test_store", config)

        filename1 = await store.get_next_filename()
        filename2 = await store.get_next_filename()

        # Second filename should have higher sequence number
        seq1 = int(filename1.replace(".parquet", ""))
        seq2 = int(filename2.replace(".parquet", ""))
        assert seq2 > seq1


class TestOneLakeStoreErrorHandling:
    """Test OneLake store error handling."""

    def test_empty_path_raises_validation_error(self):
        """Test that empty path raises ValidationError."""
        with pytest.raises(ValidationError):
            OneLakeStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                path="",  # Empty path
            )

    def test_invalid_compression_raises_validation_error(self):
        """Test that invalid compression raises ValidationError."""
        with pytest.raises(ValidationError):
            OneLakeStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                path="landing/test/",
                compression="invalid_compression",
            )
