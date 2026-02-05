"""
Unit tests for FlowFactory deletion compatibility validation.

Tests the _validate_deletion_compatibility() method that ensures Home supports
find_keys() when query-based deletions are configured.
"""
from unittest.mock import MagicMock, patch

import pytest

from hygge.core.flow.factory import FlowFactory
from hygge.core.home import Home
from hygge.homes.mssql import MssqlHome, MssqlHomeConfig
from hygge.stores.openmirroring import OpenMirroringStore, OpenMirroringStoreConfig
from hygge.utility.exceptions import ConfigError


class TestFlowFactoryDeletionValidation:
    """Test FlowFactory deletion compatibility validation."""

    @pytest.fixture
    def openmirroring_store_config(self):
        """Create OpenMirroringStoreConfig with deletion_source."""
        return OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="test_workspace",
            mirror_name="test_database",
            key_columns=["id"],
            row_marker=4,
            deletion_source={
                "server": "test.database.windows.net",
                "database": "testdb",
            },
        )

    @pytest.fixture
    def openmirroring_store(self, openmirroring_store_config):
        """Create OpenMirroringStore instance."""
        with patch(
            "hygge.stores.adls.ADLSStore.__init__", lambda self, *args, **kwargs: None
        ):
            store = OpenMirroringStore("test_store", openmirroring_store_config)
            store.logger = MagicMock()
            return store

    @pytest.fixture
    def mssql_home(self):
        """Create MssqlHome instance."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        return MssqlHome("test_home", config)

    @pytest.fixture
    def unsupported_home(self):
        """Create Home that doesn't support key finding."""

        class UnsupportedHome(Home, home_type="unsupported"):
            async def _get_batches(self):
                yield None

        return UnsupportedHome("test_home", {})

    def test_validate_deletion_compatibility_skips_non_openmirroring_store(
        self, mssql_home
    ):
        """Test that validation skips non-OpenMirroringStore instances."""
        from hygge.stores.parquet import ParquetStore, ParquetStoreConfig

        store_config = ParquetStoreConfig(path="/tmp/test")
        store = ParquetStore("test_store", store_config)

        # Should not raise error
        FlowFactory._validate_deletion_compatibility(
            store, store_config, mssql_home, "test_flow", None, MagicMock()
        )

    def test_validate_deletion_compatibility_skips_when_deletion_source_not_configured(
        self, openmirroring_store, mssql_home
    ):
        """Test that validation skips when deletion_source not configured."""
        store_config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="test_workspace",
            mirror_name="test_database",
            key_columns=["id"],
            row_marker=4,
            # deletion_source not set
        )

        # Should not raise error
        FlowFactory._validate_deletion_compatibility(
            openmirroring_store,
            store_config,
            mssql_home,
            "test_flow",
            None,
            MagicMock(),
        )

    def test_validate_deletion_compatibility_passes_when_home_supports_key_finding(
        self, openmirroring_store, openmirroring_store_config, mssql_home
    ):
        """Test that validation passes when Home supports key finding."""
        logger = MagicMock()

        # Should not raise error
        FlowFactory._validate_deletion_compatibility(
            openmirroring_store,
            openmirroring_store_config,
            mssql_home,
            "test_flow",
            None,
            logger,
        )

        # Verify debug message was logged
        assert logger.debug.called

    def test_validate_deletion_compatibility_raises_error_when_home_not_supported(
        self, openmirroring_store, openmirroring_store_config, unsupported_home
    ):
        """Test validation raises ConfigError when Home doesn't support key finding."""
        with pytest.raises(ConfigError, match="does not support find_keys"):
            FlowFactory._validate_deletion_compatibility(
                openmirroring_store,
                openmirroring_store_config,
                unsupported_home,
                "test_flow",
                None,
                MagicMock(),
            )

    def test_validate_deletion_compatibility_error_message_includes_entity_name(
        self, openmirroring_store, openmirroring_store_config, unsupported_home
    ):
        """Test that error message includes entity name when provided."""
        with pytest.raises(ConfigError, match="entity 'users'"):
            FlowFactory._validate_deletion_compatibility(
                openmirroring_store,
                openmirroring_store_config,
                unsupported_home,
                "test_flow",
                "users",
                MagicMock(),
            )
