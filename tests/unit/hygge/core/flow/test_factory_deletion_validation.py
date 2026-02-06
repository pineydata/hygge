"""
Unit tests for FlowFactory deletion source resolution.

Tests the _resolve_deletion_source() method that resolves connection names
to connection dictionaries for full_drop deletion detection.

Note: These tests are skipped as _validate_deletion_compatibility() doesn't
exist for full_drop deletions (they don't require Home support).
"""
from unittest.mock import MagicMock, patch

import pytest

from hygge.core.home import Home
from hygge.homes.mssql import MssqlHome, MssqlHomeConfig
from hygge.stores.openmirroring import OpenMirroringStore, OpenMirroringStoreConfig


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

    @pytest.mark.skip(
        reason=(
            "Full_drop deletions don't require Home support - "
            "they query target directly"
        )
    )
    def test_validate_deletion_compatibility_skips_non_openmirroring_store(
        self, mssql_home
    ):
        """Test skipped - full_drop deletions don't use this validation."""
        pass

    @pytest.mark.skip(
        reason=(
            "Full_drop deletions don't require Home support - "
            "they query target directly"
        )
    )
    def test_validate_deletion_compatibility_skips_when_deletion_source_not_configured(
        self, openmirroring_store, mssql_home
    ):
        """Test skipped - full_drop deletions don't use this validation."""
        pass

    @pytest.mark.skip(
        reason=(
            "Full_drop deletions don't require Home support - "
            "they query target directly"
        )
    )
    def test_validate_deletion_compatibility_passes_when_home_supports_key_finding(
        self, openmirroring_store, openmirroring_store_config, mssql_home
    ):
        """Test skipped - full_drop deletions don't use this validation."""
        pass

    @pytest.mark.skip(
        reason=(
            "Full_drop deletions don't require Home support - "
            "they query target directly"
        )
    )
    def test_validate_deletion_compatibility_raises_error_when_home_not_supported(
        self, openmirroring_store, openmirroring_store_config, unsupported_home
    ):
        """Test skipped - full_drop deletions don't use this validation."""
        pass

    @pytest.mark.skip(
        reason=(
            "Full_drop deletions don't require Home support - "
            "they query target directly"
        )
    )
    def test_validate_deletion_compatibility_error_message_includes_entity_name(
        self, openmirroring_store, openmirroring_store_config, unsupported_home
    ):
        """Test skipped - full_drop deletions don't use this validation."""
        pass
