"""
Unit tests for OpenMirroringStore full_drop deletion detection integration.

Tests the before_flow_start() method in OpenMirroringStore for full_drop
deletion detection (marking all target rows as deleted).
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hygge.stores.openmirroring import OpenMirroringStore, OpenMirroringStoreConfig


class TestOpenMirroringStoreDeletionIntegration:
    """Test OpenMirroringStore deletion detection integration methods."""

    @pytest.fixture
    def store_config(self):
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
    def store(self, store_config):
        """Create OpenMirroringStore instance."""
        with patch(
            "hygge.stores.adls.ADLSStore.__init__", lambda self, *args, **kwargs: None
        ):
            store = OpenMirroringStore("test_store", store_config)
            # Set required attributes that would normally be set by ADLSStore.__init__
            store.config = store_config
            store.incremental_override = store_config.incremental
            store.key_columns = store_config.key_columns
            store.entity_name = "users"
            store.logger = MagicMock()
            store._get_adls_ops = MagicMock()
            store.get_staging_directory = MagicMock(return_value="/tmp/staging")
            store.compression = "snappy"
            # Configure for full_drop mode
            store.configure_for_run("full_drop")
            return store

    @pytest.mark.asyncio
    async def test_before_flow_start_skips_when_deletion_source_not_configured(
        self, store
    ):
        """Test that before_flow_start() skips when deletion_source not configured."""
        store.config.deletion_source = None

        await store.before_flow_start()

        # Should not raise error or call _query_target_keys

    @pytest.mark.asyncio
    async def test_before_flow_start_skips_when_not_full_drop(self, store):
        """Test that before_flow_start() skips when not in full_drop mode."""
        store.configure_for_run("incremental")

        await store.before_flow_start()

        # Should not raise error or call _query_target_keys

    @pytest.mark.asyncio
    async def test_before_flow_start_writes_deletion_markers(self, store):
        """Test that before_flow_start() writes deletion markers to _tmp."""
        # In streaming mode, before_flow_start() only calls _query_target_keys()
        # which handles sequence reservation and marker writing internally
        store._query_target_keys = AsyncMock()

        await store.before_flow_start()

        # Verify _query_target_keys was called (it handles everything internally)
        assert store._query_target_keys.called

    def test_configure_for_run_resets_deletion_tracking(self, store):
        """Test that configure_for_run() resets deletion tracking."""
        store._deletions_checked = True
        store._deletion_metrics = {"query_based_deletions": 10}

        store.configure_for_run("full_drop")

        assert store._deletions_checked is False
        assert store._deletion_metrics == {
            "column_based_deletions": 0,
            "query_based_deletions": 0,
        }
