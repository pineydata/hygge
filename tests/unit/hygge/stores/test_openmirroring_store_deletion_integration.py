"""
Unit tests for OpenMirroringStore deletion detection integration.

Tests the integration methods (set_home_for_deletions, before_flow_start) in
OpenMirroringStore for query-based deletion detection.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hygge.core.home import Home
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
            store.key_columns = store_config.key_columns  # Required for find_deletions
            store.logger = MagicMock()
            store._get_adls_ops = MagicMock()
            store.get_staging_directory = MagicMock(return_value="/tmp/staging")
            store.compression = "snappy"
            return store

    def test_set_home_for_deletions_stores_home_reference(self, store):
        """Test that set_home_for_deletions() stores Home reference."""

        class TestHome(Home, home_type="test"):
            async def _get_batches(self):
                yield None

        home = TestHome("test_home", {})
        store.set_home_for_deletions(home)

        assert store._home_for_deletions == home

    @pytest.mark.asyncio
    async def test_before_flow_start_skips_when_deletion_source_not_configured(
        self, store
    ):
        """Test that before_flow_start() skips when deletion_source not configured."""
        store.config.deletion_source = None

        await store.before_flow_start()

        # Should not raise error or call find_deletions
        assert not hasattr(store, "_deletions_checked") or not store._deletions_checked

    @pytest.mark.asyncio
    async def test_before_flow_start_skips_when_already_checked(self, store):
        """Test that before_flow_start() skips when already checked."""
        store._deletions_checked = True

        await store.before_flow_start()

        # Should not call find_deletions again

    @pytest.mark.asyncio
    async def test_before_flow_start_calls_find_deletions(self, store):
        """Test that before_flow_start() calls find_deletions()."""
        with patch(
            "hygge.stores.openmirroring.store.find_deletions",
            new_callable=AsyncMock,
        ) as mock_find_deletions, patch("hygge.utility.retry.with_retry") as mock_retry:
            # Mock retry decorator: with_retry(...) returns a decorator function
            # When called with find_deletions, it should return a wrapped async
            # function. The real pattern is:
            # with_retry(...) -> decorator -> decorator(func) -> wrapped_func
            def mock_retry_decorator(*args, **kwargs):
                def decorator(func):
                    # Return an async function that calls the mocked find_deletions
                    async def wrapped(*wrapped_args, **wrapped_kwargs):
                        return await mock_find_deletions(
                            *wrapped_args, **wrapped_kwargs
                        )

                    return wrapped

                return decorator

            # Make the mock callable and return the decorator function
            mock_retry.side_effect = mock_retry_decorator

            await store.before_flow_start()

            # Verify find_deletions was called
            assert mock_find_deletions.called
            assert store._deletions_checked is True

    @pytest.mark.asyncio
    async def test_before_flow_start_fails_when_required_and_fails(self, store):
        """Test before_flow_start() fails flow when deletion_finding_required is True."""  # noqa: E501
        store.config.deletion_finding_required = True

        with patch(
            "hygge.stores.openmirroring.store.find_deletions",
            new_callable=AsyncMock,
        ) as mock_find_deletions, patch("hygge.utility.retry.with_retry") as mock_retry:
            from hygge.utility.exceptions import StoreError

            def mock_retry_decorator(*args, **kwargs):
                def decorator(func):
                    # Return an async function that calls the mocked find_deletions
                    async def wrapped(*wrapped_args, **wrapped_kwargs):
                        return await mock_find_deletions(
                            *wrapped_args, **wrapped_kwargs
                        )

                    return wrapped

                return decorator

            # Make the mock callable and return the decorator function
            mock_retry.side_effect = mock_retry_decorator
            mock_find_deletions.side_effect = StoreError("Deletion finding failed")

            with pytest.raises(
                StoreError, match="Finding deletions failed and is required"
            ):
                await store.before_flow_start()

    @pytest.mark.asyncio
    async def test_before_flow_start_continues_when_optional_and_fails(self, store):
        """Test before_flow_start() continues when deletion_finding_required is False."""  # noqa: E501
        store.config.deletion_finding_required = False

        with patch(
            "hygge.stores.openmirroring.store.find_deletions",
            new_callable=AsyncMock,
        ) as mock_find_deletions, patch("hygge.utility.retry.with_retry") as mock_retry:
            from hygge.utility.exceptions import StoreError

            def mock_retry_decorator(*args, **kwargs):
                def decorator(func):
                    # Return an async function that calls the mocked find_deletions
                    async def wrapped(*wrapped_args, **wrapped_kwargs):
                        return await mock_find_deletions(
                            *wrapped_args, **wrapped_kwargs
                        )

                    return wrapped

                return decorator

            # Make the mock callable and return the decorator function
            mock_retry.side_effect = mock_retry_decorator
            mock_find_deletions.side_effect = StoreError("Deletion finding failed")

            # Should not raise error, just log warning
            await store.before_flow_start()

            # Verify warning was logged
            assert store.logger.warning.called
            assert store._deletions_checked is True

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
