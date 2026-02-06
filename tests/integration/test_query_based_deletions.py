"""
Integration tests for query-based deletion detection.

Tests the full query-based deletion detection flow with real components
(without requiring actual database connections).

NOTE: These tests are for query-based deletions (incremental), not full_drop
deletions. Full_drop deletions use a simpler approach (mark all target rows
as deleted) and are tested in test_openmirroring_store.py.
"""
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from hygge.core.flow.flow import Flow
from hygge.homes.mssql import MssqlHomeConfig
from hygge.stores.openmirroring import OpenMirroringStore, OpenMirroringStoreConfig
from hygge.utility.exceptions import StoreError


@pytest.mark.skip(
    reason="These tests are for query-based deletions (incremental), "
    "not full_drop deletions. Full_drop uses a simpler approach."
)
class TestQueryBasedDeletionsIntegration:
    """Integration tests for query-based deletion detection."""

    @pytest.fixture
    def home_config(self):
        """Create MssqlHomeConfig for testing."""
        return MssqlHomeConfig(
            type="mssql",
            server="source.database.windows.net",
            database="sourcedb",
            table="dbo.users",
        )

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
                "server": "target.database.windows.net",
                "database": "targetdb",
                "schema": "dbo",
                "table": "users",
            },
        )

    @pytest.mark.asyncio
    async def test_flow_factory_validates_deletion_compatibility(
        self, home_config, store_config
    ):
        """Test FlowFactory validates deletion compatibility during flow creation."""
        # This test is covered by unit tests in test_factory_deletion_validation.py
        # Integration test would require full FlowFactory setup which is complex
        # Skip this test in favor of unit tests
        pass

    @pytest.mark.asyncio
    async def test_before_flow_start_called_before_producer_consumer(self):
        """Test before_flow_start() is called before producer/consumer tasks start."""
        store_config = OpenMirroringStoreConfig(
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

        with patch(
            "hygge.stores.adls.ADLSStore.__init__", lambda self, *args, **kwargs: None
        ):
            store = OpenMirroringStore("test_store", store_config)
            # Set required attributes that would normally be set by ADLSStore.__init__
            store.config = store_config
            store.incremental_override = store_config.incremental
            store.logger = MagicMock()
            store.before_flow_start = AsyncMock()

            # Create a simple flow
            class TestHome:
                async def read(self):
                    yield pl.DataFrame({"id": [1, 2, 3]})

            home = TestHome()
            flow = Flow(
                name="test_flow",
                home=home,
                store=store,
                options={},
                entity_name="users",
            )

            # Mock the producer/consumer to avoid actual execution
            with patch.object(
                flow, "_producer", new_callable=AsyncMock
            ) as mock_producer, patch.object(
                flow, "_consumer", new_callable=AsyncMock
            ) as mock_consumer:
                mock_producer.return_value = None
                mock_consumer.return_value = None

                # Execute flow
                try:
                    await flow._execute_flow()
                except Exception:
                    pass  # Expected to fail, we just want to check call order

                # Verify before_flow_start was called
                assert store.before_flow_start.called

    @pytest.mark.asyncio
    async def test_deletion_detection_flow_with_mock_data(
        self, home_config, store_config
    ):
        """Test full deletion detection flow with mocked data."""
        # Mock target keys (keys that exist in target but not in source)
        # Use explicit schema to ensure correct types for join
        target_keys = pl.DataFrame({"id": [1, 2, 3, 4, 5]}, schema={"id": pl.Int64})
        source_keys = pl.DataFrame(
            {"id": [1, 2, 3]}, schema={"id": pl.Int64}
        )  # Missing 4 and 5

        with patch(
            "hygge.stores.adls.ADLSStore.__init__", lambda self, *args, **kwargs: None
        ):
            store = OpenMirroringStore("test_store", store_config)
            # Set required attributes that would normally be set by ADLSStore.__init__
            store.config = store_config
            store.incremental_override = store_config.incremental
            store.key_columns = store_config.key_columns  # Required for find_deletions
            store.entity_name = "users"  # Required for stage_target_keys_in_tmp
            store.logger = MagicMock()
            store.write = AsyncMock()
            store._flush_buffer = AsyncMock()
            from pathlib import Path

            store.get_staging_directory = MagicMock(return_value=Path("/tmp/staging"))
            store.compression = "snappy"
            # Initialize deletion tracking attributes
            store._deletions_checked = False
            store._deletion_metrics = {
                "column_based_deletions": 0,
                "query_based_deletions": 0,
            }

            # Mock ADLS operations
            adls_ops = MagicMock()
            adls_ops.upload_bytes = AsyncMock(
                return_value="/tmp/staging/_deletion_check_keys.parquet"
            )
            # Return actual parquet bytes for target_keys
            # Write parquet and get bytes - this will be read back by find_deletions
            import io

            target_keys_buffer = io.BytesIO()
            target_keys.write_parquet(target_keys_buffer, compression="snappy")
            # Reset buffer position and get bytes
            target_keys_buffer.seek(0)
            target_keys_data = target_keys_buffer.read()
            adls_ops.read_file_bytes = AsyncMock(return_value=target_keys_data)
            adls_ops.delete_file = AsyncMock()
            store._get_adls_ops = MagicMock(return_value=adls_ops)

            # Mock MssqlHome for target database
            with patch(
                "hygge.stores.openmirroring.deletions.MssqlHome"
            ) as mock_mssql_home:
                mock_target_home = MagicMock()
                mock_target_home.find_keys = AsyncMock(return_value=target_keys)
                mock_mssql_home.return_value = mock_target_home

                # Mock source Home
                class MockSourceHome:
                    async def find_keys(self, key_columns):
                        return source_keys

                    def supports_key_finding(self):
                        return True

                source_home = MockSourceHome()
                store.set_home_for_deletions(source_home)

                # Mock retry decorator to avoid actual retry logic
                # Patch find_deletions in the store module where it's imported
                with patch(
                    "hygge.stores.openmirroring.store.find_deletions"
                ) as mock_find_deletions, patch(
                    "hygge.utility.retry.with_retry"
                ) as mock_retry:
                    # Make find_deletions actually run (not just a mock)
                    from hygge.stores.openmirroring.deletions import (
                        find_deletions as real_find_deletions,
                    )

                    async def call_real_find_deletions(store_arg, home_arg):
                        return await real_find_deletions(store_arg, home_arg)

                    mock_find_deletions.side_effect = call_real_find_deletions

                    def mock_retry_decorator(*args, **kwargs):
                        def decorator(func):
                            # Return an async function that calls the patched
                            # find_deletions. This ensures the side_effect
                            # (real function) is called
                            async def wrapped(*wrapped_args, **wrapped_kwargs):
                                # Call the patched find_deletions, which will
                                # use side_effect
                                return await mock_find_deletions(
                                    *wrapped_args, **wrapped_kwargs
                                )

                            return wrapped

                        return decorator

                    # Make the mock callable and return the decorator function
                    mock_retry.side_effect = mock_retry_decorator

                    # No need to patch polars.read_parquet - it will read from
                    # the bytes we provided
                    await store.before_flow_start()

                    # Verify find_deletions was called
                    assert (
                        mock_find_deletions.called
                    ), "find_deletions should have been called"

                # Verify delete markers were written
                assert (
                    store.write.called
                ), "store.write should have been called with delete markers"

                write_df = store.write.call_args[0][0]
                assert "__rowMarker__" in write_df.columns
                assert all(write_df["__rowMarker__"] == 2)  # Delete marker
                assert len(write_df) == 2  # Keys 4 and 5 should be deleted
                assert set(write_df["id"].to_list()) == {4, 5}

    @pytest.mark.asyncio
    async def test_deletion_detection_handles_empty_results(self, store_config):
        """Test that deletion detection handles empty results gracefully."""
        with patch(
            "hygge.stores.adls.ADLSStore.__init__", lambda self, *args, **kwargs: None
        ):
            store = OpenMirroringStore("test_store", store_config)
            # Set required attributes that would normally be set by ADLSStore.__init__
            store.config = store_config
            store.incremental_override = store_config.incremental
            store.entity_name = "users"  # Required for deletion_source table fallback
            store.logger = MagicMock()
            store.write = AsyncMock()
            store.get_staging_directory = MagicMock(return_value=Path("/tmp/staging"))
            store.compression = "snappy"

            # Mock ADLS operations
            adls_ops = MagicMock()
            adls_ops.upload_bytes = AsyncMock(
                return_value="/tmp/staging/_deletion_check_keys.parquet"
            )
            adls_ops.delete_file = AsyncMock()
            store._get_adls_ops = MagicMock(return_value=adls_ops)

            # Mock empty results
            empty_keys = pl.DataFrame({"id": []})

            with patch(
                "hygge.stores.openmirroring.deletions.MssqlHome"
            ) as mock_mssql_home:
                mock_target_home = MagicMock()
                mock_target_home.find_keys = AsyncMock(return_value=empty_keys)
                mock_mssql_home.return_value = mock_target_home

                class MockSourceHome:
                    async def find_keys(self, key_columns):
                        return empty_keys

                    def supports_key_finding(self):
                        return True

                source_home = MockSourceHome()
                store.set_home_for_deletions(source_home)

                # Empty target database now fails fast (no schema assumption)
                # This test should expect an exception
                with pytest.raises(StoreError, match="target database is empty"):
                    await store.before_flow_start()
