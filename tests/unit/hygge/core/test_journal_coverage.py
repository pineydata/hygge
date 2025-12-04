"""
Additional comprehensive tests for Journal to improve coverage.

Focuses on error handling, edge cases, and complex methods that are currently
under-tested. This file supplements test_journal.py.
"""
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from hygge.core.journal import Journal, JournalConfig
from hygge.utility.exceptions import ConfigError, JournalWriteError


@pytest.fixture
def temp_journal_dir(temp_dir):
    """Create temporary directory for journal files."""
    journal_dir = temp_dir / ".hygge_journal"
    journal_dir.mkdir(parents=True, exist_ok=True)
    return journal_dir


@pytest.fixture
def journal(temp_journal_dir):
    """Create a journal instance for testing."""
    config = JournalConfig(path=str(temp_journal_dir))
    return Journal(
        "test_journal",
        config,
        coordinator_name="test_coordinator",
        store_path=None,
        home_path=None,
    )


class TestJournalErrorHandling:
    """Test error handling paths in Journal."""

    def test_append_local_journal_handles_write_failure(self, temp_dir):
        """Test local journal append handles write failures."""
        journal_dir = temp_dir / ".hygge_journal"
        journal_dir.mkdir(parents=True, exist_ok=True)
        journal_path = journal_dir / "journal.parquet"

        config = JournalConfig(path=str(journal_dir))
        journal = Journal(
            "test_journal",
            config,
            coordinator_name="test_coordinator",
        )

        # Create existing journal file
        existing_df = pl.DataFrame(
            {
                "entity_run_id": ["existing_id"],
                "coordinator_run_id": ["coord_id"],
                "flow_run_id": ["flow_id"],
                "coordinator": ["test_coordinator"],
                "flow": ["test_flow"],
                "entity": ["test_entity"],
                "start_time": ["2024-01-01T10:00:00Z"],
                "finish_time": ["2024-01-01T10:05:00Z"],
                "status": ["success"],
                "run_type": ["full_drop"],
                "row_count": [1000],
                "duration": [5.0],
                "primary_key": [None],
                "watermark_column": [None],
                "watermark_type": [None],
                "watermark": [None],
                "message": [None],
                "schema_version": ["1.0"],
            },
            schema=Journal.JOURNAL_SCHEMA,
        )
        existing_df.write_parquet(journal_path)

        new_row_df = pl.DataFrame(
            {
                "entity_run_id": ["new_id"],
                "coordinator_run_id": ["coord_id"],
                "flow_run_id": ["flow_id"],
                "coordinator": ["test_coordinator"],
                "flow": ["test_flow"],
                "entity": ["test_entity"],
                "start_time": ["2024-01-01T11:00:00Z"],
                "finish_time": ["2024-01-01T11:05:00Z"],
                "status": ["success"],
                "run_type": ["incremental"],
                "row_count": [500],
                "duration": [3.0],
                "primary_key": [None],
                "watermark_column": [None],
                "watermark_type": [None],
                "watermark": [None],
                "message": [None],
                "schema_version": ["1.0"],
            },
            schema=Journal.JOURNAL_SCHEMA,
        )

        # Mock write_parquet to raise an error
        with patch(
            "polars.DataFrame.write_parquet", side_effect=Exception("Write failed")
        ):
            with pytest.raises(
                JournalWriteError, match="Failed to append to local journal"
            ):
                journal._append_local_journal(new_row_df)

    @pytest.mark.asyncio
    async def test_append_remote_journal_handles_read_failure(self):
        """Test remote journal append handles read failures."""
        mock_store_config = MagicMock()
        mock_store_config.type = "adls"
        mock_store_config.account_url = "https://mystorage.dfs.core.windows.net"
        mock_store_config.filesystem = "mycontainer"
        mock_store_config.credential = "managed_identity"

        config = JournalConfig(location="store")
        journal = Journal(
            "test_journal",
            config,
            coordinator_name="test_coordinator",
            store_config=mock_store_config,
        )

        mock_adls = AsyncMock()
        mock_adls.file_exists = AsyncMock(return_value=True)
        mock_adls.read_file_bytes = AsyncMock(side_effect=Exception("Read failed"))

        with patch.object(journal, "adls_ops", mock_adls):
            new_row_df = pl.DataFrame(
                {
                    "entity_run_id": ["new_id"],
                    "coordinator_run_id": ["coord_id"],
                    "flow_run_id": ["flow_id"],
                    "coordinator": ["test_coordinator"],
                    "flow": ["test_flow"],
                    "entity": ["test_entity"],
                    "start_time": ["2024-01-01T11:00:00Z"],
                    "finish_time": ["2024-01-01T11:05:00Z"],
                    "status": ["success"],
                    "run_type": ["incremental"],
                    "row_count": [500],
                    "duration": [3.0],
                    "primary_key": [None],
                    "watermark_column": [None],
                    "watermark_type": [None],
                    "watermark": [None],
                    "message": [None],
                    "schema_version": ["1.0"],
                },
                schema=Journal.JOURNAL_SCHEMA,
            )

            # Read failure should propagate (code doesn't handle it gracefully)
            with pytest.raises(Exception, match="Read failed"):
                await journal._append_remote_journal(new_row_df)

    @pytest.mark.asyncio
    async def test_append_remote_journal_handles_move_failure(self):
        """Test remote journal append handles move operation failure."""
        mock_store_config = MagicMock()
        mock_store_config.type = "adls"
        mock_store_config.account_url = "https://mystorage.dfs.core.windows.net"
        mock_store_config.filesystem = "mycontainer"
        mock_store_config.credential = "managed_identity"

        config = JournalConfig(location="store")
        journal = Journal(
            "test_journal",
            config,
            coordinator_name="test_coordinator",
            store_config=mock_store_config,
        )

        mock_adls = AsyncMock()
        mock_adls.file_exists = AsyncMock(return_value=False)
        mock_adls.create_directory_recursive = AsyncMock()
        mock_adls.upload_bytes = AsyncMock()
        mock_adls.move_file = AsyncMock(side_effect=Exception("Move failed"))

        with patch.object(journal, "adls_ops", mock_adls):
            new_row_df = pl.DataFrame(
                {
                    "entity_run_id": ["new_id"],
                    "coordinator_run_id": ["coord_id"],
                    "flow_run_id": ["flow_id"],
                    "coordinator": ["test_coordinator"],
                    "flow": ["test_flow"],
                    "entity": ["test_entity"],
                    "start_time": ["2024-01-01T11:00:00Z"],
                    "finish_time": ["2024-01-01T11:05:00Z"],
                    "status": ["success"],
                    "run_type": ["incremental"],
                    "row_count": [500],
                    "duration": [3.0],
                    "primary_key": [None],
                    "watermark_column": [None],
                    "watermark_type": [None],
                    "watermark": [None],
                    "message": [None],
                    "schema_version": ["1.0"],
                },
                schema=Journal.JOURNAL_SCHEMA,
            )

            # Should propagate move failure
            with pytest.raises(Exception, match="Move failed"):
                await journal._append_remote_journal(new_row_df)


class TestJournalRemoteStorage:
    """Test remote storage configuration and operations."""

    def test_setup_remote_storage_requires_store_config(self):
        """Test remote storage setup requires store config."""
        config = JournalConfig(location="store")
        with pytest.raises(ConfigError, match="Journal location='store' requires"):
            Journal(
                "test_journal",
                config,
                coordinator_name="test_coordinator",
                store_path=None,
                store_config=None,
            )

    def test_setup_remote_storage_requires_account_url(self):
        """Test remote storage setup requires account_url."""
        mock_store_config = MagicMock()
        mock_store_config.type = "adls"
        mock_store_config.account_url = None  # Missing
        mock_store_config.filesystem = "mycontainer"

        config = JournalConfig(location="store")
        with pytest.raises(ConfigError, match="account_url and filesystem"):
            Journal(
                "test_journal",
                config,
                coordinator_name="test_coordinator",
                store_config=mock_store_config,
            )

    def test_setup_remote_storage_requires_filesystem(self):
        """Test remote storage setup requires filesystem."""
        mock_store_config = MagicMock()
        mock_store_config.type = "adls"
        mock_store_config.account_url = "https://mystorage.dfs.core.windows.net"
        mock_store_config.filesystem = None  # Missing

        config = JournalConfig(location="store")
        with pytest.raises(ConfigError, match="account_url and filesystem"):
            Journal(
                "test_journal",
                config,
                coordinator_name="test_coordinator",
                store_config=mock_store_config,
            )

    @patch("azure.storage.filedatalake.DataLakeServiceClient")
    @patch("azure.identity.ManagedIdentityCredential")
    def test_create_adls_service_client_managed_identity(self, mock_cred, mock_client):
        """Test ADLS service client creation with managed identity."""
        Journal._create_adls_service_client(
            account_url="https://mystorage.dfs.core.windows.net",
            credential_type="managed_identity",
            tenant_id=None,
            client_id=None,
            client_secret=None,
            storage_account_key=None,
        )

        mock_cred.assert_called_once()
        mock_client.assert_called_once()

    @patch("azure.storage.filedatalake.DataLakeServiceClient")
    @patch("azure.identity.ClientSecretCredential")
    def test_create_adls_service_client_service_principal(self, mock_cred, mock_client):
        """Test ADLS service client creation with service principal."""
        Journal._create_adls_service_client(
            account_url="https://mystorage.dfs.core.windows.net",
            credential_type="service_principal",
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
            storage_account_key=None,
        )

        mock_cred.assert_called_once_with(
            tenant_id="test-tenant",
            client_id="test-client",
            client_secret="test-secret",
        )
        mock_client.assert_called_once()

    def test_create_adls_service_client_service_principal_missing_params(self):
        """Test service principal requires all params."""
        with pytest.raises(
            ConfigError, match="Service principal authentication requires"
        ):
            Journal._create_adls_service_client(
                account_url="https://mystorage.dfs.core.windows.net",
                credential_type="service_principal",
                tenant_id="test-tenant",
                client_id=None,  # Missing
                client_secret=None,  # Missing
                storage_account_key=None,
            )

    @patch("azure.storage.filedatalake.DataLakeServiceClient")
    @patch("azure.core.credentials.AzureNamedKeyCredential")
    def test_create_adls_service_client_storage_key(self, mock_named_key, mock_client):
        """Test ADLS service client creation with storage key."""
        Journal._create_adls_service_client(
            account_url="https://mystorage.dfs.core.windows.net",
            credential_type="storage_key",
            tenant_id=None,
            client_id=None,
            client_secret=None,
            storage_account_key="test-key",
        )

        # Should extract account name from URL
        mock_named_key.assert_called_once()
        call_args = mock_named_key.call_args
        assert call_args[1]["name"] == "mystorage"
        assert call_args[1]["key"] == "test-key"
        mock_client.assert_called_once()

    def test_create_adls_service_client_storage_key_missing_key(self):
        """Test storage key requires key."""
        with pytest.raises(ConfigError, match="Storage key authentication requires"):
            Journal._create_adls_service_client(
                account_url="https://mystorage.dfs.core.windows.net",
                credential_type="storage_key",
                tenant_id=None,
                client_id=None,
                client_secret=None,
                storage_account_key=None,  # Missing
            )

    @patch("azure.storage.filedatalake.DataLakeServiceClient")
    @patch("azure.identity.DefaultAzureCredential")
    def test_create_adls_service_client_default_credential(
        self, mock_default, mock_client
    ):
        """Test ADLS service client creation with default credential."""
        Journal._create_adls_service_client(
            account_url="https://mystorage.dfs.core.windows.net",
            credential_type="default",
            tenant_id=None,
            client_id=None,
            client_secret=None,
            storage_account_key=None,
        )

        mock_default.assert_called_once()
        mock_client.assert_called_once()

    def test_create_adls_service_client_handles_import_error(self):
        """Test service client creation handles import errors."""
        with patch(
            "azure.identity.ManagedIdentityCredential",
            side_effect=ImportError("Missing package"),
        ):
            with pytest.raises(
                ConfigError,
                match=(
                    "azure-identity and azure-storage-filedatalake "
                    "packages are required"
                ),
            ):
                Journal._create_adls_service_client(
                    account_url="https://mystorage.dfs.core.windows.net",
                    credential_type="managed_identity",
                    tenant_id=None,
                    client_id=None,
                    client_secret=None,
                    storage_account_key=None,
                )


class TestJournalWatermarkEdgeCases:
    """Test watermark query edge cases."""

    @pytest.mark.asyncio
    async def test_get_watermark_validates_primary_key_mismatch(self, journal):
        """Test get_watermark() validates primary_key mismatch."""
        start_time = datetime.now(timezone.utc)
        finish_time = datetime.now(timezone.utc)

        # Record run with primary_key="user_id"
        await journal.record_entity_run(
            coordinator_run_id="coord_id",
            flow_run_id="flow_id",
            coordinator="test_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time,
            finish_time=finish_time,
            status="success",
            run_type="incremental",
            row_count=1000,
            duration=5.0,
            primary_key="user_id",
            watermark_column="updated_at",
            watermark_type="datetime",
            watermark="2024-01-01T09:00:00Z",
        )

        # Query with different primary_key
        with pytest.raises(ValueError, match="Watermark config mismatch"):
            await journal.get_watermark(
                "users_flow",
                "users",
                primary_key="id",  # Different from stored
                watermark_column="updated_at",
            )

    @pytest.mark.asyncio
    async def test_get_watermark_validates_watermark_column_mismatch(self, journal):
        """Test get_watermark() validates watermark_column mismatch."""
        start_time = datetime.now(timezone.utc)
        finish_time = datetime.now(timezone.utc)

        # Record run with watermark_column="updated_at"
        await journal.record_entity_run(
            coordinator_run_id="coord_id",
            flow_run_id="flow_id",
            coordinator="test_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time,
            finish_time=finish_time,
            status="success",
            run_type="incremental",
            row_count=1000,
            duration=5.0,
            primary_key="user_id",
            watermark_column="updated_at",
            watermark_type="datetime",
            watermark="2024-01-01T09:00:00Z",
        )

        # Query with different watermark_column
        with pytest.raises(ValueError, match="Watermark config mismatch"):
            await journal.get_watermark(
                "users_flow",
                "users",
                primary_key="user_id",
                watermark_column="created_at",  # Different from stored
            )

    @pytest.mark.asyncio
    async def test_get_watermark_filters_successful_runs_only(self, journal):
        """Test get_watermark() only considers successful runs."""
        start_time = datetime.now(timezone.utc)
        finish_time = datetime.now(timezone.utc)

        # Record failed run with watermark
        await journal.record_entity_run(
            coordinator_run_id="coord_id",
            flow_run_id="flow_id",
            coordinator="test_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time,
            finish_time=finish_time,
            status="fail",  # Failed run
            run_type="incremental",
            row_count=0,
            duration=1.0,
            primary_key="user_id",
            watermark_column="updated_at",
            watermark_type="datetime",
            watermark="2024-01-01T09:00:00Z",
        )

        # Should return None (no successful runs)
        watermark = await journal.get_watermark(
            "users_flow",
            "users",
            primary_key="user_id",
            watermark_column="updated_at",
        )

        assert watermark is None

    @pytest.mark.asyncio
    async def test_get_watermark_returns_most_recent(self, journal):
        """Test get_watermark() returns most recent watermark."""
        start_time = datetime.now(timezone.utc)

        # Record two successful runs with different watermarks
        await journal.record_entity_run(
            coordinator_run_id="coord_id",
            flow_run_id="flow_id",
            coordinator="test_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time,
            finish_time=datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
            status="success",
            run_type="incremental",
            row_count=1000,
            duration=5.0,
            primary_key="user_id",
            watermark_column="updated_at",
            watermark_type="datetime",
            watermark="2024-01-01T09:00:00Z",
        )

        await journal.record_entity_run(
            coordinator_run_id="coord_id",
            flow_run_id="flow_id",
            coordinator="test_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time,
            finish_time=datetime(2024, 1, 1, 11, 0, 0, tzinfo=timezone.utc),  # Later
            status="success",
            run_type="incremental",
            row_count=1500,
            duration=6.0,
            primary_key="user_id",
            watermark_column="updated_at",
            watermark_type="datetime",
            watermark="2024-01-01T10:00:00Z",  # Later watermark
        )

        # Should return most recent watermark
        watermark = await journal.get_watermark(
            "users_flow",
            "users",
            primary_key="user_id",
            watermark_column="updated_at",
        )

        assert watermark["watermark"] == "2024-01-01T10:00:00Z"


class TestJournalSummaryEdgeCases:
    """Test summary aggregation edge cases."""

    @pytest.mark.asyncio
    async def test_get_flow_summary_with_mixed_statuses(self, journal):
        """Test flow summary with mixed entity statuses."""
        start_time = datetime.now(timezone.utc)
        finish_time = datetime.now(timezone.utc)
        flow_run_id = "flow_id"

        # Record entities with different statuses
        await journal.record_entity_run(
            coordinator_run_id="coord_id",
            flow_run_id=flow_run_id,
            coordinator="test_coordinator",
            flow="users_flow",
            entity="users",
            start_time=start_time,
            finish_time=finish_time,
            status="success",
            run_type="full_drop",
            row_count=1000,
            duration=5.0,
        )

        await journal.record_entity_run(
            coordinator_run_id="coord_id",
            flow_run_id=flow_run_id,
            coordinator="test_coordinator",
            flow="users_flow",
            entity="orders",
            start_time=start_time,
            finish_time=finish_time,
            status="fail",
            run_type="full_drop",
            row_count=0,
            duration=1.0,
        )

        await journal.record_entity_run(
            coordinator_run_id="coord_id",
            flow_run_id=flow_run_id,
            coordinator="test_coordinator",
            flow="users_flow",
            entity="products",
            start_time=start_time,
            finish_time=finish_time,
            status="skip",
            run_type="full_drop",
            row_count=0,
            duration=0.0,
        )

        summary = await journal.get_flow_summary(flow_run_id)

        assert summary["n_entities"] == 3
        assert summary["n_success"] == 1
        assert summary["n_fail"] == 1
        assert summary["n_skip"] == 1

    @pytest.mark.asyncio
    async def test_get_coordinator_summary_with_multiple_flows(self, journal):
        """Test coordinator summary with multiple flows."""
        start_time = datetime.now(timezone.utc)
        finish_time = datetime.now(timezone.utc)
        coordinator_run_id = "coord_id"

        # Record runs for multiple flows
        flows = ["users_flow", "orders_flow", "products_flow"]
        for flow in flows:
            flow_run_id = f"{flow}_run_id"
            await journal.record_entity_run(
                coordinator_run_id=coordinator_run_id,
                flow_run_id=flow_run_id,
                coordinator="test_coordinator",
                flow=flow,
                entity="entity1",
                start_time=start_time,
                finish_time=finish_time,
                status="success",
                run_type="full_drop",
                row_count=1000,
                duration=5.0,
            )

        summary = await journal.get_coordinator_summary(coordinator_run_id)

        assert summary["n_flows"] == 3
        assert summary["n_entities"] == 1  # All flows have same entity name
