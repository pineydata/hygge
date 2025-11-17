"""
Unit tests for Open Mirroring Store implementation.

Tests configuration, path management, row marker handling, metadata files,
and Open Mirroring specific features without requiring an actual OneLake account.
Integration tests should be in tests/integration/.
"""
import json
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest
from pydantic import ValidationError

from hygge.stores.openmirroring import (
    OpenMirroringStore,
    OpenMirroringStoreConfig,
)


class TestOpenMirroringStoreConfig:
    """Test Open Mirroring store configuration validation."""

    def test_config_with_minimal_required_fields(self):
        """Test config creation with minimal required fields."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )

        assert config.type == "open_mirroring"
        assert config.account_url == "https://onelake.dfs.fabric.microsoft.com"
        assert config.filesystem == "MyLake"
        assert config.mirror_name == "MyMirror"
        assert config.key_columns == ["id"]
        assert config.row_marker == 0
        assert config.file_detection == "timestamp"  # Default
        assert config.folder_deletion_wait_seconds == 2.0  # Default
        assert config.mirror_journal is False
        assert config.journal_table_name == "__hygge_journal"
        assert config.incremental is None

    def test_config_path_auto_built_for_landing_zone(self):
        """Test that path is auto-built to LandingZone if not provided."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            schema_name=None,
            # path not provided - should auto-build to Files/LandingZone/{entity}/
        )

        # Using schema_name to avoid Pydantic method shadowing
        # We test the path building logic, which should produce LandingZone path
        assert "LandingZone" in config.path
        assert "{entity}" in config.path

    def test_config_path_auto_built_with_schema(self):
        """Test that path is auto-built with schema support."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            schema_name="dbo",
            key_columns=["id"],
            row_marker=0,
        )

        # Path should include LandingZone and schema
        assert "LandingZone" in config.path
        assert "dbo.schema" in config.path
        assert "{entity}" in config.path

    def test_config_with_all_fields(self):
        """Test config creation with all optional fields."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id", "user_id"],
            row_marker=4,
            file_detection="sequential",
            folder_deletion_wait_seconds=5.0,
            partner_name="MyOrg",
            source_type="SQL",
            source_version="2019",
            starting_sequence=100,
            mirror_journal=True,
            journal_table_name="custom_journal",
            incremental=False,
        )

        assert config.row_marker == 4  # Upsert
        assert config.file_detection == "sequential"
        assert config.folder_deletion_wait_seconds == 5.0
        assert config.partner_name == "MyOrg"
        assert config.source_type == "SQL"
        assert config.starting_sequence == 100
        assert config.mirror_journal is True
        assert config.journal_table_name == "custom_journal"
        assert config.incremental is False

    def test_config_requires_mirror_name(self):
        """Test that mirror_name is required."""
        with pytest.raises(ValidationError) as exc_info:
            OpenMirroringStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                key_columns=["id"],
                row_marker=0,
                # mirror_name missing
            )
        assert "mirror_name" in str(exc_info.value).lower()

    def test_config_allows_optional_key_columns(self):
        """Test that key_columns is optional at config level.

        Validation happens at store creation, not config creation.
        """
        # Should not raise validation error at config level
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            row_marker=0,
            # key_columns missing - OK at config level
        )
        assert config.key_columns is None

    def test_config_key_columns_string_conversion(self):
        """Test that key_columns string is converted to list."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns="id",  # String format
            row_marker=0,
        )
        assert config.key_columns == ["id"]  # Converted to list

    def test_config_key_columns_list_unchanged(self):
        """Test that key_columns list stays as list."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id", "user_id"],  # List format
            row_marker=0,
        )
        assert config.key_columns == ["id", "user_id"]

    def test_config_key_columns_invalid_type(self):
        """Test that invalid key_columns type raises error."""
        with pytest.raises(ValidationError):
            OpenMirroringStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                mirror_name="MyMirror",
                key_columns=123,  # Invalid type
                row_marker=0,
            )

    def test_config_requires_row_marker(self):
        """Test that row_marker is required."""
        with pytest.raises(ValidationError) as exc_info:
            OpenMirroringStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                mirror_name="MyMirror",
                key_columns=["id"],
                # row_marker missing
            )
        assert "row_marker" in str(exc_info.value).lower()

    def test_config_validates_file_detection(self):
        """Test that file_detection must be 'timestamp' or 'sequential'."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            file_detection="timestamp",
        )
        assert config.file_detection == "timestamp"

        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            file_detection="sequential",
        )
        assert config.file_detection == "sequential"

        with pytest.raises(ValidationError):
            OpenMirroringStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                mirror_name="MyMirror",
                key_columns=["id"],
                row_marker=0,
                file_detection="invalid",
            )

    def test_config_validates_row_marker(self):
        """Test that row_marker must be 0, 1, 2, or 4."""
        for valid_marker in [0, 1, 2, 4]:
            config = OpenMirroringStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                mirror_name="MyMirror",
                key_columns=["id"],
                row_marker=valid_marker,
            )
            assert config.row_marker == valid_marker

        with pytest.raises(ValidationError):
            OpenMirroringStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                mirror_name="MyMirror",
                key_columns=["id"],
                row_marker=3,  # Invalid
            )

        with pytest.raises(ValidationError):
            OpenMirroringStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                mirror_name="MyMirror",
                key_columns=["id"],
                row_marker=5,  # Invalid
            )

    def test_config_validates_folder_deletion_wait_seconds(self):
        """Test that folder_deletion_wait_seconds must be between 0 and 60."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            folder_deletion_wait_seconds=0.0,
        )
        assert config.folder_deletion_wait_seconds == 0.0

        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            folder_deletion_wait_seconds=60.0,
        )
        assert config.folder_deletion_wait_seconds == 60.0

        with pytest.raises(ValidationError):
            OpenMirroringStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                mirror_name="MyMirror",
                key_columns=["id"],
                row_marker=0,
                folder_deletion_wait_seconds=-1.0,  # Invalid
            )

        with pytest.raises(ValidationError):
            OpenMirroringStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                mirror_name="MyMirror",
                key_columns=["id"],
                row_marker=0,
                folder_deletion_wait_seconds=61.0,  # Invalid
            )


class TestOpenMirroringStoreInitialization:
    """Test Open Mirroring store initialization."""

    def test_store_initialization(self):
        """Test Open Mirroring store initializes correctly."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            schema_name=None,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        assert store.name == "test_store"
        assert store.config == config
        assert store.key_columns == ["id"]
        assert store.row_marker == 0
        assert store.file_detection == "timestamp"
        assert store.full_drop_mode is False
        assert "LandingZone" in store.base_path
        assert "users" in store.base_path

    def test_store_with_schema(self):
        """Test Open Mirroring store with schema."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            schema_name="dbo",
            key_columns=["id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        assert store.schema_name == "dbo"
        assert "LandingZone" in store.base_path
        assert "dbo.schema" in store.base_path
        assert "users" in store.base_path

    def test_store_full_drop_run_configuration(self):
        """Test Open Mirroring store toggles truncate behaviour per run."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        # Default is incremental/append behaviour
        assert store.full_drop_mode is False
        assert store.incremental_override is None

        # Configure full_drop run
        store.configure_for_run("full_drop")
        assert store.full_drop_mode is True

        # Switching back to incremental should disable truncate mode
        store.configure_for_run("incremental")
        assert store.full_drop_mode is False

    def test_store_incremental_disabled_forces_full_drop(self):
        """When incremental is disabled, treat every run as full_drop."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            incremental=False,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.configure_for_run("incremental")
        assert store.full_drop_mode is True
        assert store.incremental_override is False

    def test_store_requires_key_columns_at_initialization(self):
        """Test that store raises error when key_columns is None."""
        from hygge.utility.exceptions import StoreError

        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            row_marker=0,
            # key_columns is None
        )

        with pytest.raises(StoreError) as exc_info:
            OpenMirroringStore("test_store", config, entity_name="users")

        assert "key_columns is required" in str(exc_info.value).lower()
        assert "entity" in str(exc_info.value).lower()

    def test_config_rejects_empty_list_key_columns(self):
        """Test that config validation rejects empty list key_columns."""
        with pytest.raises(ValidationError) as exc_info:
            OpenMirroringStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                mirror_name="MyMirror",
                key_columns=[],  # Empty list
                row_marker=0,
            )
        assert "empty list" in str(exc_info.value).lower()

    def test_config_rejects_empty_string_key_columns(self):
        """Test that config validation rejects empty string key_columns."""
        with pytest.raises(ValidationError) as exc_info:
            OpenMirroringStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                mirror_name="MyMirror",
                key_columns="",  # Empty string
                row_marker=0,
            )
        assert "empty string" in str(exc_info.value).lower()

    def test_store_initialization_with_string_key_columns(self):
        """Test that store accepts string key_columns (converted to list)."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns="id",  # String format
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")
        assert store.key_columns == ["id"]  # Should be converted to list


class TestOpenMirroringStoreRowMarker:
    """Test row marker column handling."""

    def test_add_row_marker_column_missing(self):
        """Test adding row marker column when missing."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=4,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        result = store._add_row_marker_column(df)

        assert "__rowMarker__" in result.columns
        assert "__LastLoadedAt__" in result.columns
        # __rowMarker__ should be last
        assert result.columns[-1] == "__rowMarker__"
        # __LastLoadedAt__ should be second-to-last
        assert result.columns[-2] == "__LastLoadedAt__"
        # All row marker values should be 4 (Upsert)
        assert result["__rowMarker__"].to_list() == [4, 4, 4]

    def test_add_row_marker_column_existing(self):
        """Test row marker column when already present."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "__rowMarker__": [0, 0, 0],
            }
        )

        result = store._add_row_marker_column(df)

        # Should still ensure correct column order
        assert result.columns[-1] == "__rowMarker__"
        assert result.columns[-2] == "__LastLoadedAt__"

    def test_ensure_row_marker_last(self):
        """Test that __rowMarker__ is always last column."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        # Create DataFrame with __rowMarker__ not last
        df = pl.DataFrame(
            {
                "__rowMarker__": [0, 0],
                "id": [1, 2],
                "name": ["a", "b"],
            }
        )

        result = store._ensure_row_marker_last(df)

        # __rowMarker__ should be last
        assert result.columns[-1] == "__rowMarker__"
        assert "id" in result.columns
        assert "name" in result.columns


class TestOpenMirroringStoreFileNaming:
    """Test file naming strategies."""

    @pytest.mark.asyncio
    async def test_get_next_filename_timestamp(self):
        """Test timestamp-based file naming."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            file_detection="timestamp",
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.sequence_counter = 5

        filename = await store.get_next_filename()

        # Should be timestamp_microseconds_000006.parquet format
        # (increments before returning)
        assert filename.endswith(".parquet")
        # Should have timestamp prefix, microseconds, and sequence
        # (which was incremented to 6)
        parts = filename.replace(".parquet", "").split("_")
        assert len(parts) == 4  # date, time, microseconds, sequence
        assert parts[3] == "000006"  # Sequence is now 4th part (index 3)
        # Sequence counter should be incremented
        assert store.sequence_counter == 6

    @pytest.mark.asyncio
    async def test_get_next_filename_sequential(self):
        """Test sequential file naming."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            file_detection="sequential",
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.sequence_counter = 42

        filename = await store.get_next_filename()

        # Should be 20-digit sequential: 00000000000000000043.parquet
        assert filename == "00000000000000000043.parquet"
        # Sequence counter should be incremented
        assert store.sequence_counter == 43


class TestOpenMirroringStoreMetadata:
    """Test metadata file writing."""

    @pytest.mark.asyncio
    async def test_write_metadata_json_basic(self):
        """Test writing basic metadata JSON."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        # Mock ADLS operations
        mock_adls = AsyncMock()
        mock_adls.read_json = AsyncMock(return_value=None)  # File doesn't exist
        mock_adls.file_system_client = MagicMock()
        mock_file_client = MagicMock()
        mock_adls.file_system_client.get_file_client = MagicMock(
            return_value=mock_file_client
        )
        mock_adls.timeout = 300

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            await store._write_metadata_json()

        # Should have written metadata
        assert store._metadata_written is True
        # Metadata write should occur; schema write follows, so expect >=1 call
        assert mock_file_client.upload_data.call_count >= 1
        metadata_payload = json.loads(
            mock_file_client.upload_data.call_args_list[0][0][0].decode("utf-8")
        )
        assert metadata_payload["keyColumns"] == ["id"]
        # file_detection default "timestamp", so include fileDetectionStrategy
        assert (
            metadata_payload["fileDetectionStrategy"] == "LastUpdateTimeFileDetection"
        )
        assert "isUpsertDefaultRowMarker" not in metadata_payload  # Not Upsert

    @pytest.mark.asyncio
    async def test_write_metadata_json_with_timestamp_detection(self):
        """Test metadata JSON with timestamp file detection."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            file_detection="timestamp",
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        # Mock ADLS operations
        mock_adls = AsyncMock()
        mock_adls.read_json = AsyncMock(return_value=None)
        mock_adls.file_system_client = MagicMock()
        mock_file_client = MagicMock()
        mock_adls.file_system_client.get_file_client = MagicMock(
            return_value=mock_file_client
        )
        mock_adls.timeout = 300

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            await store._write_metadata_json()

        # Check metadata includes fileDetectionStrategy
        metadata = json.loads(
            mock_file_client.upload_data.call_args_list[0][0][0].decode("utf-8")
        )
        assert metadata["fileDetectionStrategy"] == "LastUpdateTimeFileDetection"

    @pytest.mark.asyncio
    async def test_write_metadata_json_with_upsert(self):
        """Test metadata JSON with Upsert row marker."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=4,  # Upsert
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        # Mock ADLS operations
        mock_adls = AsyncMock()
        mock_adls.read_json = AsyncMock(return_value=None)
        mock_adls.file_system_client = MagicMock()
        mock_file_client = MagicMock()
        mock_adls.file_system_client.get_file_client = MagicMock(
            return_value=mock_file_client
        )
        mock_adls.timeout = 300

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            await store._write_metadata_json()

        # Check metadata includes isUpsertDefaultRowMarker
        metadata = json.loads(
            mock_file_client.upload_data.call_args_list[0][0][0].decode("utf-8")
        )
        assert metadata["isUpsertDefaultRowMarker"] is True

    @pytest.mark.asyncio
    async def test_write_metadata_json_validates_existing_keycolumns(self):
        """Test that existing metadata with different keyColumns raises error."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id", "user_id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        # Mock existing metadata with different keyColumns
        mock_adls = AsyncMock()
        mock_adls.read_json = AsyncMock(
            return_value={"keyColumns": ["id"]}  # Different!
        )

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            with pytest.raises(Exception) as exc_info:
                await store._write_metadata_json()

        # Error message should mention keyColumns (case-insensitive check)
        error_msg = str(exc_info.value).lower()
        assert "keycolumn" in error_msg or "key column" in error_msg

    @pytest.mark.asyncio
    async def test_write_metadata_json_skips_if_written(self):
        """Test that metadata write is skipped if already written."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")
        store._metadata_written = True

        mock_adls = AsyncMock()

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            await store._write_metadata_json()

        # Should not have called read_json or write
        mock_adls.read_json.assert_not_called()


class TestOpenMirroringStoreSchemaManifest:
    """Test schema manifest (_schema.json) file writing."""

    @pytest.mark.asyncio
    async def test_write_schema_json_basic(self):
        """Test writing basic schema JSON with journal columns."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        # Mock ADLS operations
        mock_adls = AsyncMock()
        mock_adls.file_system_client = MagicMock()
        mock_directory_client = MagicMock()
        mock_directory_client.exists.return_value = True
        mock_file_client = MagicMock()
        mock_adls.file_system_client.get_directory_client = MagicMock(
            return_value=mock_directory_client
        )
        mock_adls.file_system_client.get_file_client = MagicMock(
            return_value=mock_file_client
        )
        mock_adls.timeout = 300

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            await store._write_schema_json(to_tmp=False)

        # Verify schema file was written
        assert mock_file_client.upload_data.called
        schema_payload = json.loads(
            mock_file_client.upload_data.call_args[0][0].decode("utf-8")
        )

        # Verify schema structure
        assert "columns" in schema_payload
        assert isinstance(schema_payload["columns"], list)

        # Verify all journal schema columns are present
        from hygge.core.journal import Journal

        expected_columns = set(Journal.JOURNAL_SCHEMA.keys())
        actual_columns = {col["name"] for col in schema_payload["columns"]}
        assert expected_columns == actual_columns

        # Verify column types are mapped correctly
        column_map = {col["name"]: col["type"] for col in schema_payload["columns"]}
        assert column_map["entity_run_id"] == "string"
        assert column_map["row_count"] == "long"
        assert column_map["duration"] == "double"

    @pytest.mark.asyncio
    async def test_write_schema_json_to_tmp(self):
        """Test writing schema JSON to _tmp in full_drop mode."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        # Mock ADLS operations
        mock_adls = AsyncMock()
        mock_adls.file_system_client = MagicMock()
        mock_directory_client = MagicMock()
        mock_directory_client.exists.return_value = True
        mock_file_client = MagicMock()
        mock_adls.file_system_client.get_directory_client = MagicMock(
            return_value=mock_directory_client
        )
        mock_adls.file_system_client.get_file_client = MagicMock(
            return_value=mock_file_client
        )
        mock_adls.timeout = 300

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            await store._write_schema_json(to_tmp=True)

        # Verify schema was written to _tmp path
        assert store._schema_tmp_path is not None
        assert "_tmp" in store._schema_tmp_path
        assert "_schema.json" in store._schema_tmp_path
        assert mock_file_client.upload_data.called

    @pytest.mark.asyncio
    async def test_write_schema_json_raises_error_without_landingzone(self):
        """Test that writing to _tmp raises error if base_path lacks LandingZone."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")
        # Manually set a base_path without LandingZone
        store.base_path = "/MyMirror/Files/SomeOtherPath/users"

        from hygge.utility.exceptions import StoreError

        with pytest.raises(StoreError) as exc_info:
            await store._write_schema_json(to_tmp=True)

        assert "LandingZone" in str(exc_info.value)

    def test_map_polars_dtype_to_fabric(self):
        """Test Polars dtype to Fabric type mapping."""
        from hygge.stores.openmirroring.store import OpenMirroringStore

        # Test string types
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Utf8) == "string"

        # Test integer types
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Int8) == "long"
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Int64) == "long"
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.UInt32) == "long"

        # Test float types
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Float32) == "double"
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Float64) == "double"

        # Test datetime types
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Datetime) == "datetime"
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Date) == "datetime"
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Time) == "datetime"

        # Test unknown type defaults to string
        assert OpenMirroringStore._map_polars_dtype_to_fabric(pl.Boolean) == "string"


class TestOpenMirroringStoreFullDropAtomicOperations:
    """Test full_drop atomic operations including schema file movement."""

    @pytest.mark.asyncio
    async def test_finish_moves_schema_file_from_tmp_in_full_drop(self):
        """Test that finish() moves schema file from _tmp to production."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.configure_for_run("full_drop")

        # Mock ADLS operations
        mock_adls = AsyncMock()
        mock_adls.file_system_client = MagicMock()
        mock_adls.move_file = AsyncMock()
        mock_adls.delete_directory = AsyncMock()
        mock_adls.file_system_client.get_directory_client = MagicMock()
        mock_adls.file_system_client.get_file_client = MagicMock()

        # Simulate schema file written to _tmp
        schema_tmp_path = (
            "/MyMirror/Files/_tmp/__hygge.schema/__hygge_journal/_schema.json"
        )
        store._schema_tmp_path = schema_tmp_path
        store.saved_paths = [
            "/MyMirror/Files/_tmp/__hygge.schema/__hygge_journal/data.parquet"
        ]
        store.data_buffer = None  # No buffered data

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            with patch.object(store, "_delete_table_folder", new_callable=AsyncMock):
                # Mock parent finish to avoid actual file operations
                with patch.object(
                    store.__class__.__bases__[0], "finish", new_callable=AsyncMock
                ):
                    await store.finish()

        # Verify schema file was moved from _tmp to production
        # move_file is called with (source_path, dest_path)
        move_calls = mock_adls.move_file.call_args_list
        schema_move_found = False
        for call in move_calls:
            args = call[0]  # Positional arguments
            if len(args) >= 2:
                source, dest = args[0], args[1]
                if "_schema.json" in str(source) and "_tmp" in str(source):
                    assert "LandingZone" in str(
                        dest
                    ), "Destination should be in LandingZone"
                    schema_move_found = True
                    break

        assert schema_move_found, "Schema file should be moved from _tmp to production"

    @pytest.mark.asyncio
    async def test_finish_resets_schema_tmp_path(self):
        """Test that finish() resets _schema_tmp_path after moving files."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")
        store.configure_for_run("full_drop")

        # Mock ADLS operations
        mock_adls = AsyncMock()
        mock_adls.file_system_client = MagicMock()
        mock_adls.move_file = AsyncMock()
        mock_adls.delete_directory = AsyncMock()
        mock_adls.file_system_client.get_directory_client = MagicMock()
        mock_adls.file_system_client.get_file_client = MagicMock()

        # Set up tmp paths
        store._schema_tmp_path = "/tmp/schema.json"
        store.saved_paths = []

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            with patch.object(store, "_delete_table_folder", new_callable=AsyncMock):
                # Mock parent finish to avoid actual file operations
                with patch.object(
                    store.__class__.__bases__[0], "finish", new_callable=AsyncMock
                ):
                    await store.finish()

        # Verify tmp path was reset
        assert store._schema_tmp_path is None


class TestOpenMirroringStorePathBuilding:
    """Test Open Mirroring store path building."""

    def test_path_building_without_schema(self):
        """Test path building without schema."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            schema_name=None,
        )

        # Path should include database GUID prefix and LandingZone
        assert config.path.startswith("/MyMirror/")
        assert "LandingZone" in config.path
        assert "{entity}" in config.path
        # Should not have schema folder structure
        assert ".schema" not in config.path
        # Expected: /MyMirror/Files/LandingZone/{entity}/
        assert config.path == "/MyMirror/Files/LandingZone/{entity}/"

    def test_path_building_with_schema(self):
        """Test path building with schema."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            schema_name="dbo",
            key_columns=["id"],
            row_marker=0,
        )

        # Path should include database GUID prefix, LandingZone, and schema structure
        assert config.path.startswith("/MyMirror/")
        assert "LandingZone" in config.path
        assert "dbo.schema" in config.path
        assert "{entity}" in config.path
        # Expected: /MyMirror/Files/LandingZone/dbo.schema/{entity}/
        assert config.path == "/MyMirror/Files/LandingZone/dbo.schema/{entity}/"

    def test_path_building_with_custom_path(self):
        """Test that custom path includes database GUID prefix."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            path="custom/path/{entity}/",
        )

        # Custom path should have database GUID prepended
        assert config.path.startswith("/MyMirror/")
        assert config.path == "/MyMirror/custom/path/{entity}/"


class TestOpenMirroringStorePartnerEvents:
    """Test partner events JSON writing."""

    @pytest.mark.asyncio
    async def test_write_partner_events_json_basic(self):
        """Test writing basic partner events JSON."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            partner_name="MyOrg",
            source_type="SQL",
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        # Mock ADLS operations
        mock_adls = AsyncMock()
        mock_adls.read_json = AsyncMock(return_value=None)  # File doesn't exist
        mock_adls.write_json = AsyncMock()

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            await store._write_partner_events_json()

        # Should have written partner events
        assert store._partner_events_written is True
        # Should have called write_json
        mock_adls.write_json.assert_called_once()

        # Check path is correct (database level)
        call_args = mock_adls.write_json.call_args
        path = call_args[0][0]
        assert path == "Files/LandingZone/_partnerEvents.json"

        # Check content
        content = call_args[0][1]
        assert content["partnerName"] == "MyOrg"
        assert content["sourceInfo"]["sourceType"] == "SQL"

    @pytest.mark.asyncio
    async def test_write_partner_events_json_with_all_fields(self):
        """Test partner events JSON with all optional fields."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            partner_name="MyOrg",
            source_type="SQL",
            source_version="2019",
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        # Mock ADLS operations
        mock_adls = AsyncMock()
        mock_adls.read_json = AsyncMock(return_value=None)
        mock_adls.write_json = AsyncMock()

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            await store._write_partner_events_json()

        # Check content includes version
        call_args = mock_adls.write_json.call_args
        content = call_args[0][1]
        assert content["sourceInfo"]["sourceVersion"] == "2019"

    @pytest.mark.asyncio
    async def test_write_partner_events_json_skips_if_existing_consistent(self):
        """Partner events write skipped if file exists w/ same partnerName."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            partner_name="MyOrg",
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        # Mock existing file with same partnerName
        mock_adls = AsyncMock()
        mock_adls.read_json = AsyncMock(
            return_value={"partnerName": "MyOrg"}  # Same partner
        )
        mock_adls.write_json = AsyncMock()

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            await store._write_partner_events_json()

        # Should be marked as written but not actually written
        assert store._partner_events_written is True
        mock_adls.write_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_write_partner_events_json_warns_if_different_partnername(self):
        """Partner events warns if existing file has different partnerName."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            partner_name="MyOrg",
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        # Mock existing file with different partnerName
        mock_adls = AsyncMock()
        mock_adls.read_json = AsyncMock(return_value={"partnerName": "DifferentOrg"})
        mock_adls.write_json = AsyncMock()

        with patch.object(store, "_get_adls_ops", return_value=mock_adls):
            await store._write_partner_events_json()

        # Should skip write and mark as written (to avoid overwriting)
        assert store._partner_events_written is True
        mock_adls.write_json.assert_not_called()


class TestOpenMirroringStoreLastLoadedAt:
    """Test __LastLoadedAt__ column handling."""

    def test_add_last_loaded_at_missing(self):
        """Test adding __LastLoadedAt__ column when missing."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        result = store._add_last_loaded_at(df)

        assert "__LastLoadedAt__" in result.columns
        # All rows should have the same timestamp (added together)
        timestamps = result["__LastLoadedAt__"].unique()
        assert len(timestamps) == 1  # All same timestamp

    def test_add_last_loaded_at_existing(self):
        """Test that __LastLoadedAt__ is not added if already present."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        from datetime import datetime

        existing_timestamp = datetime(2024, 1, 1, 12, 0, 0)
        df = pl.DataFrame(
            {
                "id": [1, 2],
                "name": ["a", "b"],
                "__LastLoadedAt__": [existing_timestamp, existing_timestamp],
            }
        )

        result = store._add_last_loaded_at(df)

        # Should return unchanged
        assert result.equals(df)
