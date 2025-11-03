"""
Unit tests for Open Mirroring Store implementation.

Tests configuration, path management, row marker handling, metadata files,
and Open Mirroring specific features without requiring an actual OneLake account.
Integration tests should be in tests/integration/.
"""
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
        assert config.full_drop is False  # Default
        assert config.folder_deletion_wait_seconds == 2.0  # Default

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
            full_drop=True,
            folder_deletion_wait_seconds=5.0,
            partner_name="MyOrg",
            source_type="SQL",
            source_version="2019",
            starting_sequence=100,
        )

        assert config.row_marker == 4  # Upsert
        assert config.file_detection == "sequential"
        assert config.full_drop is True
        assert config.folder_deletion_wait_seconds == 5.0
        assert config.partner_name == "MyOrg"
        assert config.source_type == "SQL"
        assert config.starting_sequence == 100

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

    def test_config_requires_key_columns(self):
        """Test that key_columns is required."""
        with pytest.raises(ValidationError) as exc_info:
            OpenMirroringStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                mirror_name="MyMirror",
                row_marker=0,
                # key_columns missing
            )
        assert "key_columns" in str(exc_info.value).lower()

    def test_config_requires_key_columns_non_empty(self):
        """Test that key_columns must have at least one column."""
        with pytest.raises(ValidationError):
            OpenMirroringStoreConfig(
                account_url="https://onelake.dfs.fabric.microsoft.com",
                filesystem="MyLake",
                mirror_name="MyMirror",
                key_columns=[],  # Empty list
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
        assert store.full_drop is False
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

    def test_store_with_full_drop(self):
        """Test Open Mirroring store with full_drop enabled."""
        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            full_drop=True,
        )

        store = OpenMirroringStore("test_store", config, entity_name="users")

        assert store.full_drop is True


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

        # Should be timestamp_000006.parquet format (increments before returning)
        assert filename.endswith(".parquet")
        # Should have timestamp prefix and sequence (which was incremented to 6)
        parts = filename.replace(".parquet", "").split("_")
        assert len(parts) == 3  # date, time, sequence
        assert parts[2] == "000006"
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
        # Should have called upload_data
        mock_file_client.upload_data.assert_called_once()

        # Check metadata content
        call_args = mock_file_client.upload_data.call_args
        json_data = call_args[0][0].decode("utf-8")
        import json

        metadata = json.loads(json_data)
        assert metadata["keyColumns"] == ["id"]
        # file_detection default "timestamp", so include fileDetectionStrategy
        assert metadata["fileDetectionStrategy"] == "LastUpdateTimeFileDetection"
        assert "isUpsertDefaultRowMarker" not in metadata  # Not Upsert

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
        call_args = mock_file_client.upload_data.call_args
        json_data = call_args[0][0].decode("utf-8")
        import json

        metadata = json.loads(json_data)
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
        call_args = mock_file_client.upload_data.call_args
        json_data = call_args[0][0].decode("utf-8")
        import json

        metadata = json.loads(json_data)
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
