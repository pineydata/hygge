"""
Unit tests for FlowFactory - connection resolution and flow creation.
"""
from unittest.mock import MagicMock

import pytest

from hygge.core.flow.factory import FlowFactory
from hygge.stores.openmirroring import OpenMirroringStoreConfig
from hygge.utility.exceptions import ConfigError


class TestFlowFactoryDeletionSourceResolution:
    """Test _resolve_deletion_source() method."""

    def test_resolve_deletion_source_with_connection_name(self):
        """Test resolving deletion_source connection name to dict."""
        connections = {
            "fabric_mirror": {
                "type": "mssql",
                "server": "fabric-mirror.database.windows.net",
                "database": "MirroredDB",
            }
        }

        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            deletion_source="fabric_mirror",
            deletion_schema="dbo",
            deletion_table="Users",
        )

        logger = MagicMock()
        resolved = FlowFactory._resolve_deletion_source(
            config, connections, "test_flow", "users", logger
        )

        assert isinstance(resolved.deletion_source, dict)
        assert (
            resolved.deletion_source["server"] == "fabric-mirror.database.windows.net"
        )
        assert resolved.deletion_source["database"] == "MirroredDB"
        assert resolved.deletion_source["schema"] == "dbo"
        assert resolved.deletion_source["table"] == "Users"

    def test_resolve_deletion_source_with_inline_dict(self):
        """Test that inline dict is returned unchanged."""
        connections = {}

        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            deletion_source={
                "server": "test.database.windows.net",
                "database": "TestDB",
            },
        )

        logger = MagicMock()
        resolved = FlowFactory._resolve_deletion_source(
            config, connections, "test_flow", "users", logger
        )

        # Should return same config (unchanged)
        assert resolved is config
        assert isinstance(resolved.deletion_source, dict)
        assert resolved.deletion_source["server"] == "test.database.windows.net"

    def test_resolve_deletion_source_with_invalid_connection_name(self):
        """Test that invalid connection name raises ConfigError."""
        connections = {
            "other_connection": {
                "type": "mssql",
                "server": "test",
                "database": "testdb",
            }
        }

        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            deletion_source="nonexistent_connection",
        )

        logger = MagicMock()
        with pytest.raises(ConfigError, match="not found in hygge.yml connections"):
            FlowFactory._resolve_deletion_source(
                config, connections, "test_flow", "users", logger
            )

    def test_resolve_deletion_source_with_wrong_connection_type(self):
        """Test that non-mssql connection type raises ConfigError."""
        connections = {
            "postgres_conn": {
                "type": "postgres",
                "server": "test",
                "database": "testdb",
            }
        }

        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            deletion_source="postgres_conn",
        )

        logger = MagicMock()
        with pytest.raises(ConfigError, match="must be type 'mssql'"):
            FlowFactory._resolve_deletion_source(
                config, connections, "test_flow", "users", logger
            )

    def test_resolve_deletion_source_uses_entity_name_as_table_default(
        self,
    ):
        """Test that entity_name is used as table default when deletion_table
        not specified."""
        connections = {
            "fabric_mirror": {
                "type": "mssql",
                "server": "test",
                "database": "testdb",
            }
        }

        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            deletion_source="fabric_mirror",
            # deletion_table not specified - should use entity_name
        )

        logger = MagicMock()
        resolved = FlowFactory._resolve_deletion_source(
            config, connections, "test_flow", "users", logger
        )

        # Table should not be in resolved dict (will default to entity_name at runtime)
        assert "table" not in resolved.deletion_source

    def test_resolve_deletion_source_skips_when_not_configured(self):
        """Test that resolution skips when deletion_source is None."""
        connections = {}

        config = OpenMirroringStoreConfig(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            filesystem="MyLake",
            mirror_name="MyMirror",
            key_columns=["id"],
            row_marker=0,
            # deletion_source not configured
        )

        logger = MagicMock()
        resolved = FlowFactory._resolve_deletion_source(
            config, connections, "test_flow", "users", logger
        )

        # Should return unchanged
        assert resolved is config
