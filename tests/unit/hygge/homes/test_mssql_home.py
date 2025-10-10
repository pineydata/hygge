"""
Unit tests for MssqlHome - logic only, no database required.

Tests focus on configuration validation and query building logic
without requiring actual database connections or mocking.
"""
import pytest

from hygge.connections.constants import (
    MSSQL_BATCHING_DEFAULTS,
    MSSQL_CONNECTION_DEFAULTS,
)
from hygge.homes.mssql import MssqlHome, MssqlHomeConfig


class TestMssqlHomeConfig:
    """Test MssqlHomeConfig validation."""

    def test_config_with_named_connection(self):
        """Test config with named connection reference."""
        config = MssqlHomeConfig(type="mssql", connection="test_db", table="dbo.users")
        assert config.connection == "test_db"
        assert config.table == "dbo.users"
        assert config.server is None
        assert config.database is None

    def test_config_with_direct_connection(self):
        """Test config with direct server/database."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        assert config.server == "test.database.windows.net"
        assert config.database == "testdb"
        assert config.connection is None

    def test_config_requires_connection_or_server_database(self):
        """Test that config requires either connection or server/database."""
        with pytest.raises(ValueError, match="Must provide either 'connection'"):
            MssqlHomeConfig(
                type="mssql",
                table="dbo.users",
                # Missing both connection and server/database
            )

    def test_config_rejects_both_connection_and_direct(self):
        """Test that config rejects both connection types."""
        with pytest.raises(ValueError, match="Cannot specify both"):
            MssqlHomeConfig(
                type="mssql",
                connection="test_db",
                server="test.database.windows.net",
                database="testdb",
                table="dbo.users",
            )

    def test_config_requires_table_or_query(self):
        """Test that config requires either table or query."""
        with pytest.raises(ValueError, match="Must provide either 'table' or 'query'"):
            MssqlHomeConfig(
                type="mssql",
                connection="test_db",
                # Missing both table and query
            )

    def test_config_with_custom_query(self):
        """Test config with custom SQL query."""
        config = MssqlHomeConfig(
            type="mssql",
            connection="test_db",
            query="SELECT id, name FROM users WHERE active = 1",
        )
        assert config.query == "SELECT id, name FROM users WHERE active = 1"
        assert config.table is None

    def test_config_defaults(self):
        """Test config default values."""
        config = MssqlHomeConfig(type="mssql", connection="test_db", table="dbo.users")
        assert config.driver == MSSQL_CONNECTION_DEFAULTS.driver
        assert config.encrypt == MSSQL_CONNECTION_DEFAULTS.encrypt
        assert config.trust_cert == MSSQL_CONNECTION_DEFAULTS.trust_cert
        assert config.timeout == MSSQL_CONNECTION_DEFAULTS.timeout
        assert config.batch_size == MSSQL_BATCHING_DEFAULTS.batch_size

    def test_config_custom_options(self):
        """Test config with custom options."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
            driver="ODBC Driver 17 for SQL Server",
            encrypt="No",
            trust_cert="No",
            timeout=60,
            batch_size=50_000,
        )
        assert config.driver == "ODBC Driver 17 for SQL Server"
        assert config.encrypt == "No"
        assert config.trust_cert == "No"
        assert config.timeout == 60
        assert config.batch_size == 50_000

    def test_get_merged_options(self):
        """Test merged options include batch_size."""
        config = MssqlHomeConfig(
            type="mssql",
            connection="test_db",
            table="dbo.users",
            batch_size=25_000,
            options={"custom_option": "value"},
        )
        merged = config.get_merged_options()
        assert merged["batch_size"] == 25_000
        assert merged["custom_option"] == "value"

    def test_get_connection_options(self):
        """Test connection options extraction."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
            driver="Custom Driver",
            encrypt=MSSQL_CONNECTION_DEFAULTS.encrypt,
            trust_cert="No",
            timeout=45,
        )
        conn_opts = config.get_connection_options()
        assert conn_opts["driver"] == "Custom Driver"
        assert conn_opts["encrypt"] == MSSQL_CONNECTION_DEFAULTS.encrypt
        assert conn_opts["trust_cert"] == "No"
        assert conn_opts["timeout"] == 45


class TestMssqlHomeQueryBuilding:
    """Test query building logic without database connection."""

    def test_build_query_from_table(self):
        """Test building query from simple table name."""
        config = MssqlHomeConfig(type="mssql", connection="test_db", table="dbo.users")
        home = MssqlHome("test", config)

        query = home._build_query()
        assert query == "SELECT * FROM dbo.users"

    def test_build_query_from_table_no_schema(self):
        """Test building query from table without schema prefix."""
        config = MssqlHomeConfig(type="mssql", connection="test_db", table="users")
        home = MssqlHome("test", config)

        query = home._build_query()
        assert query == "SELECT * FROM users"

    def test_build_query_with_custom_query(self):
        """Test using custom SQL query."""
        config = MssqlHomeConfig(
            type="mssql",
            connection="test_db",
            query="SELECT id, name FROM users WHERE active = 1",
        )
        home = MssqlHome("test", config)

        query = home._build_query()
        assert query == "SELECT id, name FROM users WHERE active = 1"

    def test_build_query_custom_query_overrides_table(self):
        """Test that custom query takes precedence over table."""
        config = MssqlHomeConfig(
            type="mssql",
            connection="test_db",
            table="dbo.users",
            query="SELECT * FROM dbo.accounts",
        )
        home = MssqlHome("test", config)

        query = home._build_query()
        # Query should override table
        assert query == "SELECT * FROM dbo.accounts"
        assert "users" not in query

    def test_build_query_with_entity_substitution_table(self):
        """Test entity substitution in table name."""
        config = MssqlHomeConfig(
            type="mssql", connection="test_db", table="dbo.{entity}"
        )
        home = MssqlHome("test", config, entity_name="Account")

        query = home._build_query()
        assert query == "SELECT * FROM dbo.Account"

    def test_build_query_with_entity_substitution_query(self):
        """Test entity substitution in custom query."""
        config = MssqlHomeConfig(
            type="mssql",
            connection="test_db",
            query="SELECT * FROM dbo.{entity} WHERE status = 'active'",
        )
        home = MssqlHome("test", config, entity_name="users")

        query = home._build_query()
        assert query == "SELECT * FROM dbo.users WHERE status = 'active'"

    def test_build_query_without_entity_substitution(self):
        """Test that {entity} placeholder remains if no entity provided."""
        config = MssqlHomeConfig(
            type="mssql", connection="test_db", table="dbo.{entity}"
        )
        home = MssqlHome("test", config)  # No entity_name

        query = home._build_query()
        # Should leave placeholder as-is
        assert query == "SELECT * FROM dbo.{entity}"

    def test_build_query_entity_multiple_occurrences(self):
        """Test entity substitution with multiple placeholders."""
        config = MssqlHomeConfig(
            type="mssql",
            connection="test_db",
            query=(
                "SELECT a.*, b.* FROM {entity}_main a "
                "JOIN {entity}_detail b ON a.id = b.id"
            ),
        )
        home = MssqlHome("test", config, entity_name="sales")

        query = home._build_query()
        expected = (
            "SELECT a.*, b.* FROM sales_main a " "JOIN sales_detail b ON a.id = b.id"
        )
        assert query == expected
        assert "{entity}" not in query

    def test_build_query_complex_table_with_entity(self):
        """Test entity substitution in complex table patterns."""
        config = MssqlHomeConfig(
            type="mssql",
            connection="test_db",
            table="salesforce.dbo.SF_{entity}_History",
        )
        home = MssqlHome("test", config, entity_name="Account")

        query = home._build_query()
        assert query == "SELECT * FROM salesforce.dbo.SF_Account_History"


class TestMssqlHomeInitialization:
    """Test MssqlHome initialization and setup."""

    def test_init_with_pool(self):
        """Test initialization with connection pool."""
        config = MssqlHomeConfig(type="mssql", connection="test_db", table="dbo.users")
        # Mock pool object (just for initialization test)
        mock_pool = type("MockPool", (), {"name": "test_pool"})()

        home = MssqlHome("test", config, pool=mock_pool)

        assert home.name == "test"
        assert home.config == config
        assert home.pool == mock_pool
        assert home.entity_name is None
        assert home._connection is None
        assert home._owned_connection is False

    def test_init_without_pool(self):
        """Test initialization without connection pool."""
        config = MssqlHomeConfig(
            type="mssql",
            server="test.database.windows.net",
            database="testdb",
            table="dbo.users",
        )
        home = MssqlHome("test", config)

        assert home.pool is None
        assert home._owned_connection is False

    def test_init_with_entity_name(self):
        """Test initialization with entity name."""
        config = MssqlHomeConfig(
            type="mssql", connection="test_db", table="dbo.{entity}"
        )
        home = MssqlHome("test", config, entity_name="Account")

        assert home.entity_name == "Account"

    def test_init_merges_options(self):
        """Test that initialization merges options correctly."""
        config = MssqlHomeConfig(
            type="mssql",
            connection="test_db",
            table="dbo.users",
            batch_size=25_000,
            options={"custom": "value"},
        )
        home = MssqlHome("test", config)

        assert home.options["batch_size"] == 25_000
        assert home.options["custom"] == "value"
