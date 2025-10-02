"""
Tests for HomeConfig configuration parsing.

Following hygge's configuration philosophy:
- Test different home types (parquet, sql)
- Verify required fields for each type
- Test validation catches configuration errors
- Ensure smart defaults work correctly
"""
import pytest
from pydantic import ValidationError

from hygge.core.configs.home_config import HomeConfig


class TestHomeConfig:
    """
    Test suite for HomeConfig focusing on:
    - Type validation (parquet, sql)
    - Required field validation
    - Configuration parsing
    - Error scenarios
    """

    def test_parquet_home_config(self):
        """Test parquet home configuration."""
        config = HomeConfig(
            type="parquet",
            path="data/users.parquet"
        )

        assert config.type == "parquet"
        assert config.path == "data/users.parquet"
        assert config.connection is None
        assert config.options == {}

    def test_parquet_home_with_options(self):
        """Test parquet home configuration with options."""
        config = HomeConfig(
            type="parquet",
            path="data/users.parquet",
            options={
                "batch_size": 1000,
                "custom_option": "value"
            }
        )

        assert config.type == "parquet"
        assert config.path == "data/users.parquet"
        assert config.options["batch_size"] == 1000
        assert config.options["custom_option"] == "value"

    def test_sql_home_config(self):
        """Test SQL home configuration."""
        config = HomeConfig(
            type="sql",
            connection="sqlite:///test.db",
            path="users"  # Optional for SQL
        )

        assert config.type == "sql"
        assert config.connection == "sqlite:///test.db"
        assert config.path == "users"

    def test_sql_home_with_options(self):
        """Test SQL home configuration with options."""
        config = HomeConfig(
            type="sql",
            connection="postgresql://user:pass@localhost/db",
            path="public.users",
            options={
                "batch_size": 5000,
                "schema": "public"
            }
        )

        assert config.type == "sql"
        assert config.connection == "postgresql://user:pass@localhost/db"
        assert config.options["batch_size"] == 5000
        assert config.options["schema"] == "public"

    def test_sql_home_without_path(self):
        """Test SQL home configuration without path (path is optional for SQL)."""
        config = HomeConfig(
            type="sql",
            connection="sqlite:///test.db"
        )

        assert config.type == "sql"
        assert config.connection == "sqlite:///test.db"
        assert config.path is None

    # Error Scenarios

    def test_invalid_home_type(self):
        """Test validation catches invalid home types."""
        with pytest.raises(ValidationError) as exc_info:
            HomeConfig(
                type="invalid_type",
                path="data/users.parquet"
            )

        error = exc_info.value.errors()[0]
        assert error['type'] == 'value_error'
        assert 'Home type must be one of' in str(error['ctx']['error'])

    def test_parquet_home_without_path(self):
        """Test validation requires path for parquet homes"""
        with pytest.raises(ValidationError) as exc_info:
            HomeConfig(
                type="parquet"
                # Missing required path
            )

        error = exc_info.value.errors()[0]
        assert error['type'] == 'value_error'
        assert 'Path is required for parquet homes' in str(error['ctx']['error'])

    def test_parquet_home_with_empty_path(self):
        """Test empty path is allowed for parquet homes (validation happens during execution)."""
        # Empty string should be allowed (actual validation happens during execution)
        config = HomeConfig(
            type="parquet",
            path=""
        )
        assert config.path == ""

    def test_sql_home_without_connection(self):
        """Test validation requires connection for SQL homes."""
        with pytest.raises(ValidationError) as exc_info:
            HomeConfig(
                type="sql"
                # Missing required connection
            )

        error = exc_info.value.errors()[0]
        assert error['type'] == 'value_error'
        assert 'Connection is required for SQL homes' in str(error['ctx']['error'])

    def test_sql_home_with_empty_connection(self):
        """Test empty connection is allowed for SQL homes (validation happens during execution)."""
        # Empty string should be allowed (actual validation happens during execution)
        config = HomeConfig(
            type="sql",
            connection=""
        )
        assert config.connection == ""

    def test_missing_type_field(self):
        """Test validation catches missing type field."""
        with pytest.raises(ValidationError) as exc_info:
            HomeConfig(
                path="data/users.parquet"
                # Missing required type
            )

        error = exc_info.value.errors()[0]
        assert error['type'] == 'missing'
        assert error['loc'] == ('type',)

    # Edge Cases

    def test_case_sensitive_types(self):
        """Test that type validation is case sensitive."""
        with pytest.raises(ValidationError):
            HomeConfig(type="PARQUET", path="data/users.parquet")

        with pytest.raises(ValidationError):
            HomeConfig(type="SQL", connection="sqlite:///test.db")

    def test_none_values(self):
        """Test handling of None values in optional fields."""
        # None path should be allowed for SQL
        config = HomeConfig(
            type="sql",
            connection="sqlite:///test.db",
            path=None
        )

        assert config.path is None

        # None connection should not be allowed for SQL
        with pytest.raises(ValidationError):
            HomeConfig(
                type="sql",
                connection=None,
                path="table_name"
            )

    def test_empty_options_dict(self):
        """Test configuration with explicit empty options."""
        config = HomeConfig(
            type="parquet",
            path="data/users.parquet",
            options={}
        )

        assert config.options == {}

    def test_options_with_nested_values(self):
        """Test configuration with nested options."""
        config = HomeConfig(
            type="sql",
            connection="sqlite:///test.db",
            options={
                "batch_settings": {
                    "size": 1000,
                    "retry_count": 3
                },
                "paths": ["temp", "final"]
            }
        )

        assert config.options["batch_settings"]["size"] == 1000
        assert len(config.options["paths"]) == 2

    def test_long_path_string(self):
        """Test configuration with long path strings."""
        long_path = "/very/long/path/to/data/files/" + "users" * 100 + ".parquet"

        config = HomeConfig(
            type="parquet",
            path=long_path
        )

        assert config.path == long_path

    def test_connection_with_unicode(self):
        """Test connection string with unicode characters."""
        unicode_connection = "postgresql://用户:密码@localhost/数据库"

        config = HomeConfig(
            type="sql",
            connection=unicode_connection
        )

        assert config.connection == unicode_connection

    def test_multiple_validation_errors(self):
        """Test that multiple validation errors are reported."""
        with pytest.raises(ValidationError) as exc_info:
            HomeConfig()
            # Missing both type and required connection/path

        errors = exc_info.value.errors()
        # Should report missing type
        assert any(error['type'] == 'missing' for error in errors)

    def test_pydantic_model_features(self):
        """Test standard Pydantic model features work."""
        config = HomeConfig(
            type="parquet",
            path="data/users.parquet",
            options={"batch_size": 1000}
        )

        # Test serialization
        data = config.model_dump()
        assert data['type'] == "parquet"
        assert data['path'] == "data/users.parquet"
        assert data['options']['batch_size'] == 1000

        # Test JSON schema
        schema = config.model_json_schema()
        assert 'properties' in schema
        assert 'type' in schema['properties']
