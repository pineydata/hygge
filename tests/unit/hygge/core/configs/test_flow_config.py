"""
Tests for FlowConfig configuration parsing.

Following hygge's "Convention over Configuration" philosophy:
- Test simple home: path syntax works with smart defaults
- Test advanced config features work correctly
- Verify smart defaults are applied appropriately
- Test validation catches configuration errors
"""
import pytest
from pydantic import ValidationError

from hygge.core.flow import FlowConfig
from hygge.core.home import HomeConfig
from hygge.core.store import StoreConfig


class TestFlowConfig:
    """
    Test suite for FlowConfig focusing on:
    - Simple configuration parsing (Rails spirit)
    - Advanced configuration parsing
    - Smart defaults application
    - Configuration validation
    """

    def test_simple_home_path_config(self):
        """Test simple home: path configuration works with defaults."""
        config = FlowConfig(home="data/users.parquet", store="data/lake/users")

        # Should parse home as HomeConfig with defaults
        assert isinstance(config.home_config, HomeConfig)
        assert config.home_config.path == "data/users.parquet"
        assert config.home_config.type == "parquet"  # Default type

        # Should parse store as StoreConfig with defaults
        assert isinstance(config.store_config, StoreConfig)

    def test_simple_store_path_config(self):
        """Test simple store: path configuration works with defaults."""
        config = FlowConfig(home="data/users.parquet", store="data/output")

        # Should parse store as StoreConfig with defaults
        assert isinstance(config.store_config, StoreConfig)
        assert config.store_config.path == "data/output"
        assert config.store_config.type == "parquet"  # Default type

    def test_advanced_home_config(self):
        """Test advanced home configuration parsing."""
        home_config = {
            "type": "sql",
            "table": "users",
            "connection": "sqlite:///test.db",
            "options": {"batch_size": 5000, "custom_option": "value"},
        }

        config = FlowConfig(home=home_config, store="data/output")

        # Should parse as HomeConfig
        assert isinstance(config.home_config, HomeConfig)
        assert config.home_config.type == "sql"
        assert config.home_config.table == "users"
        assert config.home_config.connection == "sqlite:///test.db"

        # Options should be merged with defaults
        assert config.home_config.options["batch_size"] == 5000
        assert config.home_config.options["custom_option"] == "value"

    def test_advanced_store_config(self):
        """Test advanced store configuration parsing."""
        store_config = {
            "type": "parquet",
            "path": "data/lake/users",
            "options": {"compression": "zstd", "custom_option": "value"},
        }

        config = FlowConfig(home="data/users.parquet", store=store_config)

        # Should parse as StoreConfig
        assert isinstance(config.store_config, StoreConfig)
        assert config.store_config.type == "parquet"
        assert config.store_config.path == "data/lake/users"

        # Options should be merged with defaults
        assert config.store_config.options["compression"] == "zstd"
        assert config.store_config.options["custom_option"] == "value"

    def test_home_config_with_defaults_only(self):
        """Test home config using defaults when type not specified."""
        home_config = {
            "path": "data/users.parquet",
            "options": {"custom_option": "value"},
        }

        config = FlowConfig(home=home_config, store="data/output")

        # Should use default type
        assert config.home_config.type == "parquet"
        assert config.home_config.path == "data/users.parquet"
        assert config.home_config.options["custom_option"] == "value"

    def test_store_config_with_defaults_only(self):
        """Test store config using defaults when type not specified."""
        store_config = {
            "path": "data/lake/users",
            "options": {"custom_option": "value"},
        }

        config = FlowConfig(home="data/users.parquet", store=store_config)

        # Should use default type
        assert config.store_config.type == "parquet"
        assert config.store_config.path == "data/lake/users"
        assert config.store_config.options["custom_option"] == "value"

    def test_config_with_flow_options(self):
        """Test flow configuration with additional options."""
        config = FlowConfig(
            home="data/users.parquet",
            store="data/output",
            options={"custom_flow_option": "value", "timeout": 600},
        )

        assert config.options["custom_flow_option"] == "value"
        assert config.options["timeout"] == 600

    def test_empty_flow_options_default(self):
        """Test flow configuration with no additional options."""
        config = FlowConfig(home="data/users.parquet", store="data/output")

        assert config.options == {}

    # Error Scenarios

    def test_config_without_home(self):
        """Test that configuration without home raises validation error."""
        with pytest.raises(ValidationError) as exc_info:
            FlowConfig(store="data/output")

        error = exc_info.value.errors()[0]
        assert error["type"] == "missing"
        assert "home" in error["loc"]

    def test_config_without_store(self):
        """Test that configuration without store raises validation error."""
        with pytest.raises(ValidationError) as exc_info:
            FlowConfig(home="data/users.parquet")

        error = exc_info.value.errors()[0]
        assert error["type"] == "missing"
        assert "store" in error["loc"]

    def test_invalid_home_config_structure(self):
        """Test validation catches invalid home configuration."""
        with pytest.raises(ValidationError) as exc_info:
            FlowConfig(
                home={"invalid_field": "value"},  # Missing required fields
                store="data/output",
            )

        # Should have validation errors
        errors = exc_info.value.errors()
        assert len(errors) > 0

    def test_invalid_store_config_structure(self):
        """Test validation catches invalid store configuration."""
        with pytest.raises(ValidationError) as exc_info:
            FlowConfig(
                home="data/users.parquet",
                store={"invalid_field": "value"},  # Missing required fields
            )

        # Should have validation errors
        errors = exc_info.value.errors()
        assert len(errors) > 0

    def test_home_none_value(self):
        """Test that None home value raises validation error."""
        with pytest.raises(ValidationError):
            FlowConfig(home=None, store="data/output")

    def test_store_none_value(self):
        """Test that None store value raises validation error."""
        with pytest.raises(ValidationError):
            FlowConfig(home="data/users.parquet", store=None)

    def test_empty_string_paths(self):
        """Test handling of empty string paths."""
        config = FlowConfig(home="", store="")

        # Should parse but with empty paths
        assert config.home_config.path == ""
        assert config.store_config.path == ""

    # Edge Cases

    def test_large_number_config(self):
        """Test configuration with large numbers."""
        config = FlowConfig(
            home="data/users.parquet",
            store="data/output",
            options={"large_value": 999999999, "small_value": 0.0001},
        )

        assert config.options["large_value"] == 999999999
        assert config.options["small_value"] == 0.0001

    def test_unicode_paths(self):
        """Test configuration with unicode paths."""
        unicode_path = "data/用户/users.parquet"

        config = FlowConfig(
            home=unicode_path, store=unicode_path.replace("用户", "lake")
        )

        assert config.home_config.path == unicode_path
        assert "lake" in config.store_config.path

    def test_empty_options_dict(self):
        """Test configuration with explicit empty options."""
        config = FlowConfig(home="data/users.parquet", store="data/output", options={})

        assert config.options == {}

    def test_config_with_complex_nested_options(self):
        """Test configuration with complex nested options."""
        complex_options = {
            "batch_settings": {"size": 1000, "retry_count": 3},
            "paths": ["data/temp", "data/final"],
        }

        config = FlowConfig(
            home="data/users.parquet", store="data/output", options=complex_options
        )

        assert config.options["batch_settings"]["size"] == 1000
        assert len(config.options["paths"]) == 2

    def test_flow_config_defaults(self):
        """Test FlowConfig provides reasonable defaults."""
        config = FlowConfig(home="data/users.parquet", store="data/output")

        # Should have defaults
        assert config.queue_size == 10  # Default from FlowConfig
        assert config.timeout == 300  # Default from FlowConfig


class TestConfigurationPropertyAccess:
    """Test suite for property access methods."""

    def test_home_config_property(self):
        """Test home_config property always returns HomeConfig."""
        config = FlowConfig(home="data/users.parquet", store="data/output")

        # Should always return HomeConfig instance
        assert isinstance(config.home_config, HomeConfig)

        # Test advanced config
        advanced_config = FlowConfig(
            home={"type": "sql", "table": "users", "connection": "test.db"},
            store="data/output",
        )

        assert isinstance(advanced_config.home_config, HomeConfig)

    def test_store_config_property(self):
        """Test store config property always returns StoreConfig."""
        config = FlowConfig(home="data/users.parquet", store="data/output")

        # Should always return StoreConfig instance
        assert isinstance(config.store_config, StoreConfig)

        # Test advanced config
        advanced_config = FlowConfig(
            home="data/users.parquet",
            store={"type": "parquet", "path": "data/output", "options": {}},
        )

        assert isinstance(advanced_config.store_config, StoreConfig)
