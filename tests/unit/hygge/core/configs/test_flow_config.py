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
from hygge.core.home import Home
from hygge.core.store import Store

# Import concrete implementations to register them


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

        # Should parse home as Home instance with defaults
        assert isinstance(config.home_instance, Home)
        assert config.home_instance.name == "flow_home"

        # Should parse store as Store instance with defaults
        assert isinstance(config.store_instance, Store)
        assert config.store_instance.name == ""

    def test_simple_store_path_config(self):
        """Test simple store: path configuration works with defaults."""
        config = FlowConfig(home="data/users.parquet", store="data/output")

        # Should parse store as Store instance with defaults
        assert isinstance(config.store_instance, Store)
        assert config.store_instance.name == ""

    def test_advanced_home_config(self):
        """Test advanced home configuration parsing."""
        home_config = {
            "type": "parquet",
            "path": "data/users.parquet",
            "batch_size": 5000,
        }

        config = FlowConfig(home=home_config, store="data/output")

        # Should parse as Home instance
        assert isinstance(config.home_instance, Home)
        assert config.home_instance.name == "flow_home"

    def test_advanced_store_config(self):
        """Test advanced store configuration parsing."""
        store_config = {
            "type": "parquet",
            "path": "data/lake/users",
            "compression": "zstd",
        }

        config = FlowConfig(home="data/users.parquet", store=store_config)

        # Should parse as Store instance
        assert isinstance(config.store_instance, Store)
        assert config.store_instance.name == ""

    def test_home_config_with_defaults_only(self):
        """Test home config using defaults when type not specified."""
        home_config = {
            "path": "data/users.parquet",
        }

        config = FlowConfig(home=home_config, store="data/output")

        # Should use default type and create Home instance
        assert isinstance(config.home_instance, Home)

    def test_store_config_with_defaults_only(self):
        """Test store config using defaults when type not specified."""
        store_config = {
            "path": "data/lake/users",
        }

        config = FlowConfig(home="data/users.parquet", store=store_config)

        # Should use default type and create Store instance
        assert isinstance(config.store_instance, Store)

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
        with pytest.raises(ValidationError) as exc_info:
            FlowConfig(home="", store="")

        # Should have validation errors for both home and store
        errors = exc_info.value.errors()
        assert len(errors) == 2
        assert any(
            "Path is required for parquet homes" in str(error["ctx"]["error"])
            for error in errors
        )
        assert any(
            "Path is required for parquet stores" in str(error["ctx"]["error"])
            for error in errors
        )

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

        assert isinstance(config.home_instance, Home)
        assert isinstance(config.store_instance, Store)

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

    def test_home_instance_property(self):
        """Test home_instance property always returns Home."""
        config = FlowConfig(home="data/users.parquet", store="data/output")

        # Should always return Home instance
        assert isinstance(config.home_instance, Home)

        # Test advanced config
        advanced_config = FlowConfig(
            home={"type": "parquet", "path": "data/users.parquet"},
            store="data/output",
        )

        assert isinstance(advanced_config.home_instance, Home)

    def test_store_instance_property(self):
        """Test store instance property always returns Store."""
        config = FlowConfig(home="data/users.parquet", store="data/output")

        # Should always return Store instance
        assert isinstance(config.store_instance, Store)

        # Test advanced config
        advanced_config = FlowConfig(
            home="data/users.parquet",
            store={"type": "parquet", "path": "data/output"},
        )

        assert isinstance(advanced_config.store_instance, Store)
