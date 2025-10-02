"""
Integration tests for configuration system.

These tests verify that configurations work end-to-end:
- YAML configuration parsing
- Simple vs advanced configuration handling
- Smart defaults application
- Configuration to flow execution pipeline

Following hygge's testing philosophy:
- Focus on user experience and data integrity
- Test the complete flow from config to execution
- Verify smart defaults "just work"
"""
import pytest
import yaml
import polars as pl
from pathlib import Path
from unittest.mock import Mock, patch

from hygge.core.configs.settings import HyggeSettings
from hygge.core.configs.flow_config import FlowConfig
from hygge.core.configs.home_config import HomeConfig
from hygge.core.configs.store_config import StoreConfig


class TestConfigurationIntegration:
    """
    Test suite for configuration integration focusing on:
    - YAML configuration parsing
    - Configuration validation pipeline
    - Smart defaults application
    - Configuration error handling
    """

    def test_simple_yaml_config_parsing(self, simple_flow_config):
        """Test parsing simple YAML configuration."""
        # Convert dict to YAML and back to test YAML parsing
        yaml_content = yaml.dump(simple_flow_config)
        parsed_config = yaml.safe_load(yaml_content)

        # Should validate successfully
        assert parsed_config["flows"]["test_flow"]["home"] == "data/source/test.parquet"
        assert parsed_config["flows"]["test_flow"]["store"] == "data/destination/output"

    def test_advanced_yaml_config_parsing(self, advanced_flow_config):
        """Test parsing advanced YAML configuration."""
        yaml_content = yaml.dump(advanced_flow_config)
        parsed_config = yaml.safe_load(yaml_content)

        # Should parse nested structure correctly
        flow = parsed_config["flows"]["advanced_flow"]
        assert flow["home"]["type"] == "parquet"
        assert flow["store"]["type"] == "parquet"
        assert flow["home"]["options"]["batch_size"] == 5000
        assert flow["store"]["options"]["compression"] == "snappy"

    def test_multiple_flows_yaml_parsing(self, multiple_flows_config):
        """Test parsing configuration with multiple flows."""
        yaml_content = yaml.dump(multiple_flows_config)
        parsed_config = yaml.safe_load(yaml_content)

        # Should parse all flows
        flows = parsed_config["flows"]
        assert len(flows) == 3
        assert "users_flow" in flows
        assert "orders_flow" in flows
        assert "products_flow" in flows

        # Mixed config styles should work
        assert isinstance(flows["users_flow"]["home"], str)
        assert isinstance(flows["products_flow"]["home"], dict)

    def test_simple_flow_config_validation(self, simple_flow_config, hygge_settings_defaults):
        """Test FlowConfig validation with simple configuration."""
        flow_data = simple_flow_config["flows"]["test_flow"]

        config = FlowConfig(**flow_data)

        # Should parse successfully
        assert config.home_config.path == "data/source/test.parquet"
        assert config.store_config.path == "data/destination/output"

        # Should apply smart defaults
        assert config.home_config.type == hygge_settings_defaults.home_type
        assert config.store_config.type == hygge_settings_defaults.store_type

    def test_advanced_flow_config_validation(self, advanced_flow_config):
        """Test FlowConfig validation with advanced configuration."""
        flow_data = advanced_flow_config["flows"]["advanced_flow"]

        config = FlowConfig(**flow_data)

        # Should parse advanced configuration correctly
        assert config.home_config.type == "parquet"
        assert config.home_config.path == "data/source/users.parquet"
        assert config.home_config.options["batch_size"] == 5000

        assert config.store_config.type == "parquet"
        assert config.store_config.path == "data/lake/users"
        assert config.store_config.options["compression"] == "snappy"
        assert config.store_config.options["batch_size"] == 10000

    def test_mixed_configuration_styles(self, multiple_flows_config):
        """Test configuration with mixed simple and advanced styles."""
        flows = multiple_flows_config["flows"]

        # Test simple configuration
        users_config = FlowConfig(**flows["users_flow"])
        assert users_config.home_config.path == "data/source/users.parquet"
        assert users_config.home_config.type == "parquet"  # Default applied

        # Test advanced configuration
        products_config = FlowConfig(**flows["products_flow"])
        assert products_config.home_config.path == "data/source/products.parquet"
        assert products_config.home_config.options["batch_size"] == 1000

    def test_smart_defaults_application(self, hygge_settings_custom):
        """Test that smart defaults are applied correctly."""
        # Test with custom settings
        simple_config = {
            "home": "data/test.parquet",
            "store": "data/output"
        }

        # Patch settings for testing
        with patch('hygge.core.configs.flow_config.settings', hygge_settings_custom):
            config = FlowConfig(**simple_config)

            # Extension detection should override global settings for home type
            assert config.home_config.type == 'parquet'  # .parquet extension detected
            assert config.store_config.type == hygge_settings_custom.store_type

            # Should apply custom settings
            assert config.home_config.options["batch_size"] == hygge_settings_custom.home_batch_size
            assert config.store_config.options["batch_size"] == hygge_settings_custom.store_batch_size

    def test_extension_detection_vs_global_settings(self):
        """Test that file extensions override global settings."""
        # Test with custom settings that prefer SQL
        custom_settings = HyggeSettings(
            home_type='parquet',  # Use parquet as default for realistic testing
            store_type='parquet',
            home_batch_size=9999
        )

        with patch('hygge.core.configs.flow_config.settings', custom_settings):
            # .parquet extension should be detected regardless of global settings
            parquet_config = FlowConfig(home="data/users.parquet", store="output/")
            assert parquet_config.home_config.type == 'parquet'
            assert parquet_config.home_config.options["batch_size"] == 9999  # Uses custom settings

            # No extension should use global settings (parquet in this case)
            no_ext_config = FlowConfig(home="data/users", store="output/")
            assert no_ext_config.home_config.type == 'parquet'  # Uses global default
            assert no_ext_config.home_config.options["batch_size"] == 9999  # Uses custom settings

        # Test SQL preference (requires explicit SQL config with connection)
        sql_settings = HyggeSettings(home_type='sql', store_type='parquet', home_batch_size=5555)
        with patch('hygge.core.configs.flow_config.settings', sql_settings):
            # Explicit SQL config should work with connection
            sql_config = FlowConfig(
                home={"type": "sql", "connection": "postgresql://test", "table": "users"},
                store="output/"
            )
            assert sql_config.home_config.type == 'sql'
            assert sql_config.home_config.options["batch_size"] == 5555

            # .parquet extension still overrides global SQL preference
            override_config = FlowConfig(home="data/users.parquet", store="output/")
            assert override_config.home_config.type == 'parquet'  # Extension wins
            assert override_config.home_config.options["batch_size"] == 5555  # Custom settings used

    def test_configuration_error_handling(self, invalid_config):
        """Test configuration error handling and validation."""
        # Invalid configuration should raise validation error
        flow_data = invalid_config["flows"]["bad_flow"]

        with pytest.raises(Exception):  # Could be ValidationError or ValueError
            FlowConfig(**flow_data)

    def test_configuration_with_missing_required_fields(self):
        """Test configuration validation with missing fields."""
        # Missing home field
        with pytest.raises(Exception):
            FlowConfig(store="data/output")

        # Missing store field
        with pytest.raises(Exception):
            FlowConfig(home="data/test.parquet")

    def test_configuration_with_empty_values(self):
        """Test configuration handling with empty values."""
        # Empty string paths should be handled
        config = FlowConfig(
            home="",
            store=""
        )

        assert config.home_config.path == ""
        assert config.store_config.path == ""

    def test_yaml_with_evironment_variables(self):
        """Test YAML configuration with environment variable substitution."""
        # This tests string substitution patterns
        config_with_env = {
            "flows": {
                "env_flow": {
                    "home": "${HOME}/data/users.parquet",
                    "store": "${OUTPUT_DIR}/lake/users"
                }
            }
        }

        # Should parse without error (actual env substitution would be in coordinator)
        yaml_content = yaml.dump(config_with_env)
        parsed_config = yaml.safe_load(yaml_content)

        assert parsed_config["flows"]["env_flow"]["home"] == "${HOME}/data/users.parquet"

    def test_configuration_performance(self, simple_flow_config):
        """Test configuration parsing performance with multiple flows."""
        # Create configuration with many flows
        large_config = {
            "flows": {
                f"flow_{i}": {
                    "home": f"data/source/test_{i}.parquet",
                    "store": f"data/output/test_{i}"
                }
                for i in range(100)
            }
        }

        # Should parse quickly
        for flow_name, flow_data in large_config["flows"].items():
            config = FlowConfig(**flow_data)
            assert config.home_config.path.endswith(".parquet")

    def test_configuration_with_unicode(self):
        """Test configuration with unicode characters."""
        uniode_config = {
            "flows": {
                "unicode_flow": {
                    "home": "data/用户/users.parquet",
                    "store": "data/lake/用户"
                }
            }
        }

        flow_data = uniode_config["flows"]["unicode_flow"]
        config = FlowConfig(**flow_data)

        assert config.home_config.path == "data/用户/users.parquet"
        assert config.store_config.path == "data/lake/用户"


class TestConfigurationCornerCases:
    """Test edge cases and corner scenarios."""

    def test_configuration_with_none_values(self):
        """Test configuration with None values."""
        config_data = {
            "home": {"type": "parquet", "path": "test.parquet", "options": {"key": None}},
            "store": {"type": "parquet", "path": "output", "options": {"key": None}}
        }

        config = FlowConfig(**config_data)

        # None values in options should be preserved
        assert config.home_config.options["key"] is None
        assert config.store_config.options["key"] is None

    def test_configuration_with_boolean_values(self):
        """Test configuration with boolean values."""
        config_data = {
            "home": {"type": "parquet", "path": "test.parquet", "options": {"enabled": True}},
            "store": {"type": "parquet", "path": "output", "options": {"deleted": False}}
        }

        config = FlowConfig(**config_data)

        assert config.home_config.options["enabled"] is True
        assert config.store_config.options["deleted"] is False

    def test_configuration_with_numeric_precision(self):
        """Test configuration with various numeric precisions."""
        config_data = {
            "home": {"type": "parquet", "path": "test.parquet", "options": {"ratio": 0.123456789}},
            "store": {"type": "parquet", "path": "output", "options": {"large_int": 999999999999999}}
        }

        config = FlowConfig(**config_data)

        assert config.home_config.options["ratio"] == 0.123456789
        assert config.store_config.options["large_int"] == 999999999999999

    def test_configuration_with_large_options_dict(self):
        """Test configuration with large options dictionary."""
        large_options = {f"key_{i}": f"value_{i}" for i in range(1000)}

        config_data = {
            "home": {"type": "parquet", "path": "test.parquet", "options": large_options},
            "store": {"type": "parquet", "path": "output", "options": {}}
        }

        config = FlowConfig(**config_data)

        # Should handle large options dictionary (plus smart defaults)
        assert len(config.home_config.options) >= 1000  # Smart defaults may add batch_size, etc.
        assert config.home_config.options["key_0"] == "value_0"
        assert config.home_config.options["key_999"] == "value_999"

    def test_configuration_nested_options(self):
        """Test configuration with deeply nested options."""
        nested_options = {
            "level1": {
                "level2": {
                    "level3": {
                        "level4": "deep_value"
                    }
                }
            }
        }

        config_data = {
            "home": {"type": "parquet", "path": "test.parquet", "options": nested_options},
            "store": {"type": "parquet", "path": "output", "options": {}}
        }

        config = FlowConfig(**config_data)

        assert config.home_config.options["level1"]["level2"]["level3"]["level4"] == "deep_value"
