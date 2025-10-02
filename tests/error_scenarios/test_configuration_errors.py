"""
Error scenario tests for configuration system.

These tests verify that hygge handles configuration errors gracefully:
- Invalid configuration formats
- Missing required fields
- Type mismatches
- Network and I/O errors during config validation
- Edge cases that users might encounter

Following hygge's reliability principles:
- Graceful failure handling
- Clear error messages
- User-friendly error reporting
"""
import pytest
import yaml
from pathlib import Path
from unittest.mock import Mock, patch
import tempfile
import os

from hygge.core.configs.flow_config import FlowConfig
from hygge.core.configs.home_config import HomeConfig
from hygge.core.configs.store_config import StoreConfig
from hygge.core.configs.settings import HyggeSettings


class TestConfigurationErrorScenarios:
    """
    Test suite for configuration error scenarios focusing on:
    - Invalid input formats
    - Missing required fields
    - Type mismatches
    - Edge cases
    """

    def test_malformed_yaml_config(self):
        """Test handling of malformed YAML configuration."""
        malformed_yaml = """
        flows:
          test_flow:
            home: "data/test.parquet"
            # Missing closing quote
            store: "data/output
        """

        with pytest.raises(yaml.YAMLError):
            yaml.safe_load(malformed_yaml)

    def test_invalid_json_in_options(self):
        """Test handling of invalid JSON-like data in options."""
        config_data = {
            "home": "data/test.parquet",
            "store": "data/output",
            "options": {
                "invalid_json": {"key": "value"},  # Valid
                "circular_ref": {}  # Will be filled below
            }
        }

        # Create circular reference
        config_data["options"]["circular_ref"]["self"] = config_data["options"]["circular_ref"]

        # Should handle gracefully (Pydantic might serialize different from original)
        config = FlowConfig(**config_data)
        # This tests Pydantic's handling of complex structures

    def test_missing_required_fields(self):
        """Test validation errors for missing required fields."""
        # Missing home
        with pytest.raises(Exception) as exc_info:
            FlowConfig(store="data/output")

        # Missing store
        with pytest.raises(Exception) as exc_info:
            FlowConfig(home="data/test.parquet")

    def test_invalid_home_type(self):
        """Test validation with invalid home type."""
        invalid_config = {
            "home": {"type": "invalid_type", "path": "data/test.parquet"},
            "store": "data/output"
        }

        with pytest.raises(Exception) as exc_info:
            FlowConfig(**invalid_config)

    def test_invalid_store_type(self):
        """Test validation with invalid store type."""
        invalid_config = {
            "home": "data/test.parquet",
            "store": {"type": "invalid_type", "path": "data/output"}
        }

        with pytest.raises(Exception) as exc_info:
            FlowConfig(**invalid_config)

    def test_missing_home_path_for_parquet(self):
        """Test error when parquet home is missing path."""
        invalid_config = {
            "home": {"type": "parquet"},  # Missing required path
            "store": "data/output"
        }

        with pytest.raises(Exception) as exc_info:
            FlowConfig(**invalid_config)

    def test_missing_home_connection_for_sql(self):
        """Test error when SQL home is missing connection."""
        invalid_config = {
            "home": {"type": "sql"},  # Missing required connection
            "store": "data/output"
        }

        with pytest.raises(Exception) as exc_info:
            FlowConfig(**invalid_config)

    def test_invalid_option_types(self):
        """Test configuration with invalid option value types."""
        # This tests Pydantic's type coercion behavior
        config_data = {
            "home": {"type": "parquet", "path": "test.parquet", "options": {}},
            "store": "data/output"
        }

        # Should handle various types correctly
        config = FlowConfig(**config_data)
        assert isinstance(config.home_config.options, dict)

    def test_large_configuration_file(self):
        """Test handling of very large configuration files."""
        # Create a configuration with many flows
        large_config = {
            "flows": {}
        }

        for i in range(10000):  # Very large number of flows
            large_config["flows"][f"flow_{i}"] = {
                "home": f"data/source/test_{i}.parquet",
                "store": f"data/output/test_{i}"
            }

        # Should parse without memory issues
        yaml_content = yaml.dump(large_config)
        parsed_config = yaml.safe_load(yaml_content)

        assert len(parsed_config["flows"]) == 10000

    def test_deeply_nested_configuration(self):
        """Test configuration with very deep nesting."""
        deeply_nested = {"level1": {}}
        current = deeply_nested["level1"]

        # Create 100 levels of nesting
        for i in range(100):
            current[f"level{i+2}"] = {}
            current = current[f"level{i+2}"]

        current["value"] = "deep_value"

        config_data = {
            "home": {"type": "parquet", "path": "test.parquet", "options": deeply_nested},
            "store": "data/output"
        }

        # Should handle deep nesting without recursion errors
        config = FlowConfig(**config_data)
        assert config.home_config.options["level1"] is not None

    def test_empty_configuration(self):
        """Test handling of empty configuration."""
        with pytest.raises(Exception):
            FlowConfig()

    def test_none_configuration_values(self):
        """Test handling of None values in inappropriate places."""
        with pytest.raises(Exception):
            FlowConfig(home=None, store=None)

    def test_empty_string_configuration(self):
        """Test configuration with empty strings."""
        config = FlowConfig(home="", store="")

        # Empty strings should be handled
        assert config.home_config.path == ""
        assert config.store_config.path == ""

    def test_unexpected_field_types(self):
        """Test configuration with unexpected field types."""
        # Test with list instead of dict
        invalid_config = {
            "home": ["data", "test.parquet"],  # List instead of string or dict
            "store": "data/output"
        }

        with pytest.raises(Exception):
            FlowConfig(**invalid_config)

    def test_mixed_type_configuration(self):
        """Test configuration mixing valid and invalid types."""
        mixed_config = {
            "home": "data/test.parquet",  # Valid simple config
            "store": {"invalid_field": "value"}  # Invalid store config
        }

        with pytest.raises(Exception):
            FlowConfig(**mixed_config)


class TestSettingsErrorScenarios:
    """Test suite for HyggeSettings error scenarios."""

    def test_invalid_settings_values(self):
        """Test HyggeSettings with invalid values."""
        # Invalid home type
        with pytest.raises(Exception):
            HyggeSettings(home_type="invalid")

        # Invalid compression
        with pytest.raises(Exception):
            HyggeSettings(store_compression="invalid")

        # Negative batch sizes
        with pytest.raises(Exception):
            HyggeSettings(home_batch_size=-1)

        # Queue size out of range
        with pytest.raises(Exception):
            HyggeSettings(flow_queue_size=101)

    def test_settings_boundary_values(self):
        """Test HyggeSettings with boundary values."""
        # Maximum allowed values
        settings = HyggeSettings(
            home_batch_size=1,  # Minimum
            store_batch_size=1,  # Minimum
            flow_queue_size=1,  # Minimum
            flow_timeout=1  # Minimum
        )

        assert settings.home_batch_size == 1
        assert settings.home_batch_size == 1
        assert settings.flow_queue_size == 1
        assert settings.flow_timeout == 1

        # Maximum queue size
        settings = HyggeSettings(flow_queue_size=100)
        assert settings.flow_queue_size == 100

    def test_settings_extreme_values(self):
        """Test HyggeSettings with extreme values."""
        # Very large batch sizes
        settings = HyggeSettings(
            home_batch_size=999999999,
            store_batch_size=999999999,
            flow_timeout=86400  # 24 hours
        )

        assert settings.home_batch_size == 999999999
        assert settings.store_batch_size == 999999999
        assert settings.flow_timeout == 86400


class TestFileSystemErrorScenarios:
    """Test suite for file system related configuration errors."""

    def test_configuration_with_non_existent_files(self, temp_dir):
        """Test configuration referencing non-existent files."""
        non_existent_path = temp_dir / "non_existent.parquet"

        config_data = {
            "home": str(non_existent_path),
            "store": str(temp_dir / "output")
        }

        # Configuration parsing should succeed (files checked later)
        config = FlowConfig(**config_data)
        assert config.home_config.path == str(non_existent_path)

    def test_configuration_with_directory_paths(self, temp_dir):
        """Test configuration with directory paths instead of files."""
        temp_dir.mkdir(parents=True, exist_ok=True)

        config_data = {
            "home": str(temp_dir),  # Directory instead of file
            "store": str(temp_dir / "output")
        }

        # Configuration should parse (actual path validation happens in execution)
        config = FlowConfig(**config_data)
        assert config.home_config.path == str(temp_dir)

    def test_configuration_with_protected_paths(self):
        """Test configuration with system-protected paths."""
        protected_paths = [
            "/etc/passwd",
            "/sys/kernel",
            "/proc/cpuinfo",
            "C:\\Windows\\System32"
        ]

        for path in protected_paths:
            config_data = {
                "home": path,
                "store": "/tmp/output"  # Safe output path
            }

            # Configuration should parse (access validation happens later)
            config = FlowConfig(**config_data)
            assert config.home_config.path == path


class TestNetworkErrorScenarios:
    """Test suite for network related configuration errors."""

    def test_invalid_sql_connection_strings(self):
        """Test configuration with invalid SQL connection strings."""
        invalid_connections = [
            "not_a_url",
            "postgresql://",
            "sqlite:///",
            "mysql://invalid:invalid@nonexistent:9999/nonexistent",
            ""  # Empty connection
        ]

        for connection in invalid_connections:
            # Test that connection string parsing doesn't crash
            config_data = {
                "home": {"type": "sql", "connection": connection},
                "store": "data/output"
            }

            if connection:  # Empty connection fails validation
                # This tests Pydantic validation, not actual connection
                try:
                    config = FlowConfig(**config_data)
                    assert config.home_config.connection == connection
                except Exception:
                    # Some invalid formats should raise validation errors
                    pass

    def test_sql_configuration_without_required_fields(self):
        """Test SQL configuration missing required fields."""
        incomplete_sql_configs = [
            {"type": "sql"},  # Missing connection
            {"type": "sql", "connection": None},  # None connection
            {"type": "sql", "connection": ""},  # Empty connection
        ]

        for sql_config in incomplete_sql_configs:
            config_data = {
                "home": sql_config,
                "store": "data/output"
            }

            with pytest.raises(Exception):
                FlowConfig(**config_data)


class TestConfigurationPerformanceErrors:
    """Test suite for performance-related configuration errors."""

    def test_configuration_resource_exhaustion(self):
        """Test configuration behavior under resource constraints."""
        # Very large options dictionary
        massive_options = {f"key_{i}": f"value_{i}" for i in range(100000)}

        config_data = {
            "home": {"type": "parquet", "path": "test.parquet", "options": massive_options},
            "store": "data/output"
        }

        # Should handle large options without crashing
        config = FlowConfig(**config_data)
        assert len(config.home_config.options) == 100000

    def test_recursive_configuration_parsing(self):
        """Test configuration with potential recursion issues."""
        # Create a configuration that references itself in options
        recursive_config = {"self_ref": {}}
        recursive_config["self_ref"] = recursive_config

        config_data = {
            "home": {"type": "parquet", "path": "test.parquet", "options": recursive_config},
            "store": "data/output"
        }

        # Pydantic should handle recursive structures
        config = FlowConfig(**config_data)
        # Note: Pydantic will break the recursion during serialization

    def test_malformed_binary_data_in_options(self):
        """Test configuration with binary-like data in options."""
        binary_data = b'\x00\x01\x02\x03'

        config_data = {
            "home": {"type": "parquet", "path": "test.parquet", "options": {}},
            "store": "data/output",
            "options": {"binary": binary_data}
        }

        # Should handle binary data appropriately
        config = FlowConfig(**config_data)
        assert config.options["binary"] == binary_data


class TestUserErrorScenarios:
    """Test suite for common user configuration errors."""

    def test_typo_in_configuration_key(self):
        """Test handling of common typos in configuration keys."""
        typo_configs = [
            {"hom": "test.parquet", "store": "output"},  # 'hom' instead of 'home'
            {"home": "test.parquet", "stor": "output"},  # 'stor' instead of 'store'
            {"home": "test.parquet", "store": "output", "optins": {}}  # 'optins' instead of 'options'
        ]

        for typo_config in typo_configs:
            with pytest.raises(Exception):
                FlowConfig(**typo_config)

    def test_case_sensitivity_errors(self):
        """Test configuration with case sensitivity issues."""
        case_configs = [
            {"HOME": "test.parquet", "store": "output"},  # Wrong case
            {"home": "test.parquet", "STORE": "output"},  # Wrong case
            {"home": "test.parquet", "Store": "output"},   # Mixed case
        ]

        for case_config in case_configs:
            with pytest.raises(Exception):
                FlowConfig(**case_config)

    def test_conflicting_configuration_options(self):
        """Test configuration with conflicting options."""
        config_data = {
            "home": {
                "type": "parquet",
                "path": "test.parquet",
                "options": {
                    "batch_size": 1000,
                    "batch_size": 2000  # Duplicate key
                }
            },
            "store": "data/output"
        }

        # Python dictionaries only keep last value for duplicate keys
        config = FlowConfig(**config_data)
        assert config.home_config.options["batch_size"] == 2000

    def test_oversized_configuration_values(self):
        """Test configuration with unreasonably large values."""
        oversized_config = {
            "home": "test.parquet",
            "store": "output",
            "options": {
                "huge_string": "x" * 1000000,  # 1MB string
                "huge_number": 999999999999999999999999999,
                "deep_list": [[[] for _ in range(100)] for _ in range(100)]
            }
        }

        # Should handle oversized but valid values
        config = FlowConfig(**oversized_config)
        assert len(config.options["huge_string"]) == 1000000
