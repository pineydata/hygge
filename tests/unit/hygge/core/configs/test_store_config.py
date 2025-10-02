"""
Tests for StoreConfig configuration parsing.

Following hygge's configuration philosophy:
- Test store type validation
- Verify required fields
- Test configuration parsing
- Test error scenarios
"""
import pytest
from pydantic import ValidationError

from hygge.core.configs.store_config import StoreConfig


class TestStoreConfig:
    """
    Test suite for StoreConfig focusing on:
    - Type validation (parquet)
    - Required field validation
    - Configuration parsing
    - Error scenarios
    """

    def test_parquet_store_config(self):
        """Test parquet store configuration."""
        config = StoreConfig(
            type="parquet",
            path="data/lake/users"
        )

        assert config.type == "parquet"
        assert config.path == "data/lake/users"
        assert config.options == {}

    def test_parquet_store_with_options(self):
        """Test parquet store configuration with options."""
        config = StoreConfig(
            type="parquet",
            path="data/lake/users",
            options={
                "compression": "snappy",
                "batch_size": 10000,
                "custom_option": "value"
            }
        )

        assert config.type == "parquet"
        assert config.path == "data/lake/users"
        assert config.options["compression"] == "snappy"
        assert config.options["batch_size"] == 10000
        assert config.options["custom_option"] == "value"

    def test_store_with_empty_options(self):
        """Test store configuration with explicit empty options."""
        config = StoreConfig(
            type="parquet",
            path="data/lake/users",
            options={}
        )

        assert config.options == {}

    # Error Scenarios

    def test_invalid_store_type(self):
        """Test validation catches invalid store types."""
        with pytest.raises(ValidationError) as exc_info:
            StoreConfig(
                type="invalid_type",
                path="data/lake/users"
            )

        error = exc_info.value.errors()[0]
        assert error['type'] == 'value_error'
        assert 'Store type must be one of' in str(error['ctx']['error'])

    def test_missing_type_field(self):
        """Test validation catches missing type field."""
        with pytest.raises(ValidationError) as exc_info:
            StoreConfig(
                path="data/lake/users"
                # Missing required type
            )

        error = exc_info.value.errors()[0]
        assert error['type'] == 'missing'
        assert error['loc'] == ('type',)

    def test_missing_path_field(self):
        """Test validation catches missing path field."""
        with pytest.raises(ValidationError) as exc_info:
            StoreConfig(
                type="parquet"
                # Missing required path
            )

        error = exc_info.value.errors()[0]
        assert error['type'] == 'missing'
        assert error['loc'] == ('path',)

    def test_empty_path_value(self):
        """Test that empty string path is valid (Path can be empty)."""
        config = StoreConfig(
            type="parquet",
            path=""
        )

        assert config.path == ""

    def test_none_path_value(self):
        """Test validation catches None path."""
        with pytest.raises(ValidationError) as exc_info:
            StoreConfig(
                type="parquet",
                path=None
            )

        error = exc_info.value.errors()[0]
        # Pydantic reports this as string_type since path must be a string
        assert error['type'] == 'string_type'

    # Edge Cases

    def test_case_sensitive_type(self):
        """Test that type validation is case sensitive."""
        with pytest.raises(ValidationError):
            StoreConfig(type="PARQUET", path="data/lake/users")

    def test_long_path_string(self):
        """Test configuration with long path strings."""
        long_path = "/very/long/path/to/data/storage/" + "lake" * 100

        config = StoreConfig(
            type="parquet",
            path=long_path
        )

        assert config.path == long_path

    def test_unicode_path(self):
        """Test configuration with unicode path."""
        unicode_path = "data/用户数据/lake"

        config = StoreConfig(
            type="parquet",
            path=unicode_path
        )

        assert config.path == unicode_path

    def test_options_with_complex_values(self):
        """Test configuration with complex option values."""
        config = StoreConfig(
            type="parquet",
            path="data/lake/users",
            options={
                "compression_settings": {
                    "type": "zstd",
                    "level": 3
                },
                "paths": ["temp", "final"],
                "metadata": {
                    "author": "hygge",
                    "version": "1.0.0"
                }
            }
        )

        assert config.options["compression_settings"]["type"] == "zstd"
        assert config.options["compression_settings"]["level"] == 3
        assert len(config.options["paths"]) == 2
        assert config.options["metadata"]["author"] == "hygge"

    def test_options_with_numeric_values(self):
        """Test configuration with various numeric option values."""
        config = StoreConfig(
            type="parquet",
            path="data/lake/users",
            options={
                "batch_size": 10000,
                "file_count": 5,
                "size_threshold": 500000000,
                "ratio": 0.75
            }
        )

        assert config.options["batch_size"] == 10000
        assert config.options["file_count"] == 5
        assert config.options["size_threshold"] == 500000000
        assert config.options["ratio"] == 0.75

    def test_multiple_validation_errors(self):
        """Test that multiple validation errors are reported."""
        with pytest.raises(ValidationError) as exc_info:
            StoreConfig()
            # Missing both type and path

        errors = exc_info.value.errors()
        # Should report missing type and path
        error_types = [error['type'] for error in errors]
        assert 'missing' in error_types

    def test_pydantic_model_features(self):
        """Test standard Pydantic model features work."""
        config = StoreConfig(
            type="parquet",
            path="data/lake/users",
            options={"compression": "snappy"}
        )

        # Test serialization
        data = config.model_dump()
        assert data['type'] == "parquet"
        assert data['path'] == "data/lake/users"
        assert data['options']['compression'] == "snappy"

        # Test JSON schema
        schema = config.model_json_schema()
        assert 'properties' in schema
        assert 'type' in schema['properties']
        assert 'path' in schema['properties']
