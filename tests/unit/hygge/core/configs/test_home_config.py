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

from hygge.core.home import HomeConfig

# Import concrete implementations to register them


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
        config = HomeConfig.create({"type": "parquet", "path": "data/users.parquet"})

        assert config.type == "parquet"
        assert config.path == "data/users.parquet"
        assert config.connection is None
        assert config.options == {}

    def test_parquet_home_with_options(self):
        """Test parquet home configuration with options."""
        config = HomeConfig.create(
            {
                "type": "parquet",
                "path": "data/users.parquet",
                "options": {"batch_size": 1000, "custom_option": "value"},
            }
        )

        assert config.type == "parquet"
        assert config.path == "data/users.parquet"
        assert config.options["batch_size"] == 1000
        assert config.options["custom_option"] == "value"

    # SQL tests removed - only parquet is supported in POC

    # Error Scenarios

    def test_invalid_home_type(self):
        """Test validation catches invalid home types."""
        with pytest.raises(ValueError) as exc_info:
            HomeConfig.create({"type": "invalid_type", "path": "data/users.parquet"})

        assert "Unknown home config type: invalid_type" in str(exc_info.value)

    def test_parquet_home_without_path(self):
        """Test validation requires path for parquet homes"""
        with pytest.raises(ValidationError) as exc_info:
            HomeConfig.create(
                {
                    "type": "parquet"
                    # Missing required path
                }
            )

        error = exc_info.value.errors()[0]
        assert error["type"] == "missing"
        assert error["loc"] == ("path",)

    def test_parquet_home_with_empty_path(self):
        """Test empty path is not allowed for parquet homes."""
        with pytest.raises(ValidationError) as exc_info:
            HomeConfig.create({"type": "parquet", "path": ""})

        error = exc_info.value.errors()[0]
        assert error["type"] == "value_error"
        assert "Path is required for parquet homes" in str(error["ctx"]["error"])

    # SQL tests removed - only parquet is supported in POC

    def test_missing_type_field(self):
        """Test missing type field defaults to parquet."""
        config = HomeConfig.create(
            {
                "path": "data/users.parquet"
                # Missing type - should default to parquet
            }
        )

        assert config.type == "parquet"
        assert config.path == "data/users.parquet"

    # Edge Cases

    def test_case_sensitive_types(self):
        """Test that type validation is case sensitive."""
        with pytest.raises(ValueError) as exc_info:
            HomeConfig.create({"type": "PARQUET", "path": "data/users.parquet"})

        assert "Unknown home config type: PARQUET" in str(exc_info.value)

    # SQL tests removed - only parquet is supported in POC

    def test_empty_options_dict(self):
        """Test configuration with explicit empty options."""
        config = HomeConfig.create(
            {"type": "parquet", "path": "data/users.parquet", "options": {}}
        )

        assert config.options == {}

    # SQL tests removed - only parquet is supported in POC

    def test_long_path_string(self):
        """Test configuration with long path strings."""
        long_path = "/very/long/path/to/data/files/" + "users" * 100 + ".parquet"

        config = HomeConfig.create({"type": "parquet", "path": long_path})

        assert config.path == long_path

    # SQL tests removed - only parquet is supported in POC

    def test_multiple_validation_errors(self):
        """Test that multiple validation errors are reported."""
        with pytest.raises(ValidationError) as exc_info:
            HomeConfig.create({})
            # Missing both type and required connection/path

        errors = exc_info.value.errors()
        # Should report missing type
        assert any(error["type"] == "missing" for error in errors)

    def test_pydantic_model_features(self):
        """Test standard Pydantic model features work."""
        config = HomeConfig.create(
            {
                "type": "parquet",
                "path": "data/users.parquet",
                "options": {"batch_size": 1000},
            }
        )

        # Test serialization
        data = config.model_dump()
        assert data["type"] == "parquet"
        assert data["path"] == "data/users.parquet"
        assert data["options"]["batch_size"] == 1000

        # Test JSON schema
        schema = config.model_json_schema()
        assert "properties" in schema
        assert "type" in schema["properties"]
