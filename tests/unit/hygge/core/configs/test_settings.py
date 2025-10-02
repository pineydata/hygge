"""
Tests for HyggeSettings configuration system.

Following hygge's testing principles:
- Test smart defaults work correctly
- Verify validation catches bad values
- Ensure user experience is smooth
- Test configuration application methods
"""
import pytest
from pydantic import ValidationError

from hygge.core.configs.settings import HyggeSettings


class TestHyggeSettings:
    """
    Test suite for HyggeSettings focusing on:
    - Smart defaults that work for users
    - Validation that catches configuration errors
    - Settings application methods
    - Environment integration
    """

    def test_default_settings_values(self):
        """Test that default settings provide good defaults."""
        settings = HyggeSettings()

        # Type defaults - should be practical
        assert settings.home_type == 'parquet'
        assert settings.store_type == 'parquet'

        # Home settings - optimized for reading
        assert settings.home_batch_size == 10000
        assert settings.home_row_multiplier == 300000

        # Store settings - optimized for writing
        assert settings.store_batch_size == 100000
        assert settings.store_compression == 'snappy'

        # Flow settings - reasonable defaults
        assert settings.flow_queue_size == 10
        assert settings.flow_timeout == 300

    def test_custom_settings_values(self):
        """Test that custom settings work correctly."""
        settings = HyggeSettings(
            home_type='sql',
            store_type='parquet',
            home_batch_size=5000,
            store_batch_size=50000,
            flow_queue_size=5
        )

        assert settings.home_type == 'sql'
        assert settings.store_type == 'parquet'
        assert settings.home_batch_size == 5000
        assert settings.store_batch_size == 50000
        assert settings.flow_queue_size == 5

    def test_invalid_home_type_validation(self):
        """Test validation catches invalid home types."""
        with pytest.raises(ValidationError) as exc_info:
            HyggeSettings(home_type='invalid_type')

        error = exc_info.value.errors()[0]
        assert error['type'] == 'value_error'
        assert 'Type must be one of' in str(error['ctx']['error'])

    def test_invalid_store_type_validation(self):
        """Test validation catches invalid store types."""
        with pytest.raises(ValidationError) as exc_info:
            HyggeSettings(store_type='invalid_type')

        error = exc_info.value.errors()[0]
        assert error['type'] == 'value_error'
        assert 'Type must be one of' in str(error['ctx']['error'])

    def test_invalid_compression_validation(self):
        """Test validation catches invalid compression settings."""
        with pytest.raises(ValidationError) as exc_info:
            HyggeSettings(store_compression='invalid_compression')

        error = exc_info.value.errors()[0]
        assert error['type'] == 'value_error'
        assert 'Compression must be one of' in str(error['ctx']['error'])

    def test_batch_size_minimum_validation(self):
        """Test batch size minimum value validation."""
        # Test home batch size
        with pytest.raises(ValidationError) as exc_info:
            HyggeSettings(home_batch_size=0)

        error = exc_info.value.errors()[0]
        assert error['type'] == 'greater_than_equal'

        # Test store batch size
        with pytest.raises(ValidationError) as exc_info:
            HyggeSettings(store_batch_size=-1)

        error = exc_info.value.errors()[0]
        assert error['type'] == 'greater_than_equal'

    def test_flow_queue_size_range_validation(self):
        """Test flow queue size stays within reasonable bounds."""
        # Test maximum value
        with pytest.raises(ValidationError) as exc_info:
            HyggeSettings(flow_queue_size=101)

        error = exc_info.value.errors()[0]
        assert error['type'] == 'less_than_equal'

        # Test minimum value
        with pytest.raises(ValidationError) as exc_info:
            HyggeSettings(flow_queue_size=0)

        error = exc_info.value.errors()[0]
        assert error['type'] == 'greater_than_equal'

    def test_timeout_minimum_validation(self):
        """Test timeout minimum value validation."""
        with pytest.raises(ValidationError) as exc_info:
            HyggeSettings(flow_timeout=0)

        error = exc_info.value.errors()[0]
        assert error['type'] == 'greater_than_equal'

    def test_get_methods_return_correct_values(self):
        """Test getter methods return properly formatted values."""
        settings = HyggeSettings(
            home_type='sql',
            store_type='parquet',
            home_batch_size=5000
        )

        assert settings.get_home_type() == 'sql'
        assert settings.get_store_type() == 'parquet'

    def test_get_home_settings(self):
        """Test home settings getter returns correct format."""
        settings = HyggeSettings(
            home_batch_size=5000,
            home_row_multiplier=150000
        )

        home_settings = settings.get_home_settings()

        assert home_settings['batch_size'] == 5000
        assert home_settings['row_multiplier'] == 150000
        assert isinstance(home_settings, dict)

    def test_get_store_settings(self):
        """Test store settings getter returns correct format."""
        settings = HyggeSettings(
            store_batch_size=50000,
            store_compression='zstd'
        )

        store_settings = settings.get_store_settings()

        assert store_settings['batch_size'] == 50000
        assert store_settings['compression'] == 'zstd'
        assert isinstance(store_settings, dict)

    def test_get_flow_settings(self):
        """Test flow settings getter returns correct format."""
        settings = HyggeSettings(
            flow_queue_size=5,
            flow_timeout=180
        )

        flow_settings = settings.get_flow_settings()

        assert flow_settings['queue_size'] == 5
        assert flow_settings['timeout'] == 180
        assert isinstance(flow_settings, dict)

    def test_apply_home_settings_preserves_existing(self):
        """Test that apply_home_settings preserves user values."""
        settings = HyggeSettings(home_batch_size=5000)

        user_options = {
            'batch_size': 2000,  # Should override default
            'custom_option': 'value'  # Should be preserved
        }

        result = settings.apply_home_settings(user_options)

        assert result['batch_size'] == 2000  # User value wins
        assert result['row_multiplier'] == 300000  # Default applied
        assert result['custom_option'] == 'value'  # User value preserved

    def test_apply_store_settings_preserves_existing(self):
        """Test that apply_store_settings preserves user values."""
        settings = HyggeSettings(store_batch_size=50000)

        user_options = {
            'compression': 'gzip',  # Should override default
            'custom_option': 'value'  # Should be preserved
        }

        result = settings.apply_store_settings(user_options)

        assert result['compression'] == 'gzip'  # User value wins
        assert result['batch_size'] == 50000  # Default applied
        assert result['custom_option'] == 'value'  # User value preserved

    def test_apply_flow_settings_preserves_existing(self):
        """Test that apply_flow_settings preserves user values."""
        settings = HyggeSettings(flow_queue_size=5)

        user_options = {
            'timeout': 600,  # Should override default
            'custom_option': 'value'  # Should be preserved
        }

        result = settings.apply_flow_settings(user_options)

        assert result['timeout'] == 600  # User value wins
        assert result['queue_size'] == 5  # Default applied
        assert result['custom_option'] == 'value'  # User value preserved

    def test_apply_settings_with_empty_options(self):
        """Test applying settings with empty options."""
        settings = HyggeSettings()

        empty_options = {}
        result = settings.apply_home_settings(empty_options)

        # Should return only defaults
        assert result == settings.get_home_settings()
        assert len(result) == 2  # Only batch_size and row_multiplier

    def test_global_settings_instance(self):
        """Test that global settings instance works correctly."""
        from hygge.core.configs.settings import settings

        # Should be a HyggeSettings instance
        assert isinstance(settings, HyggeSettings)

        # Should have default values
        assert settings.home_type == 'parquet'
        assert settings.store_type == 'parquet'

    # Edge Cases and Error Scenarios

    def test_multiple_validation_errors(self):
        """Test that multiple validation errors are reported correctly."""
        with pytest.raises(ValidationError) as exc_info:
            HyggeSettings(
                home_type='invalid',
                store_type='invalid',
                home_batch_size=-1,
                store_batch_size=0
            )

        errors = exc_info.value.errors()
        # Should have multiple validation errors
        assert len(errors) >= 3

    def test_type_case_sensitivity(self):
        """Test that type validation is case sensitive."""
        with pytest.raises(ValidationError):
            HyggeSettings(home_type='PARQUET')  # Wrong case

        # Correct case should work
        settings = HyggeSettings(home_type='parquet')
        assert settings.home_type == 'parquet'

    def test_none_values_validation(self):
        """Test that None values are properly validated."""
        # These should raise validation errors
        fields_to_test = [
            ('home_type', None),
            ('store_type', None),
            ('home_batch_size', None),
            ('store_batch_size', None),
            ('flow_queue_size', None),
            ('flow_timeout', None)
        ]

        for field, value in fields_to_test:
            with pytest.raises(ValidationError):
                HyggeSettings(**{field: value})

    def test_settings_model_config(self):
        """Test that Pydantic model configuration works correctly."""
        settings = HyggeSettings()

        # Should have standard Pydantic model methods
        assert hasattr(settings, 'model_dump')
        assert hasattr(settings, 'model_json_schema')

        # Should be able to serialize/deserialize
        data = settings.model_dump()
        new_settings = HyggeSettings(**data)
        assert new_settings.home_type == settings.home_type
