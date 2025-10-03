"""
Tests for the Factory class.

Following hygge's testing principles:
- Test behavior that matters to users
- Focus on extensibility and error handling
- Keep tests clear and maintainable
"""
import pytest

from hygge.core.factory import Factory
from hygge.core.home import Home
from hygge.core.store import Store


class TestFactory:
    """Test suite for Factory class."""

    def test_get_supported_types(self):
        """Test getting supported type lists."""
        factory = Factory()
        home_types = factory.get_supported_home_types()
        store_types = factory.get_supported_store_types()

        assert 'parquet' in home_types
        assert 'parquet' in store_types

        # Should return lists, not the actual mappings
        assert isinstance(home_types, list)
        assert isinstance(store_types, list)

    def test_create_home_with_valid_type(self):
        """Test creating home with valid type."""
        # Mock config object with required methods
        class MockConfig:
            def __init__(self):
                self.type = 'parquet'
                self.path = 'test/path.parquet'
                self.options = {}

            def get_merged_options(self):
                return {'batch_size': 10000}

        config = MockConfig()
        factory = Factory()
        home = factory.create_home('test_home', config)

        # Should create a Home instance
        assert isinstance(home, Home)

    def test_create_home_with_invalid_type(self):
        """Test creating home with invalid type."""
        class MockConfig:
            def __init__(self):
                self.type = 'invalid_type'

        config = MockConfig()
        factory = Factory()

        with pytest.raises(ValueError) as exc_info:
            factory.create_home('test_home', config)

        assert "Unsupported home type: invalid_type" in str(exc_info.value)

    def test_create_store_with_valid_type(self):
        """Test creating store with valid type."""
        class MockConfig:
            def __init__(self):
                self.type = 'parquet'
                self.path = 'test/output'
                self.options = {}

            def get_merged_options(self, flow_name=None):
                return {'batch_size': 100000, 'compression': 'snappy'}

        config = MockConfig()
        factory = Factory()
        store = factory.create_store('test_store', config)

        # Should create a Store instance
        assert isinstance(store, Store)

    def test_create_store_with_invalid_type(self):
        """Test creating store with invalid type."""
        class MockConfig:
            def __init__(self):
                self.type = 'invalid_type'

        config = MockConfig()
        factory = Factory()

        with pytest.raises(ValueError) as exc_info:
            factory.create_store('test_store', config)

        assert "Unsupported store type: invalid_type" in str(exc_info.value)

    def test_register_home_type(self):
        """Test registering new home type."""
        # Create a mock home class with proper constructor
        class MockHome(Home):
            def __init__(self, name, config, **kwargs):
                super().__init__(name, kwargs)

        factory = Factory()

        # Register the new type
        factory.register_home_type('mock', MockHome)

        # Should now be available
        assert 'mock' in factory.get_supported_home_types()

        # Should be able to create it
        class MockConfig:
            def __init__(self):
                self.type = 'mock'
                self.path = 'test/path'
                self.options = {}

            def get_merged_options(self):
                return {'batch_size': 10000}

        config = MockConfig()
        home = factory.create_home('test_mock', config)
        assert isinstance(home, MockHome)

    def test_register_store_type(self):
        """Test registering new store type."""
        # Create a mock store class with proper constructor
        class MockStore(Store):
            def __init__(self, name, config, flow_name=None, **kwargs):
                super().__init__(name, kwargs)

        factory = Factory()

        # Register the new type
        factory.register_store_type('mock', MockStore)

        # Should now be available
        assert 'mock' in factory.get_supported_store_types()

        # Should be able to create it
        class MockConfig:
            def __init__(self):
                self.type = 'mock'
                self.path = 'test/output'
                self.options = {}

            def get_merged_options(self, flow_name=None):
                return {'batch_size': 100000}

        config = MockConfig()
        store = factory.create_store('test_mock', config)
        assert isinstance(store, MockStore)

    def test_type_mappings_are_instance_attributes(self):
        """Test that type mappings are instance attributes."""
        factory = Factory()

        # Should be able to access instance attributes
        assert hasattr(factory, '_home_types')
        assert hasattr(factory, '_store_types')

        # Should be dictionaries
        assert isinstance(factory._home_types, dict)
        assert isinstance(factory._store_types, dict)

        # Should contain expected types
        assert 'parquet' in factory._home_types
        assert 'parquet' in factory._store_types
