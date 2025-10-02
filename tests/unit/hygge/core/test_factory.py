"""
Tests for the HyggeFactory class.

Following hygge's testing principles:
- Test behavior that matters to users
- Focus on extensibility and error handling
- Keep tests clear and maintainable
"""
import pytest

from hygge.core.factory import HyggeFactory
from hygge.core.home import Home
from hygge.core.store import Store


class TestHyggeFactory:
    """Test suite for HyggeFactory class."""

    def test_get_supported_types(self):
        """Test getting supported type lists."""
        home_types = HyggeFactory.get_supported_home_types()
        store_types = HyggeFactory.get_supported_store_types()

        assert 'parquet' in home_types
        assert 'sql' in home_types
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
        home = HyggeFactory.create_home('test_home', config)

        # Should create a Home instance
        assert isinstance(home, Home)
        assert home.name == 'test_home'

    def test_create_home_with_invalid_type(self):
        """Test creating home with invalid type."""
        class MockConfig:
            def __init__(self):
                self.type = 'invalid_type'

        config = MockConfig()

        with pytest.raises(ValueError) as exc_info:
            HyggeFactory.create_home('test_home', config)

        assert "Unknown home type: invalid_type" in str(exc_info.value)
        assert "Available types:" in str(exc_info.value)

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
        store = HyggeFactory.create_store('test_store', 'test_flow', config)

        # Should create a Store instance
        assert isinstance(store, Store)
        assert store.name == 'test_store'

    def test_create_store_with_invalid_type(self):
        """Test creating store with invalid type."""
        class MockConfig:
            def __init__(self):
                self.type = 'invalid_type'

        config = MockConfig()

        with pytest.raises(ValueError) as exc_info:
            HyggeFactory.create_store('test_store', 'test_flow', config)

        assert "Unknown store type: invalid_type" in str(exc_info.value)
        assert "Available types:" in str(exc_info.value)

    def test_register_home_type(self):
        """Test registering new home type."""
        # Create a mock home class with proper constructor
        class MockHome(Home):
            def __init__(self, name, config, **kwargs):
                super().__init__(name, kwargs)

        # Register the new type
        HyggeFactory.register_home_type('mock', MockHome)

        # Should now be available
        assert 'mock' in HyggeFactory.get_supported_home_types()

        # Should be able to create it
        class MockConfig:
            def __init__(self):
                self.type = 'mock'
                self.path = 'test/path'
                self.options = {}

            def get_merged_options(self):
                return {'batch_size': 10000}

        config = MockConfig()
        home = HyggeFactory.create_home('test_mock', config)
        assert isinstance(home, MockHome)

    def test_register_store_type(self):
        """Test registering new store type."""
        # Create a mock store class with proper constructor
        class MockStore(Store):
            def __init__(self, name, config, flow_name=None, **kwargs):
                super().__init__(name, kwargs)

        # Register the new type
        HyggeFactory.register_store_type('mock', MockStore)

        # Should now be available
        assert 'mock' in HyggeFactory.get_supported_store_types()

        # Should be able to create it
        class MockConfig:
            def __init__(self):
                self.type = 'mock'
                self.path = 'test/output'
                self.options = {}

            def get_merged_options(self, flow_name=None):
                return {'batch_size': 100000}

        config = MockConfig()
        store = HyggeFactory.create_store('test_mock', 'test_flow', config)
        assert isinstance(store, MockStore)

    def test_type_mappings_are_class_attributes(self):
        """Test that type mappings are class attributes, not instance attributes."""
        # Should be able to access without instantiation
        assert hasattr(HyggeFactory, 'HOME_TYPES')
        assert hasattr(HyggeFactory, 'STORE_TYPES')

        # Should be dictionaries
        assert isinstance(HyggeFactory.HOME_TYPES, dict)
        assert isinstance(HyggeFactory.STORE_TYPES, dict)

        # Should contain expected types
        assert 'parquet' in HyggeFactory.HOME_TYPES
        assert 'sql' in HyggeFactory.HOME_TYPES
        assert 'parquet' in HyggeFactory.STORE_TYPES
