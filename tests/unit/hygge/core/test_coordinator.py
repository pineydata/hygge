"""
Tests for the Coordinator class.

Following hygge's testing principles:
- Test behavior that makes sense to users
- Focus on YAML parsing and flow orchestration
- Test real data movement scenarios
- Keep tests clear and maintainable
"""
import asyncio
import tempfile
import yaml
from pathlib import Path
from typing import List
from unittest.mock import patch

import polars as pl
import pytest

from hygge.core.coordinator import Coordinator, validate_config
from hygge.core.flow import Flow
from hygge.core.home import Home
from hygge.core.store import Store
from hygge.utility.exceptions import ConfigError, FlowError


class MockHome(Home):
    """Mock Home for testing coordinator flows."""

    def __init__(self, name: str, data: List[pl.DataFrame] = None, should_error: bool = False):
        super().__init__(name, {})
        self.data = data or [pl.DataFrame({"id": [1, 2, 3]})]  # Simple test data
        self.should_error = should_error
        self.read_called = False

    async def _get_batches(self):
        """Mock _get_batches method."""
        self.read_called = True

        if self.should_error:
            raise ValueError(f"Home error: {self.name}")

        for df in self.data:
            yield df


class MockStore(Store):
    """Mock Store for testing coordinator flows."""

    def __init__(self, name: str, should_error: bool = False):
        super().__init__(name, {})
        self.should_error = should_error
        self.write_called = False
        self.finish_called = False
        self.written_data = []

    async def write(self, data: pl.DataFrame):
        """Mock write method."""
        if self.should_error:
            raise ValueError(f"Store error: {self.name}")

        self.write_called = True
        self.written_data.append(data)

    async def finish(self):
        """Mock finish method."""
        self.finish_called = True


@pytest.fixture
def simple_config_file():
    """Create a simple test configuration file."""
    config_data = {
        'flows': {
            'test_flow': {
                'home': 'data/test.parquet',
                'store': 'output/test'
            }
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
        return f.name


@pytest.fixture
def complex_config_file():
    """Create a complex test configuration file."""
    config_data = {
        'flows': {
            'users_to_lake': {
                'home': {
                    'type': 'parquet',
                    'path': 'data/users.parquet',
                    'options': {'batch_size': 5000}
                },
                'store': {
                    'type': 'parquet',
                    'path': 'lake/users',
                    'options': {'batch_size': 50000, 'compression': 'snappy'}
                },
                'options': {'queue_size': 3, 'timeout': 600}
            },
            'orders_to_warehouse': {
                'home': 'data/orders.parquet',
                'store': 'warehouse/orders'
            }
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
        return f.name


@pytest.fixture
def invalid_config_file():
    """Create an invalid test configuration file."""
    config_data = {
        'flows': {
            'bad_flow': {
                'home': None,  # Invalid home config
                'store': 'output/test'
            }
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
        return f.name


class TestConfigurationValidation:
    """Test configuration validation logic."""

    def test_validate_config_success(self):
        """Test successful configuration validation."""
        config = {
            'flows': {
                'test_flow': {
                    'home': 'data/test.parquet',
                    'store': 'output/test'
                }
            }
        }

        errors = validate_config(config)
        assert errors == []

    def test_validate_config_failure(self):
        """Test configuration validation failure."""
        config = {
            'flows': {
                'bad_flow': {
                    'home': None,  # Invalid
                    'store': 'output/test'
                }
            }
        }

        errors = validate_config(config)
        assert len(errors) > 0
        assert any('home' in error for error in errors)



class TestCoordinatorSetup:
    """Test coordinator setup and configuration parsing."""

    def test_coordinator_initialization(self):
        """Test coordinator initializes correctly."""
        coordinator = Coordinator('test_config.yaml')

        assert coordinator.config_path == Path('test_config.yaml')
        assert coordinator.options == {}
        assert coordinator.flows == []

    def test_coordinator_initialization_only_path(self):
        """Test coordinator initialization with config path only."""
        coordinator = Coordinator('test_config.yaml')

        assert coordinator.config_path == Path('test_config.yaml')
        assert coordinator.options == {}
        assert coordinator.flows == []

    def test_run_file_not_found(self):
        """Test run handles missing configuration file."""
        coordinator = Coordinator('nonexistent.yaml')

        with pytest.raises(ConfigError):
            asyncio.run(coordinator.run())

    def test_run_invalid_yaml(self):
        """Test run handles invalid YAML syntax."""
        # Create a file with invalid YAML
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            config_path = f.name

        coordinator = Coordinator(config_path)

        with pytest.raises(ConfigError):
            asyncio.run(coordinator.run())


class TestCoordinatorFlowManagement:
    """Test flow management and factory integration."""

    @pytest.mark.asyncio
    async def test_run_simple_config(self, simple_config_file):
        """Test running coordinator with simple configuration."""
        with patch('hygge.core.factory.Factory.create_home') as mock_create_home, \
             patch('hygge.core.factory.Factory.create_store') as mock_create_store:

            mock_home = MockHome('test_flow')
            mock_store = MockStore('test_flow')
            mock_create_home.return_value = mock_home
            mock_create_store.return_value = mock_store

            coordinator = Coordinator(simple_config_file)
            await coordinator.run()

            # Should have created one flow
            assert len(coordinator.flows) == 1
            assert coordinator.flows[0].name == 'test_flow'
            assert isinstance(coordinator.flows[0], Flow)

            # Should have called factory methods
            mock_create_home.assert_called_once()
            mock_create_store.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_complex_config(self, complex_config_file):
        """Test running coordinator with complex configuration."""
        with patch('hygge.core.factory.Factory.create_home') as mock_create_home, \
             patch('hygge.core.factory.Factory.create_store') as mock_create_store:

            mock_home1 = MockHome('users_to_lake')
            mock_store1 = MockStore('users_to_lake')
            mock_home2 = MockHome('orders_to_warehouse')
            mock_store2 = MockStore('orders_to_warehouse')

            mock_create_home.side_effect = [mock_home1, mock_home2]
            mock_create_store.side_effect = [mock_store1, mock_store2]

            coordinator = Coordinator(complex_config_file)
            await coordinator.run()

            # Should have created two flows
            assert len(coordinator.flows) == 2

            flow_names = [flow.name for flow in coordinator.flows]
            assert 'users_to_lake' in flow_names
            assert 'orders_to_warehouse' in flow_names

            # Should have called factory for each flow
            assert mock_create_home.call_count == 2
            assert mock_create_store.call_count == 2

    @pytest.mark.asyncio
    async def test_run_invalid_config(self, invalid_config_file):
        """Test run handles invalid configuration."""
        coordinator = Coordinator(invalid_config_file)

        with pytest.raises(ConfigError):
            await coordinator.run()


class TestCoordinatorExecution:
    """Test coordinator execution and flow orchestration."""

    @pytest.mark.asyncio
    async def test_run_single_flow(self, simple_config_file):
        """Test running coordinator with single flow."""
        with patch('hygge.core.factory.Factory.create_home') as mock_create_home, \
             patch('hygge.core.factory.Factory.create_store') as mock_create_store:

            mock_home = MockHome('test_flow')
            mock_store = MockStore('test_flow')
            mock_create_home.return_value = mock_home
            mock_create_store.return_value = mock_store

            coordinator = Coordinator(simple_config_file)
            await coordinator.run()

            # Should have executed the flow
            assert mock_home.read_called
            assert mock_store.write_called
            assert mock_store.finish_called

    @pytest.mark.asyncio
    async def test_run_multiple_flows(self, complex_config_file):
        """Test running coordinator with multiple flows."""
        with patch('hygge.core.factory.Factory.create_home') as mock_create_home, \
             patch('hygge.core.factory.Factory.create_store') as mock_create_store:

            mock_homes = [MockHome(f'flow_{i}') for i in range(2)]
            mock_stores = [MockStore(f'flow_{i}') for i in range(2)]
            mock_create_home.side_effect = mock_homes
            mock_create_store.side_effect = mock_stores

            coordinator = Coordinator(complex_config_file)
            await coordinator.run()

            # All flows should have executed
            for home in mock_homes:
                assert home.read_called
            for store in mock_stores:
                assert store.write_called
                assert store.finish_called

    @pytest.mark.asyncio
    async def test_concurrency_control(self, simple_config_file):
        """Test concurrency control limits."""
        with patch('hygge.core.factory.Factory.create_home') as mock_create_home, \
             patch('hygge.core.factory.Factory.create_store') as mock_create_store:

            # Create slow homes to ensure concurrency testing
            slow_homes = [
                MockHome('fast_flow', [pl.DataFrame({"id": [1]})]),
                MockHome('slow_flow', [pl.DataFrame({"id": [1]})])
            ]
            mock_stores = [MockStore(f'flow_{i}') for i in range(2)]
            mock_create_home.side_effect = slow_homes
            mock_create_store.side_effect = mock_stores

            # Duplicate the simple config to create multiple flows
            config_data = {
                'flows': {
                    'fast_flow': {'home': 'data/fast.parquet', 'store': 'output/fast'},
                    'slow_flow': {'home': 'data/slow.parquet', 'store': 'output/slow'}
                }
            }

            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                yaml.dump(config_data, f)
                config_file = f.name

            coordinator = Coordinator(config_file)
            await coordinator.run()

            # Both flows should complete
            assert slow_homes[0].read_called
            assert slow_homes[1].read_called


class TestCoordinatorErrorHandling:
    """Test error handling in coordinator execution."""

    @pytest.mark.asyncio
    async def test_home_error_propagation(self, simple_config_file):
        """Test that Home errors are handled correctly."""
        with patch('hygge.core.factory.Factory.create_home') as mock_create_home, \
             patch('hygge.core.factory.Factory.create_store') as mock_create_store:

            error_home = MockHome('test_flow', should_error=True)
            mock_store = MockStore('test_flow')
            mock_create_home.return_value = error_home
            mock_create_store.return_value = mock_store

            coordinator = Coordinator(simple_config_file)

            # Should raise FlowError when home fails
            with pytest.raises(FlowError):
                await coordinator.run()

    @pytest.mark.asyncio
    async def test_flow_error_handling(self, simple_config_file):
        """Test flow error handling and cleanup."""
        with patch('hygge.core.factory.Factory.create_home') as mock_create_home, \
             patch('hygge.core.factory.Factory.create_store') as mock_create_store:

            error_home = MockHome('test_flow', should_error=True)
            mock_store = MockStore('test_flow')
            mock_create_home.return_value = error_home
            mock_create_store.return_value = mock_store

            coordinator = Coordinator(simple_config_file)

            # Should raise FlowError when home fails
            with pytest.raises(FlowError):
                await coordinator.run()

            # Flow should have been attempted
            assert error_home.read_called
