"""
Integration tests for Coordinator with real data movement.

These tests verify end-to-end functionality:
- YAML configuration parsing into actual flows
- Real Home/Store instantiation via HyggeFactory
- Data movement with actual parquet files
- Error scenarios and recovery
"""
import pytest
import tempfile
import polars as pl
from pathlib import Path
import yaml
from unittest.mock import patch

from hygge.core.coordinator import Coordinator
from hygge.core.factory import HyggeFactory
from hygge.utility.exceptions import ConfigError


@pytest.fixture
def sample_parquet_file():
    """Create a sample parquet file for testing."""
    # Create sample data
    data = pl.DataFrame({
        'id': range(100),
        'name': [f'user_{i}' for i in range(100)],
        'value': [i * 10 for i in range(100)]
    })

    # Write to temporary file
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        data.write_parquet(f.name)
        yield f.name

    # Cleanup
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def output_directory():
    """Create output directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def simple_coordinator_config(sample_parquet_file, output_directory):
    """Create a simple coordinator configuration."""
    config_data = {
        'flows': {
            'test_parquet_flow': {
                'home': sample_parquet_file,
                'store': output_directory
            }
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
        yield f.name

    # Cleanup
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def advanced_coordinator_config(sample_parquet_file, output_directory):
    """Create an advanced coordinator configuration."""
    config_data = {
        'flows': {
            'advanced_parquet_flow': {
                'home': {
                    'type': 'parquet',
                    'path': sample_parquet_file,
                    'options': {'batch_size': 50}
                },
                'store': {
                    'type': 'parquet',
                    'path': output_directory,
                    'options': {'batch_size': 100, 'compression': 'snappy'}
                },
                'options': {'queue_size': 2, 'timeout': 100}
            }
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
        yield f.name

    # Cleanup
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def multi_flow_config(sample_parquet_file, output_directory):
    """Create a multi-flow configuration."""
    flow1_data = pl.DataFrame({'id': range(50), 'type': 'A'})
    flow2_data = pl.DataFrame({'id': range(50, 100), 'type': 'B'})

    # Create separate parquet files for each flow
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f1:
        flow1_data.write_parquet(f1.name)
        flow1_path = f1.name

    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f2:
        flow2_data.write_parquet(f2.name)
        flow2_path = f2.name

    config_data = {
        'flows': {
            'flow_a': {
                'home': flow1_path,
                'store': f'{output_directory}/flow_a'
            },
            'flow_b': {
                'home': flow2_path,
                'store': f'{output_directory}/flow_b'
            }
        }
    }

    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
        config_path = f.name

    yield config_path, flow1_path, flow2_path

    # Cleanup
    for path in [config_path, flow1_path, flow2_path]:
        Path(path).unlink(missing_ok=True)


class TestCoordinatorRealDataMovement:
    """Test coordinator with real parquet data movement."""

    @pytest.mark.asyncio
    async def test_simple_parquet_to_parquet_flow(self, simple_coordinator_config, sample_parquet_file, output_directory):
        """Test simple parquet to parquet flow via coordinator."""
        # When running coordinator
        coordinator = Coordinator(simple_coordinator_config)
        await coordinator.setup()
        await coordinator.start()

        # Then should have created one flow
        assert len(coordinator.flows) == 1
        flow = coordinator.flows[0]
        assert flow.name == 'test_parquet_flow'

        # And flow should have processed data
        assert flow.total_rows > 0
        assert flow.batches_processed > 0

        # And data should be written to output directory
        output_files = list(Path(output_directory).glob('*.parquet'))
        assert len(output_files) > 0

        # Verify data integrity
        output_data = pl.read_parquet(output_files[0])
        input_data = pl.read_parquet(sample_parquet_file)

        # Should have same number of rows
        assert len(output_data) == len(input_data)

        # Should have same column names
        assert set(output_data.columns) == set(input_data.columns)

    @pytest.mark.asyncio
    async def test_advanced_parquet_configuration(self, advanced_coordinator_config, sample_parquet_file, output_directory):
            """Test advanced parquet configuration with custom options."""
            # When running coordinator with advanced config
            coordinator = Coordinator(advanced_coordinator_config)
            await coordinator.setup()
            await coordinator.start()

            # Then should have created one flow with custom options
            assert len(coordinator.flows) == 1
            flow = coordinator.flows[0]

            # Should have applied custom queue size from config
            assert flow.queue_size == 2  # From config options

            # Should have processed data
            assert flow.total_rows > 0
            assert flow.batches_processed > 0

    @pytest.mark.asyncio
    async def test_multiple_flows_orchestration(self, multi_flow_config, output_directory):
        """Test coordinator orchestrates multiple flows correctly."""
        config_path, flow1_path, flow2_path = multi_flow_config

        # When running coordinator with multiple flows
        coordinator = Coordinator(config_path, {'max_concurrent': 2})
        await coordinator.setup()
        await coordinator.start()

        # Then should have created two flows
        assert len(coordinator.flows) == 2

        flow_names = [flow.name for flow in coordinator.flows]
        assert 'flow_a' in flow_names
        assert 'flow_b' in flow_names

        # Both flows should have processed data
        for flow in coordinator.flows:
            assert flow.total_rows > 0
            assert flow.batches_processed > 0

        # Data should be written to separate directories
        flow_a_files = list(Path(f'{output_directory}/flow_a').glob('*.parquet'))
        flow_b_files = list(Path(f'{output_directory}/flow_b').glob('*.parquet'))

        assert len(flow_a_files) > 0
        assert len(flow_b_files) > 0

        # Verify both flows are complete
        for flow in coordinator.flows:
            assert flow.store.finish_called


class TestCoordinatorErrorScenarios:
    """Test coordinator error handling with real scenarios."""

    @pytest.mark.asyncio
    async def test_config_file_not_found(self):
        """Test coordinator handles missing config file."""
        coordinator = Coordinator('nonexistent_config.yaml')

        with pytest.raises(ConfigError) as exc_info:
            await coordinator.setup()

        assert "Configuration file not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_invalid_yaml_syntax(self):
        """Test coordinator handles invalid YAML."""
        # Create invalid YAML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid:\n  yaml: content: [")  # Invalid YAML
            config_path = f.name

        coordinator = Coordinator(config_path)

        with pytest.raises(ConfigError) as exc_info:
            await coordinator.setup()

        assert "Invalid YAML syntax" in str(exc_info.value)

        # Cleanup
        Path(config_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_missing_parquet_file(self, output_directory):
        """Test coordinator handles missing source parquet file."""
        config_data = {
            'flows': {
                'missing_file_flow': {
                    'home': 'nonexistent_file.parquet',
                    'store': output_directory
                }
            }
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            config_path = f.name

        coordinator = Coordinator(config_path)

        # Setup should succeed (it just creates flow config)
        await coordinator.setup()

        # But running should fail when trying to read missing file
        with pytest.raises(Exception):
            await coordinator.start()

        # Cleanup
        Path(config_path).unlink(missing_ok=True)


class TestCoordinatorConfigurationEdgeCases:
    """Test coordinator with various configuration edge cases."""

    @pytest.mark.asyncio
    async def test_empty_flows_config(self):
        """Test coordinator handles empty flows configuration."""
        config_data = {'flows': {}}

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            config_path = f.name

        coordinator = Coordinator(config_path)
        await coordinator.setup()
        await coordinator.start()

        # Should complete successfully with no flows
        assert len(coordinator.flows) == 0

        # Cleanup
        Path(config_path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_concurrency_limits(self, multi_flow_config, output_directory):
        """Test coordinator respects concurrency limits."""
        config_path, flow1_path, flow2_path = multi_flow_config

        # When running with concurrency limit of 1
        with patch('asyncio.sleep') as mock_sleep:
            mock_sleep.side_effect = lambda x: None  # Speed up tests

            coordinator = Coordinator(config_path, {'max_concurrent': 1})
            await coordinator.setup()
            await coordinator.start()

        # Should still complete successfully
        assert len(coordinator.flows) == 2

        # Both flows should have processed data
        for flow in coordinator.flows:
            assert flow.total_rows > 0
            assert flow.batches_processed > 0
