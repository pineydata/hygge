"""
Integration test for complete parquet-to-parquet data movement.

Tests the complete end-to-end workflow:
1. Read from source parquet file
2. Process through Flow with producer-consumer pattern
3. Write to destination parquet files
4. Verify data integrity

Following hygge's testing philosophy:
- Focus on user experience and data integrity
- Test behavior that matters to users
- Verify complete workflows work correctly
"""
import pytest
import asyncio
import polars as pl
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any

from hygge import Flow
from hygge.homes import ParquetHome
from hygge.stores import ParquetStore


@pytest.fixture
def temp_data_dir():
    """Create temporary directory for test data."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_parquet_data(temp_data_dir: Path) -> Path:
    """Create sample parquet file with test data."""
    # Create test data similar to examples/parquet_flow.py
    df = pl.DataFrame({
        'id': range(10000),
        'value': [f'test_{i}' for i in range(10000)],
        'number': [i * 6 for i in range(10000)],
        'category': [f'cat_{i % 5}' for i in range(10000)],
        'score': [i * 0.1 for i in range(10000)]
    })

    source_file = temp_data_dir / "source.parquet"
    df.write_parquet(source_file)

    return source_file


@pytest.fixture
def flow_config(temp_data_dir: Path, sample_parquet_data: Path) -> Dict[str, Any]:
    """Create flow configuration for integration test."""
    store_dir = temp_data_dir / "store"
    store_dir.mkdir()

    return {
        'name': 'integration_test_flow',
        'home_class': ParquetHome,
        'home_config': {
            'name': 'source',
            'path': str(sample_parquet_data),
            'options': {
                'batch_size': 1000  # Small batches for testing
            }
        },
        'store_class': ParquetStore,
        'store_config': {
            'name': 'destination',
            'path': str(store_dir),
            'options': {
                'batch_size': 2000,  # Different batch size to test accumulation
                'file_pattern': "test_{sequence:020d}.parquet",
                'compression': 'snappy'
            }
        },
        'flow_options': {
            'queue_size': 3  # Small queue for testing
        }
    }


class TestParquetToParquetFlow:
    """Test complete parquet-to-parquet data movement workflow."""

    @pytest.mark.asyncio
    async def test_complete_data_movement(self, flow_config: Dict[str, Any], temp_data_dir: Path, sample_parquet_data: Path):
        """Test complete end-to-end data movement with verification."""
        # Given a flow configuration
        flow_name = flow_config['name']
        home_config = flow_config['home_config']
        store_config = flow_config['store_config']
        flow_options = flow_config['flow_options']

        # Create Home and Store instances
        home = ParquetHome(
            name=home_config['name'],
            path=home_config['path'],
            options=home_config['options']
        )

        store = ParquetStore(
            name=store_config['name'],
            path=store_config['path'],
            options=store_config['options']
        )

        # Create Flow
        flow = Flow(flow_name, home, store, flow_options)

        # When running the flow
        await flow.start()

        # Then verify data integrity
        store_path = Path(store_config['path'])
        output_files = list(store_path.glob('*.parquet'))

        # Should have created output files
        assert len(output_files) > 0, "No output files created"

        # Verify total rows match
        total_output_rows = 0
        for file_path in output_files:
            df = pl.read_parquet(file_path)
            total_output_rows += len(df)

        # Read original data for comparison
        original_df = pl.read_parquet(sample_parquet_data)
        original_rows = len(original_df)

        assert total_output_rows == original_rows, f"Row count mismatch: {total_output_rows} vs {original_rows}"

        # Verify data content matches (read first output file)
        first_output_file = min(output_files, key=lambda p: p.name)
        output_df = pl.read_parquet(first_output_file)

        # Check that data structure is preserved
        assert set(output_df.columns) == set(original_df.columns), "Column mismatch"

        # Verify some sample data matches
        assert output_df['id'][0] == original_df['id'][0], "Data content mismatch"
        assert output_df['value'][0] == original_df['value'][0], "Data content mismatch"

    @pytest.mark.asyncio
    async def test_flow_with_different_batch_sizes(self, flow_config: Dict[str, Any]):
        """Test that flow handles different Home and Store batch sizes correctly."""
        # Modify config to have very different batch sizes
        flow_config['home_config']['options']['batch_size'] = 500
        flow_config['store_config']['options']['batch_size'] = 3000

        home = ParquetHome(
            name=flow_config['home_config']['name'],
            path=flow_config['home_config']['path'],
            options=flow_config['home_config']['options']
        )

        store = ParquetStore(
            name=flow_config['store_config']['name'],
            path=flow_config['store_config']['path'],
            options=flow_config['store_config']['options']
        )

        flow = Flow(flow_config['name'], home, store, flow_config['flow_options'])

        # Should complete without errors
        await flow.start()

        # Verify flow completed successfully
        assert flow.total_rows > 0, "No rows processed"
        assert flow.batches_processed > 0, "No batches processed"

    @pytest.mark.asyncio
    async def test_flow_error_handling(self, temp_data_dir: Path):
        """Test flow handles errors gracefully."""
        # Create invalid source file
        invalid_source = temp_data_dir / "invalid.parquet"
        invalid_source.write_text("not a parquet file")

        store_dir = temp_data_dir / "error_store"
        store_dir.mkdir()

        home = ParquetHome(
            name="invalid",
            path=str(invalid_source),
            options={'batch_size': 1000}
        )

        store = ParquetStore(
            name="destination",
            path=str(store_dir),
            options={'batch_size': 1000}
        )

        flow = Flow("error_test", home, store, {'queue_size': 2})

        # Should raise an error
        with pytest.raises(Exception):
            await flow.start()

    @pytest.mark.asyncio
    async def test_flow_performance_metrics(self, flow_config: Dict[str, Any]):
        """Test that flow tracks performance metrics correctly."""
        home = ParquetHome(
            name=flow_config['home_config']['name'],
            path=flow_config['home_config']['path'],
            options=flow_config['home_config']['options']
        )

        store = ParquetStore(
            name=flow_config['store_config']['name'],
            path=flow_config['store_config']['path'],
            options=flow_config['store_config']['options']
        )

        flow = Flow(flow_config['name'], home, store, flow_config['flow_options'])

        await flow.start()

        # Verify metrics are tracked
        assert flow.total_rows > 0, "Total rows should be tracked"
        assert flow.batches_processed > 0, "Batches processed should be tracked"
        assert flow.start_time is not None, "Start time should be tracked"

        # Verify reasonable performance (should complete quickly for test data)
        duration = asyncio.get_event_loop().time() - flow.start_time
        assert duration > 0, "Duration should be positive"
        assert duration < 60, "Should complete within reasonable time"

    @pytest.mark.asyncio
    async def test_multiple_flows_sequential(self, temp_data_dir: Path):
        """Test running multiple flows sequentially."""
        # Create multiple source files
        source1 = temp_data_dir / "source1.parquet"
        source2 = temp_data_dir / "source2.parquet"

        df1 = pl.DataFrame({'id': range(1000), 'value': ['flow1'] * 1000})
        df2 = pl.DataFrame({'id': range(1000), 'value': ['flow2'] * 1000})

        df1.write_parquet(source1)
        df2.write_parquet(source2)

        store_dir = temp_data_dir / "multi_store"
        store_dir.mkdir()

        # Create and run first flow
        home1 = ParquetHome("flow1", str(source1), {'batch_size': 500})
        store1 = ParquetStore("flow1", str(store_dir / "flow1"), {'batch_size': 500})
        flow1 = Flow("flow1", home1, store1, {'queue_size': 2})

        await flow1.start()

        # Create and run second flow
        home2 = ParquetHome("flow2", str(source2), {'batch_size': 500})
        store2 = ParquetStore("flow2", str(store_dir / "flow2"), {'batch_size': 500})
        flow2 = Flow("flow2", home2, store2, {'queue_size': 2})

        await flow2.start()

        # Verify both flows completed
        assert flow1.total_rows == 1000, "First flow should have 1000 rows"
        assert flow2.total_rows == 1000, "Second flow should have 1000 rows"

        # Verify output files exist
        assert len(list((store_dir / "flow1").glob("*.parquet"))) > 0
        assert len(list((store_dir / "flow2").glob("*.parquet"))) > 0
