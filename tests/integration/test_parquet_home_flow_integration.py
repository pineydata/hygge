"""
Integration tests for ParquetHome with Flow system.

Following hygge's testing principles:
- Test end-to-end data movement scenarios with ParquetHome
- Focus on user experience and data integrity
- Verify producer-consumer pattern works correctly
- Test real data movement with actual parquet files
"""
import pytest
import polars as pl
import tempfile
import os
from pathlib import Path

from hygge.core.flow import Flow
from hygge.core.homes.parquet_home import ParquetHome
from hygge.core.homes.configs.parquet_home_config import ParquetHomeConfig
from hygge.core.stores.parquet_store import ParquetStore
from hygge.core.stores.configs.parquet_store_config import ParquetStoreConfig
from hygge.utility.exceptions import FlowError, HomeError


class MockStore:
    """Mock Store for testing Flow integration."""

    def __init__(self, name: str, options=None):
        self.name = name
        self.options = options or {}
        self.written_batches = []
        self.finish_called = False

    async def write(self, df: pl.DataFrame) -> None:
        """Mock write method that stores data."""
        self.written_batches.append(df)

    async def finish(self) -> None:
        """Mock finish method."""
        self.finish_called = True


class TestParquetHomeFlowIntegration:
    """Test ParquetHome integration with Flow system."""

    @pytest.mark.asyncio
    async def test_parquet_home_to_mock_store_flow(self):
        """Test complete flow from ParquetHome to MockStore."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create source parquet file
            source_path = Path(tmp_dir) / "source.parquet"
            expected_df = pl.DataFrame({
                "id": range(100),
                "name": [f"user_{i}" for i in range(100)],
                "value": [i * 2 for i in range(100)]
            })
            expected_df.write_parquet(source_path)

            # Create ParquetHome
            home_config = ParquetHomeConfig(path=str(source_path))
            home = ParquetHome("test_home", home_config)

            # Create MockStore
            store = MockStore("test_store")

            # Create Flow
            flow = Flow("test_flow", home, store)

            # Run the flow
            await flow.start()

            # Verify results
            assert len(store.written_batches) == 1
            assert store.written_batches[0].equals(expected_df)
            assert store.finish_called
            assert flow.total_rows == 100
            assert flow.batches_processed == 1
            assert flow.start_time is not None

    @pytest.mark.asyncio
    async def test_parquet_home_multiple_files_flow(self):
        """Test flow with ParquetHome reading multiple files."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create multiple source parquet files
            expected_dfs = []
            for i in range(3):
                df = pl.DataFrame({
                    "id": range(i*10, (i+1)*10),
                    "batch": [i] * 10,
                    "value": [f"batch_{i}_row_{j}" for j in range(10)]
                })
                df.write_parquet(Path(tmp_dir) / f"batch_{i}.parquet")
                expected_dfs.append(df)

            # Create ParquetHome for directory
            home_config = ParquetHomeConfig(path=tmp_dir)
            home = ParquetHome("test_home", home_config)

            # Create MockStore
            store = MockStore("test_store")

            # Create Flow
            flow = Flow("test_flow", home, store)

            # Run the flow
            await flow.start()

            # Verify results
            assert len(store.written_batches) == 3
            for i, batch in enumerate(store.written_batches):
                assert batch.equals(expected_dfs[i])
            assert store.finish_called
            assert flow.total_rows == 30
            assert flow.batches_processed == 3

    @pytest.mark.asyncio
    async def test_parquet_home_to_parquet_store_flow(self):
        """Test complete flow from ParquetHome to ParquetStore."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create source parquet file
            source_path = Path(tmp_dir) / "source.parquet"
            expected_df = pl.DataFrame({
                "id": range(50),
                "name": [f"user_{i}" for i in range(50)],
                "value": [i * 3 for i in range(50)]
            })
            expected_df.write_parquet(source_path)

            # Create ParquetHome
            home_config = ParquetHomeConfig(path=str(source_path))
            home = ParquetHome("test_home", home_config)

            # Create ParquetStore
            store_path = Path(tmp_dir) / "output"
            store_config = ParquetStoreConfig(path=str(store_path))
            store = ParquetStore("test_store", store_config)

            # Create Flow
            flow = Flow("test_flow", home, store)

            # Run the flow
            await flow.start()

            # Verify results
            assert flow.total_rows == 50
            assert flow.batches_processed == 1

            # Verify output file was created
            output_files = list(store_path.glob("*.parquet"))
            assert len(output_files) == 1

            # Verify output data matches input
            result_df = pl.read_parquet(output_files[0])
            assert result_df.equals(expected_df)

    @pytest.mark.asyncio
    async def test_parquet_home_flow_error_handling(self):
        """Test flow error handling with ParquetHome."""
        # Create ParquetHome with nonexistent path
        home_config = ParquetHomeConfig(path="/nonexistent/path.parquet")
        home = ParquetHome("test_home", home_config)

        # Create MockStore
        store = MockStore("test_store")

        # Create Flow
        flow = Flow("test_flow", home, store)

        # Run the flow - should handle error gracefully
        with pytest.raises(HomeError):
            await flow.start()

        # Verify store was not written to
        assert len(store.written_batches) == 0
        assert not store.finish_called

    @pytest.mark.asyncio
    async def test_parquet_home_flow_with_custom_options(self):
        """Test ParquetHome flow with custom configuration options."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create source parquet file
            source_path = Path(tmp_dir) / "source.parquet"
            df = pl.DataFrame({"id": range(1000), "value": ["test"] * 1000})
            df.write_parquet(source_path)

            # Create ParquetHome with custom options
            home_config = ParquetHomeConfig(
                path=str(source_path),
                options={'batch_size': 5000}
            )
            home = ParquetHome("test_home", home_config)

            # Create MockStore
            store = MockStore("test_store")

            # Create Flow with custom options
            flow_options = {'queue_size': 5, 'timeout': 60}
            flow = Flow("test_flow", home, store, flow_options)

            # Run the flow
            await flow.start()

            # Verify results
            assert flow.total_rows == 1000
            assert len(store.written_batches) == 1
            assert store.written_batches[0].equals(df)
            assert store.finish_called


class TestParquetHomeFlowConcurrency:
    """Test ParquetHome with concurrent flows."""

    @pytest.mark.asyncio
    async def test_multiple_parquet_home_flows_concurrent(self):
        """Test multiple ParquetHome flows running concurrently."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create multiple source files
            source_files = []
            expected_dfs = []
            for i in range(3):
                df = pl.DataFrame({
                    "id": range(i*10, (i+1)*10),
                    "flow": [f"flow_{i}"] * 10,
                    "value": [j * 2 for j in range(i*10, (i+1)*10)]
                })
                source_path = Path(tmp_dir) / f"source_{i}.parquet"
                df.write_parquet(source_path)
                source_files.append(source_path)
                expected_dfs.append(df)

            # Create multiple flows
            flows = []
            stores = []
            for i in range(3):
                home_config = ParquetHomeConfig(path=str(source_files[i]))
                home = ParquetHome(f"home_{i}", home_config)
                store = MockStore(f"store_{i}")
                flow = Flow(f"flow_{i}", home, store)
                flows.append(flow)
                stores.append(store)

            # Run all flows concurrently
            await asyncio.gather(*[flow.start() for flow in flows])

            # Verify all flows completed successfully
            for i, (flow, store) in enumerate(zip(flows, stores)):
                assert flow.total_rows == 10
                assert flow.batches_processed == 1
                assert len(store.written_batches) == 1
                assert store.written_batches[0].equals(expected_dfs[i])
                assert store.finish_called


class TestParquetHomeFlowPerformance:
    """Test ParquetHome flow performance characteristics."""

    @pytest.mark.asyncio
    async def test_parquet_home_flow_performance_tracking(self):
        """Test that ParquetHome flow tracks performance correctly."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create larger dataset for performance testing
            source_path = Path(tmp_dir) / "source.parquet"
            df = pl.DataFrame({
                "id": range(10000),
                "name": [f"user_{i}" for i in range(10000)],
                "value": [i * 2 for i in range(10000)]
            })
            df.write_parquet(source_path)

            # Create ParquetHome
            home_config = ParquetHomeConfig(path=str(source_path))
            home = ParquetHome("test_home", home_config)

            # Create MockStore
            store = MockStore("test_store")

            # Create Flow
            flow = Flow("test_flow", home, store)

            # Run the flow
            await flow.start()

            # Verify performance tracking
            assert flow.total_rows == 10000
            assert flow.batches_processed == 1
            assert flow.start_time is not None

            # Should have some processing time
            duration = asyncio.get_event_loop().time() - flow.start_time
            assert duration > 0
