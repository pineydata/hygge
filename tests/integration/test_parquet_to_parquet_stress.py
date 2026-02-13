"""
Stress test for parquet-to-parquet data movement at midmarket scale.

This test validates framework reliability at production scale:
1. Large data volume (100M+ rows)
2. Concurrent flows (10+ flows running simultaneously)
3. Memory efficiency under load
4. Data integrity at scale
5. Performance metrics and throughput

Following hygge's testing philosophy:
- Focus on behavior that matters to users
- Test real-world scenarios
- Verify data integrity and reliability

Run with: pytest tests/integration/test_parquet_to_parquet_stress.py -v -s
"""

import asyncio
import shutil
import tempfile
from pathlib import Path

import polars as pl
import pytest

from hygge import Flow
from hygge.homes import ParquetHome, ParquetHomeConfig
from hygge.stores import ParquetStore, ParquetStoreConfig


@pytest.fixture
def temp_data_dir():
    """Create temporary directory for test data."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


def _create_large_parquet_file(
    temp_data_dir: Path, num_rows: int, filename: str = "large_source.parquet"
) -> Path:
    """
    Helper to create large parquet file with test data.

    Creates data in chunks to avoid memory issues.
    """
    # Create test data in chunks to avoid memory issues
    chunk_size = 1_000_000  # 1M rows per chunk
    chunks = []

    for chunk_start in range(0, num_rows, chunk_size):
        chunk_end = min(chunk_start + chunk_size, num_rows)
        chunk_df = pl.DataFrame(
            {
                "id": range(chunk_start, chunk_end),
                "value": [f"test_{i}" for i in range(chunk_start, chunk_end)],
                "number": [i * 6 for i in range(chunk_start, chunk_end)],
                "category": [f"cat_{i % 5}" for i in range(chunk_start, chunk_end)],
                "score": [i * 0.1 for i in range(chunk_start, chunk_end)],
                "timestamp": [
                    f"2024-01-01T{i % 86400:05d}" for i in range(chunk_start, chunk_end)
                ],
            }
        )
        chunks.append(chunk_df)

    # Combine and write
    source_file = temp_data_dir / filename
    combined_df = pl.concat(chunks)
    combined_df.write_parquet(source_file)

    return source_file


@pytest.mark.asyncio
async def test_large_volume_parquet_to_parquet(temp_data_dir: Path):
    """
    Test large volume data movement (10M rows).

    Validates:
    - Framework handles large datasets without memory issues
    - Data integrity preserved across large volumes
    - Performance metrics tracked correctly
    - Batching works correctly at scale
    """
    # Create 10M row dataset
    num_rows = 10_000_000
    large_parquet_data = _create_large_parquet_file(temp_data_dir, num_rows)

    store_dir = temp_data_dir / "large_store"
    store_dir.mkdir()

    # Configure for large volume
    home_config = ParquetHomeConfig(
        path=str(large_parquet_data),
        batch_size=100_000,  # Larger batches for efficiency
        options={},
    )
    home = ParquetHome("large_source", home_config)

    store_config = ParquetStoreConfig(
        path=str(store_dir),
        batch_size=200_000,  # Accumulate larger batches before writing
        file_pattern="large_{sequence:020d}.parquet",
        compression="snappy",
        options={},
    )
    store = ParquetStore("large_destination", store_config)

    flow = Flow(
        "large_volume_flow",
        home,
        store,
        {"queue_size": 5},  # Larger queue for throughput
        entity_name="large_volume",
        base_flow_name="large_volume",
    )

    # Run the flow
    start_time = asyncio.get_event_loop().time()
    await flow.start()
    duration = asyncio.get_event_loop().time() - start_time

    # Verify data integrity
    output_files = list(store_dir.glob("*.parquet"))
    assert len(output_files) > 0, "No output files created"

    # Count total rows from output
    total_output_rows = 0
    for file_path in output_files:
        df = pl.read_parquet(file_path)
        total_output_rows += len(df)

    # Count rows from source
    source_df = pl.read_parquet(large_parquet_data)
    source_rows = len(source_df)

    assert (
        total_output_rows == source_rows
    ), f"Row count mismatch: {total_output_rows:,} vs {source_rows:,}"

    # Verify flow metrics
    assert flow.total_rows == source_rows, "Flow total_rows should match source"
    assert flow.batches_processed > 0, "Should have processed batches"

    # Calculate throughput
    rows_per_sec = source_rows / duration if duration > 0 else 0
    print(
        f"\nLarge volume test: {source_rows:,} rows in {duration:.2f}s "
        f"({rows_per_sec:,.0f} rows/s)"
    )

    # Verify reasonable performance (should complete within reasonable time)
    # 100M rows should complete in under 10 minutes on modern hardware
    assert duration < 600, f"Should complete within 10 minutes, took {duration:.2f}s"


@pytest.mark.asyncio
async def test_concurrent_flows_stress(temp_data_dir: Path):
    """
    Test concurrent flows (10+ flows running simultaneously).

    Validates:
    - Coordinator handles concurrent execution correctly
    - No data corruption with parallel flows
    - Memory usage stays reasonable
    - All flows complete successfully
    """
    num_flows = 10
    rows_per_flow = 1_000_000  # 1M rows per flow = 10M total

    # Create source files for each flow
    source_files = []
    store_dirs = []

    for i in range(num_flows):
        # Create source data
        source_file = temp_data_dir / f"source_{i}.parquet"
        df = pl.DataFrame(
            {
                "id": range(i * rows_per_flow, (i + 1) * rows_per_flow),
                "flow_id": [i] * rows_per_flow,
                "value": [f"flow_{i}_row_{j}" for j in range(rows_per_flow)],
                "number": [j * 7 for j in range(rows_per_flow)],
            }
        )
        df.write_parquet(source_file)
        source_files.append(source_file)

        # Create store directory
        store_dir = temp_data_dir / f"store_{i}"
        store_dir.mkdir()
        store_dirs.append(store_dir)

    # Create flows
    flows = []
    for i in range(num_flows):
        home_config = ParquetHomeConfig(
            path=str(source_files[i]),
            batch_size=50_000,
            options={},
        )
        home = ParquetHome(f"source_{i}", home_config)

        store_config = ParquetStoreConfig(
            path=str(store_dirs[i]),
            batch_size=100_000,
            file_pattern=f"flow_{i}_{{sequence:020d}}.parquet",
            compression="snappy",
            options={},
        )
        store = ParquetStore(f"store_{i}", store_config)

        flow = Flow(
            f"concurrent_flow_{i}",
            home,
            store,
            {"queue_size": 3},
            entity_name=f"flow_{i}",
            base_flow_name=f"flow_{i}",
        )
        flows.append(flow)

    # Run all flows concurrently
    start_time = asyncio.get_event_loop().time()
    await asyncio.gather(*[flow.start() for flow in flows])
    duration = asyncio.get_event_loop().time() - start_time

    # Verify all flows completed successfully
    for i, flow in enumerate(flows):
        assert (
            flow.total_rows == rows_per_flow
        ), f"Flow {i} should have {rows_per_flow:,} rows"
        assert flow.batches_processed > 0, f"Flow {i} should have processed batches"

        # Verify output files exist
        output_files = list(store_dirs[i].glob("*.parquet"))
        assert len(output_files) > 0, f"Flow {i} should have created output files"

        # Verify row count
        total_rows = sum(len(pl.read_parquet(f)) for f in output_files)
        assert (
            total_rows == rows_per_flow
        ), f"Flow {i} output should have {rows_per_flow:,} rows"

    total_rows = sum(flow.total_rows for flow in flows)
    rows_per_sec = total_rows / duration if duration > 0 else 0
    print(
        f"\nConcurrent flows test: {num_flows} flows, {total_rows:,} total rows "
        f"in {duration:.2f}s ({rows_per_sec:,.0f} rows/s)"
    )

    # Should complete within reasonable time (10 flows × 1M rows each)
    assert duration < 300, f"Should complete within 5 minutes, took {duration:.2f}s"


@pytest.mark.slow
@pytest.mark.asyncio
async def test_extreme_volume_parquet_to_parquet(temp_data_dir: Path):
    """
    Test extreme volume (100M+ rows) to validate midmarket scale limits.

    This test can be skipped in normal test runs due to time/memory requirements.
    Run explicitly when testing production scale scenarios.
    """
    # Create 100M row dataset
    num_rows = 100_000_000

    # Create in smaller chunks to manage memory
    chunk_size = 5_000_000  # 5M rows per chunk
    chunks = []

    print(f"\nCreating {num_rows:,} row dataset...")
    for chunk_start in range(0, num_rows, chunk_size):
        chunk_end = min(chunk_start + chunk_size, num_rows)
        chunk_df = pl.DataFrame(
            {
                "id": range(chunk_start, chunk_end),
                "value": [f"row_{i}" for i in range(chunk_start, chunk_end)],
                "number": [i * 3 for i in range(chunk_start, chunk_end)],
                "category": [f"cat_{i % 10}" for i in range(chunk_start, chunk_end)],
            }
        )
        chunks.append(chunk_df)

        if len(chunks) % 5 == 0:
            print(f"  Created {chunk_end:,} rows...")

    # Write to parquet
    source_file = temp_data_dir / "extreme_source.parquet"
    print(f"Writing {num_rows:,} rows to parquet...")
    combined_df = pl.concat(chunks)
    combined_df.write_parquet(source_file)
    print("Source file created.")

    # Configure flow
    store_dir = temp_data_dir / "extreme_store"
    store_dir.mkdir()

    home_config = ParquetHomeConfig(
        path=str(source_file),
        batch_size=500_000,  # Large batches for efficiency
        options={},
    )
    home = ParquetHome("extreme_source", home_config)

    store_config = ParquetStoreConfig(
        path=str(store_dir),
        batch_size=1_000_000,  # Very large batches
        file_pattern="extreme_{sequence:020d}.parquet",
        compression="snappy",
        options={},
    )
    store = ParquetStore("extreme_destination", store_config)

    flow = Flow(
        "extreme_volume_flow",
        home,
        store,
        {"queue_size": 10},
        entity_name="extreme_volume",
        base_flow_name="extreme_volume",
    )

    # Run the flow
    print("Starting flow...")
    start_time = asyncio.get_event_loop().time()
    await flow.start()
    duration = asyncio.get_event_loop().time() - start_time

    # Verify
    output_files = list(store_dir.glob("*.parquet"))
    assert len(output_files) > 0, "No output files created"

    # Verify flow metrics
    assert (
        flow.total_rows == num_rows
    ), f"Flow should have {num_rows:,} rows, got {flow.total_rows:,}"

    rows_per_sec = num_rows / duration if duration > 0 else 0
    print(
        f"\nExtreme volume test: {num_rows:,} rows in {duration:.2f}s "
        f"({rows_per_sec:,.0f} rows/s)"
    )
    print(f"Created {len(output_files)} output files")

    # Should complete within reasonable time (100M rows in under 30 minutes)
    assert duration < 1800, f"Should complete within 30 minutes, took {duration:.2f}s"


@pytest.mark.asyncio
async def test_memory_efficiency_large_volume(temp_data_dir: Path):
    """
    Test memory efficiency with large volumes.

    Validates that framework doesn't accumulate excessive memory
    during large data movement operations.
    """
    try:
        import os

        import psutil

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        track_memory = True
    except ImportError:
        # psutil not available, skip memory tracking
        track_memory = False
        initial_memory = 0

    # Create 50M row dataset
    num_rows = 50_000_000
    source_file = temp_data_dir / "memory_test_source.parquet"

    # Create in chunks
    chunk_size = 2_000_000
    chunks = []
    for chunk_start in range(0, num_rows, chunk_size):
        chunk_end = min(chunk_start + chunk_size, num_rows)
        chunk_df = pl.DataFrame(
            {
                "id": range(chunk_start, chunk_end),
                "value": [f"mem_test_{i}" for i in range(chunk_start, chunk_end)],
            }
        )
        chunks.append(chunk_df)

    combined_df = pl.concat(chunks)
    combined_df.write_parquet(source_file)

    # Configure flow
    store_dir = temp_data_dir / "memory_store"
    store_dir.mkdir()

    home_config = ParquetHomeConfig(
        path=str(source_file),
        batch_size=200_000,
        options={},
    )
    home = ParquetHome("memory_source", home_config)

    store_config = ParquetStoreConfig(
        path=str(store_dir),
        batch_size=400_000,
        options={},
    )
    store = ParquetStore("memory_destination", store_config)

    flow = Flow(
        "memory_test_flow",
        home,
        store,
        {"queue_size": 3},  # Smaller queue to limit memory
        entity_name="memory_test",
        base_flow_name="memory_test",
    )

    # Run flow
    await flow.start()

    # Check memory usage if psutil available
    if track_memory:
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory

        print(
            f"\nMemory test: Initial {initial_memory:.1f}MB, "
            f"Final {final_memory:.1f}MB, Increase {memory_increase:.1f}MB"
        )

        # Memory increase should be reasonable (less than 2GB for 50M rows)
        # This is a sanity check - actual limits depend on system
        assert (
            memory_increase < 2048
        ), f"Memory increase too high: {memory_increase:.1f}MB"
    else:
        print("\nMemory test: psutil not available, skipping memory tracking")

    # Verify data integrity
    output_files = list(store_dir.glob("*.parquet"))
    total_rows = sum(len(pl.read_parquet(f)) for f in output_files)
    assert total_rows == num_rows, f"Should have {num_rows:,} rows, got {total_rows:,}"
