"""
Tests for dry-run preview functionality.

Tests the --dry-run flag that previews flows without moving data.
"""

import polars as pl
import pytest

from hygge.core.flow import Flow
from hygge.homes.parquet import ParquetHome, ParquetHomeConfig
from hygge.stores.parquet import ParquetStore, ParquetStoreConfig


@pytest.mark.asyncio
async def test_flow_preview_basic(tmp_path):
    """Test basic flow preview with parquet source."""
    # Create test data
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    source_file = source_dir / "test.parquet"

    test_data = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [100, 200, 300],
        }
    )
    test_data.write_parquet(source_file)

    # Create flow with proper config objects
    home_config = ParquetHomeConfig(path=str(source_file))
    home = ParquetHome(name="test_home", config=home_config)

    store_config = ParquetStoreConfig(path=str(tmp_path / "destination"))
    store = ParquetStore(name="test_store", config=store_config)

    flow = Flow(
        name="test_flow",
        home=home,
        store=store,
        entity_name="test_entity",
        base_flow_name="test",
    )

    # Preview
    preview_info = await flow.preview()

    # Verify preview structure (no connection needed!)
    assert preview_info["flow_name"] == "test_flow"
    assert preview_info["status"] == "ready"
    assert "home_info" in preview_info
    assert "store_info" in preview_info

    # Verify home/store info captured from config
    assert preview_info["home_info"]["type"] == "parquet"
    assert str(source_file) in str(preview_info["home_info"].get("path", ""))
    assert preview_info["store_info"]["type"] == "parquet"


@pytest.mark.asyncio
async def test_flow_preview_no_connection_test(tmp_path):
    """Test that flow preview doesn't test connections."""
    # Don't create the source file - preview should still work!
    source_file = tmp_path / "missing.parquet"

    home_config = ParquetHomeConfig(path=str(source_file))
    home = ParquetHome(name="test_home", config=home_config)

    store_config = ParquetStoreConfig(path=str(tmp_path / "destination"))
    store = ParquetStore(name="test_store", config=store_config)

    flow = Flow(
        name="test_flow",
        home=home,
        store=store,
        entity_name="test_entity",
        base_flow_name="test",
    )

    # Preview should work even with missing file (doesn't connect!)
    preview_info = await flow.preview()

    # No errors - we don't test connections in dry-run
    assert preview_info["status"] == "ready"
    assert "home_info" in preview_info
    assert "store_info" in preview_info


@pytest.mark.asyncio
async def test_flow_preview_with_watermark(tmp_path):
    """Test flow preview with incremental/watermark configuration."""
    # Create test data
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    source_file = source_dir / "test.parquet"

    test_data = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "updated_at": ["2024-01-01", "2024-01-02", "2024-01-03"],
        }
    )
    test_data.write_parquet(source_file)

    # Create flow with watermark
    home_config = ParquetHomeConfig(path=str(source_file))
    home = ParquetHome(name="test_home", config=home_config)

    store_config = ParquetStoreConfig(path=str(tmp_path / "destination"))
    store = ParquetStore(name="test_store", config=store_config)

    flow = Flow(
        name="test_flow",
        home=home,
        store=store,
        entity_name="test_entity",
        base_flow_name="test",
        run_type="incremental",
        watermark_config={"column": "updated_at"},
    )

    # Preview
    preview_info = await flow.preview()

    # Verify incremental info captured
    assert preview_info["incremental_info"]["enabled"] is True
    assert preview_info["incremental_info"]["watermark_column"] == "updated_at"
    assert preview_info["incremental_info"]["run_type"] == "incremental"


@pytest.mark.asyncio
async def test_coordinator_preview_simple(tmp_path):
    """Test coordinator preview with simple flow setup."""
    # Create test data
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    source_file = source_dir / "users.parquet"

    test_data = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )
    test_data.write_parquet(source_file)

    # Create flow manually and test preview through coordinator pattern
    home_config = ParquetHomeConfig(path=str(source_file))
    home = ParquetHome(name="test_home", config=home_config)

    store_config = ParquetStoreConfig(path=str(tmp_path / "destination"))
    store = ParquetStore(name="test_store", config=store_config)

    flow = Flow(
        name="test_flow",
        home=home,
        store=store,
        entity_name="test_entity",
        base_flow_name="test",
    )

    # Test preview on the flow
    preview_info = await flow.preview()

    # Verify coordinator would be able to work with this preview info
    assert preview_info["flow_name"] == "test_flow"
    assert "home_info" in preview_info
    assert "store_info" in preview_info

    # This tests that the preview infrastructure would work with coordinator
    # Full coordinator integration testing is done in integration tests


def test_preview_format_concise():
    """Test concise preview formatting."""
    import sys
    from io import StringIO

    from hygge.cli import _print_concise_flow

    preview_result = {
        "flow_name": "test_flow",
        "home_info": {"type": "parquet"},
        "store_info": {"type": "parquet"},
        "incremental_info": {"enabled": True},
        "warnings": [],
    }

    # Capture output
    captured = StringIO()
    sys.stdout = captured
    _print_concise_flow(preview_result)
    sys.stdout = sys.__stdout__

    output = captured.getvalue()

    # Verify output contains key elements
    assert "test_flow" in output
    assert "parquet" in output
    assert "→" in output
    assert "✓" in output or "⚠️" in output


def test_preview_format_verbose():
    """Test verbose preview formatting."""
    import sys
    from io import StringIO

    from hygge.cli import _print_verbose_flow

    preview_result = {
        "flow_name": "test_flow",
        "entity_name": "users",
        "base_flow_name": "test",
        "home_info": {
            "type": "parquet",
            "path": "/path/to/source.parquet",
        },
        "store_info": {
            "type": "parquet",
            "path": "/path/to/destination",
        },
        "incremental_info": {
            "enabled": True,
            "watermark_column": "updated_at",
        },
        "warnings": [],
    }

    # Capture output
    captured = StringIO()
    sys.stdout = captured
    _print_verbose_flow(preview_result)
    sys.stdout = sys.__stdout__

    output = captured.getvalue()

    # Verify detailed output
    assert "test.users" in output or "test_flow" in output
    assert "📥 Source" in output
    assert "📤 Destination" in output
    assert "💧 Incremental Mode" in output
    assert "/path/to/source.parquet" in output
    assert "/path/to/destination" in output
    assert "✓ Ready to preview" in output
