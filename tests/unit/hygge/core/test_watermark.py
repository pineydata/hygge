"""
Tests for Watermark class.

Following hygge's testing principles:
- Test behavior that matters to users
- Focus on data integrity and reliability
- Keep tests clear and maintainable
- Test isolated watermark logic
"""

from datetime import datetime, timezone

import polars as pl
import pytest

from hygge.core.watermark import Watermark
from hygge.messages import get_logger
from hygge.utility.exceptions import ConfigError


@pytest.fixture
def logger():
    """Logger fixture for Watermark tests."""
    return get_logger("test.watermark")


class TestWatermarkInitialization:
    """Test Watermark initialization."""

    def test_init_with_config(self, logger):
        """Test Watermark initializes with config."""
        config = {"primary_key": "user_id", "watermark_column": "updated_at"}
        watermark = Watermark(config, logger)
        assert watermark.primary_key == "user_id"
        assert watermark.watermark_column == "updated_at"
        assert watermark.get_watermark_value() is None
        assert watermark.get_watermark_type() is None

    def test_init_without_primary_key(self, logger):
        """Test Watermark initializes without primary_key."""
        config = {"watermark_column": "updated_at"}
        watermark = Watermark(config, logger)
        assert watermark.primary_key is None
        assert watermark.watermark_column == "updated_at"


class TestWatermarkSchemaValidation:
    """Test watermark schema validation."""

    def test_validate_schema_success(self, logger):
        """Test validate_schema() succeeds with valid schema."""
        config = {"primary_key": "user_id", "watermark_column": "updated_at"}
        watermark = Watermark(config, logger)

        # Create DataFrame to get proper schema with fully-specified types
        df = pl.DataFrame(
            {
                "user_id": [1, 2],
                "updated_at": [
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    datetime(2024, 1, 2, tzinfo=timezone.utc),
                ],
                "name": ["a", "b"],
            }
        )
        schema = df.schema
        # Should not raise
        watermark.validate_schema(schema)

    def test_validate_schema_missing_primary_key(self, logger):
        """Test validate_schema() fails when primary key missing."""
        config = {"primary_key": "user_id", "watermark_column": "updated_at"}
        watermark = Watermark(config, logger)

        # Create DataFrame to get proper schema
        df = pl.DataFrame(
            {
                "updated_at": [
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                ],
                "name": ["a"],
            }
        )
        schema = df.schema

        with pytest.raises(ConfigError, match="Primary key 'user_id' not found"):
            watermark.validate_schema(schema)

    def test_validate_schema_missing_watermark_column(self, logger):
        """Test validate_schema() fails when watermark column missing."""
        config = {"primary_key": "user_id", "watermark_column": "updated_at"}
        watermark = Watermark(config, logger)

        # Create DataFrame to get proper schema
        df = pl.DataFrame({"user_id": [1, 2], "name": ["a", "b"]})
        schema = df.schema

        with pytest.raises(
            ConfigError, match="Watermark column 'updated_at' not found"
        ):
            watermark.validate_schema(schema)

    def test_validate_schema_unsupported_type(self, logger):
        """Test validate_schema() fails with unsupported type."""
        config = {"primary_key": "user_id", "watermark_column": "data"}
        watermark = Watermark(config, logger)

        schema = pl.Schema({"user_id": pl.Int64, "data": pl.Binary})

        with pytest.raises(ConfigError, match="Unsupported watermark type"):
            watermark.validate_schema(schema)

    def test_validate_schema_datetime_supported(self, logger):
        """Test validate_schema() accepts datetime type."""
        config = {"watermark_column": "updated_at"}
        watermark = Watermark(config, logger)

        # Create DataFrame to get proper schema with fully-specified datetime type
        df = pl.DataFrame(
            {
                "updated_at": [
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                ]
            }
        )
        schema = df.schema
        # Should not raise
        watermark.validate_schema(schema)

    def test_validate_schema_integer_types_supported(self, logger):
        """Test validate_schema() accepts all integer types."""
        config = {"watermark_column": "id"}
        watermark = Watermark(config, logger)

        integer_types = [
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
        ]

        for dtype in integer_types:
            schema = pl.Schema({"id": dtype})
            # Should not raise
            watermark.validate_schema(schema)

    def test_validate_schema_string_supported(self, logger):
        """Test validate_schema() accepts string type."""
        config = {"watermark_column": "code"}
        watermark = Watermark(config, logger)

        schema = pl.Schema({"code": pl.Utf8})
        # Should not raise
        watermark.validate_schema(schema)


class TestWatermarkUpdate:
    """Test watermark value tracking."""

    def test_update_datetime(self, logger):
        """Test update() tracks max datetime value."""
        config = {"watermark_column": "updated_at"}
        watermark = Watermark(config, logger)

        batch = pl.DataFrame(
            {
                "updated_at": [
                    datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
                    datetime(2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
                    datetime(2025, 1, 1, 15, 0, 0, tzinfo=timezone.utc),
                ]
            }
        )

        watermark.update(batch)
        assert watermark.get_watermark_type() == "datetime"
        assert watermark.get_watermark_value() == datetime(
            2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc
        )

    def test_update_integer(self, logger):
        """Test update() tracks max integer value."""
        config = {"watermark_column": "id"}
        watermark = Watermark(config, logger)

        batch = pl.DataFrame({"id": [1, 5, 3, 10, 2]})

        watermark.update(batch)
        assert watermark.get_watermark_type() == "int"
        assert watermark.get_watermark_value() == 10

    def test_update_string(self, logger):
        """Test update() tracks max string value."""
        config = {"watermark_column": "code"}
        watermark = Watermark(config, logger)

        batch = pl.DataFrame({"code": ["A", "C", "B", "Z"]})

        watermark.update(batch)
        assert watermark.get_watermark_type() == "string"
        assert watermark.get_watermark_value() == "Z"

    def test_update_multiple_batches(self, logger):
        """Test update() tracks max across multiple batches."""
        config = {"watermark_column": "updated_at"}
        watermark = Watermark(config, logger)

        batch1 = pl.DataFrame(
            {
                "updated_at": [
                    datetime(2025, 1, 1, 10, 0, 0, tzinfo=timezone.utc),
                    datetime(2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc),
                ]
            }
        )
        batch2 = pl.DataFrame(
            {
                "updated_at": [
                    datetime(2025, 1, 3, 10, 0, 0, tzinfo=timezone.utc),
                    datetime(2025, 1, 1, 15, 0, 0, tzinfo=timezone.utc),
                ]
            }
        )

        watermark.update(batch1)
        watermark.update(batch2)

        assert watermark.get_watermark_value() == datetime(
            2025, 1, 3, 10, 0, 0, tzinfo=timezone.utc
        )

    def test_update_handles_all_nulls(self, logger):
        """Test update() handles batches with all null values."""
        config = {"watermark_column": "updated_at"}
        watermark = Watermark(config, logger)

        batch = pl.DataFrame({"updated_at": [None, None, None]})

        watermark.update(batch)
        # Should not set watermark value
        assert watermark.get_watermark_value() is None

    def test_update_raises_on_missing_column_after_validation(self, logger):
        """Test update() raises ConfigError if column missing after validation."""
        config = {"watermark_column": "updated_at"}
        watermark = Watermark(config, logger)

        # Simulate validation passed (schema had the column)
        # But batch doesn't have it (schema mismatch)
        batch = pl.DataFrame({"id": [1, 2, 3]})

        # Should raise ConfigError - this indicates a serious schema mismatch
        with pytest.raises(
            ConfigError, match="Watermark column 'updated_at' not found"
        ):
            watermark.update(batch)

    def test_update_type_consistency_warning(self, logger):
        """Test update() warns on type inconsistency."""
        config = {"watermark_column": "value"}
        watermark = Watermark(config, logger)

        batch1 = pl.DataFrame({"value": [1, 2, 3]})
        batch2 = pl.DataFrame({"value": ["a", "b", "c"]})

        watermark.update(batch1)
        watermark.update(batch2)

        # Should keep first type
        assert watermark.get_watermark_type() == "int"
        assert watermark.get_watermark_value() == 3

    def test_update_warns_on_unsupported_type(self, logger):
        """Test update() warns on unsupported types (should be caught by validation)."""
        config = {"watermark_column": "data"}
        watermark = Watermark(config, logger)

        batch = pl.DataFrame({"data": [b"binary", b"data"]})

        # Should log warning but not raise (validation is authoritative)
        watermark.update(batch)
        # Should not set watermark value
        assert watermark.get_watermark_value() is None
        # Warning should be logged (we can't easily test this without mocking logger)


class TestWatermarkSerialization:
    """Test watermark serialization for journal storage."""

    def test_serialize_datetime(self, logger):
        """Test serialize_watermark() formats datetime as ISO string."""
        config = {"watermark_column": "updated_at"}
        watermark = Watermark(config, logger)

        batch = pl.DataFrame(
            {"updated_at": [datetime(2025, 1, 2, 10, 30, 45, tzinfo=timezone.utc)]}
        )

        watermark.update(batch)
        serialized = watermark.serialize_watermark()

        assert serialized == "2025-01-02T10:30:45+00:00"

    def test_serialize_integer(self, logger):
        """Test serialize_watermark() formats integer as string."""
        config = {"watermark_column": "id"}
        watermark = Watermark(config, logger)

        batch = pl.DataFrame({"id": [12345]})

        watermark.update(batch)
        serialized = watermark.serialize_watermark()

        assert serialized == "12345"

    def test_serialize_string(self, logger):
        """Test serialize_watermark() returns string as-is."""
        config = {"watermark_column": "code"}
        watermark = Watermark(config, logger)

        batch = pl.DataFrame({"code": ["ABC123"]})

        watermark.update(batch)
        serialized = watermark.serialize_watermark()

        assert serialized == "ABC123"

    def test_serialize_none_when_no_watermark(self, logger):
        """Test serialize_watermark() returns None when no watermark set."""
        config = {"watermark_column": "updated_at"}
        watermark = Watermark(config, logger)

        serialized = watermark.serialize_watermark()
        assert serialized is None


class TestWatermarkReset:
    """Test watermark reset functionality."""

    def test_reset_clears_watermark(self, logger):
        """Test reset() clears watermark value and type."""
        config = {"watermark_column": "updated_at"}
        watermark = Watermark(config, logger)

        batch = pl.DataFrame(
            {"updated_at": [datetime(2025, 1, 2, 10, 0, 0, tzinfo=timezone.utc)]}
        )

        watermark.update(batch)
        assert watermark.get_watermark_value() is not None

        watermark.reset()
        assert watermark.get_watermark_value() is None
        assert watermark.get_watermark_type() is None
