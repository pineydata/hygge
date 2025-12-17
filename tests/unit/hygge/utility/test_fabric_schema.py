"""
Tests for Fabric schema helpers that map Polars dtypes to Fabric schema
entries. These helpers are used by Open Mirroring and are intended to be
reusable for other Fabric destinations.
"""

from __future__ import annotations

import polars as pl

from hygge.utility.fabric_schema import (
    build_fabric_schema_columns,
    map_polars_dtype_to_fabric,
)


class TestMapPolarsDtypeToFabric:
    """Unit tests for basic dtype → Fabric type mapping."""

    def test_string_and_utf8_map_to_string(self):
        assert map_polars_dtype_to_fabric(pl.Utf8) == "string"
        assert map_polars_dtype_to_fabric(pl.String) == "string"

    def test_integers_map_to_long(self):
        for dtype in (
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
        ):
            assert map_polars_dtype_to_fabric(dtype) == "long"

    def test_floats_map_to_double(self):
        assert map_polars_dtype_to_fabric(pl.Float32) == "double"
        assert map_polars_dtype_to_fabric(pl.Float64) == "double"

    def test_temporal_types_map_to_datetime(self):
        assert map_polars_dtype_to_fabric(pl.Datetime) == "datetime"
        assert map_polars_dtype_to_fabric(pl.Date) == "datetime"
        assert map_polars_dtype_to_fabric(pl.Time) == "datetime"

    def test_decimal_maps_to_decimal(self):
        decimal_type = pl.Decimal(precision=10, scale=2)
        assert map_polars_dtype_to_fabric(decimal_type) == "decimal"

    def test_boolean_maps_to_boolean(self):
        assert map_polars_dtype_to_fabric(pl.Boolean) == "boolean"

    def test_unknown_and_null_types_fall_back_to_string(self):
        assert map_polars_dtype_to_fabric(pl.Null) == "string"
        # Object/mixed types should also be treated as string
        assert map_polars_dtype_to_fabric(pl.Object) == "string"


class TestBuildFabricSchemaColumns:
    """Behavioural tests for schema column construction."""

    def test_build_columns_from_mapping(self):
        schema = {
            "id": pl.Int64,
            "name": pl.Utf8,
            "created_at": pl.Datetime,
        }

        columns = build_fabric_schema_columns(schema)
        by_name = {col["name"]: col for col in columns}

        assert by_name["id"]["type"] == "long"
        assert by_name["name"]["type"] == "string"
        assert by_name["created_at"]["type"] == "datetime"
        # All columns are conservatively nullable
        assert all(col["nullable"] is True for col in columns)

    def test_build_columns_from_iterable(self):
        schema_items = [("flag", pl.Boolean), ("amount", pl.Float64)]

        columns = build_fabric_schema_columns(schema_items)
        by_name = {col["name"]: col for col in columns}

        assert by_name["flag"]["type"] == "boolean"
        assert by_name["amount"]["type"] == "double"

    def test_decimal_columns_include_precision_and_scale_when_available(self):
        decimal_type = pl.Decimal(precision=18, scale=4)
        schema = {"price": decimal_type}

        columns = build_fabric_schema_columns(schema)
        assert len(columns) == 1
        col = columns[0]

        assert col["name"] == "price"
        assert col["type"] == "decimal"
        # Precision/scale should be surfaced when Polars exposes them
        assert col.get("precision") == 18
        assert col.get("scale") == 4

    def test_null_and_mixed_type_columns_are_nullable_strings(self):
        schema = {
            "all_nulls": pl.Null,
            "mixed": pl.Object,
        }

        columns = build_fabric_schema_columns(schema)
        by_name = {col["name"]: col for col in columns}

        assert by_name["all_nulls"]["type"] == "string"
        assert by_name["all_nulls"]["nullable"] is True
        assert by_name["mixed"]["type"] == "string"
        assert by_name["mixed"]["nullable"] is True
