"""
Helpers for generating Fabric-compatible schema manifests from Polars dtypes.

This module centralizes the logic for mapping Polars dtypes to the lightweight
schema representation used by hygge when publishing `_schema.json` files for
Fabric **Parquet-backed** destinations (for example, Open Mirroring journal
mirrors and other OneLake landing zones).

It is intentionally **not** a Warehouse DDL helper: Fabric Warehouse tables
use the `@Warehouse` T-SQL datatype surface (e.g. `BIGINT`, `NVARCHAR`, and
`DECIMAL(p,s)`), which should be modelled separately when Warehouse support
is added.

Design goals:
- Single, well-documented place to understand how Polars dtypes map to Fabric
  mirroring schema types.
- Safe defaults that preserve existing behaviour (no breaking changes).
- Better handling for additional types like decimals and booleans.
- Explicit nullability handling so Fabric doesn't silently drop NULLs.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Mapping

import polars as pl

logger = logging.getLogger(__name__)


def map_polars_dtype_to_fabric(dtype: pl.DataType) -> str:
    """
    Map a Polars dtype to a Fabric-compatible logical type string.

    This intentionally mirrors the existing behaviour in
    `OpenMirroringStore._map_polars_dtype_to_fabric` while extending support
    for additional types (decimals, booleans). Existing journal schemas only
    use Utf8/Int64/Float64/Datetime, so extending the mapping for new dtypes
    is backward compatible.

    The mapping is deliberately conservative:
    - All string-like types → ``"string"``
    - All integer types → ``"long"``
    - All float types → ``"double"``
    - Date/Datetime/Time → ``"datetime"``
    - Decimal → ``"decimal"`` (precision/scale are handled by callers if needed)
    - Boolean → ``"boolean"``
    - Everything else → ``"string"`` as a safe fallback
    """
    if dtype in {pl.Utf8, pl.String}:
        return "string"

    integer_types = {
        pl.Int8,
        pl.Int16,
        pl.Int32,
        pl.Int64,
        pl.UInt8,
        pl.UInt16,
        pl.UInt32,
        pl.UInt64,
    }
    if dtype in integer_types:
        return "long"

    if dtype in {pl.Float32, pl.Float64}:
        return "double"

    if dtype in {pl.Datetime, pl.Date, pl.Time}:
        return "datetime"

    # Decimals: preserve intent instead of stringifying.
    # Precision/scale are not encoded at this level to keep the manifest
    # lightweight and compatible; callers can attach them if/when needed.
    if isinstance(dtype, pl.Decimal):
        return "decimal"

    # Booleans should be represented explicitly so Fabric doesn't interpret
    # them as strings and lose semantics.
    if dtype == pl.Boolean:
        return "boolean"

    # Binary-like and object/mixed types fall back to string, which mirrors
    # previous behaviour and is the safest cross-engine representation.
    # This also covers pl.Null and pl.Object, which frequently show up in
    # "all null" or mixed-type columns.
    logger.debug(
        f"Unknown Polars dtype {dtype}, falling back to 'string' for Fabric schema"
    )
    return "string"


def build_fabric_schema_columns(
    schema: Mapping[str, pl.DataType] | Iterable[tuple[str, pl.DataType]],
) -> List[Dict[str, Any]]:
    """
    Build a list of column descriptors for a Fabric-style schema manifest.

    Parameters
    ----------
    schema:
        Either a Polars schema mapping (e.g. ``DataFrame.schema`` or
        ``Journal.JOURNAL_SCHEMA``) or any iterable of ``(name, dtype)`` pairs.

    Returns
    -------
    list of dict
        A list of column descriptors like::

            [
                {"name": "entity_run_id", "type": "string", "nullable": True},
                {"name": "row_count", "type": "long", "nullable": True},
            ]

    Notes
    -----
    - We conservatively mark columns as ``nullable=True``. Fabric is much
      happier accepting NULLs in a column declared nullable than rejecting
      writes because a column was declared non-nullable and later received
      NULLs. This matches hygge's "comfort over surprises" philosophy.
    - Edge cases like all-null columns (``pl.Null``) or mixed/unknown dtypes
      still produce a valid entry with ``type="string"`` and
      ``nullable=True``.
    """
    if isinstance(schema, Mapping):
        items = schema.items()
    else:
        items = schema

    columns: List[Dict[str, Any]] = []
    for name, dtype in items:
        fabric_type = map_polars_dtype_to_fabric(dtype)

        column_def: Dict[str, Any] = {
            "name": name,
            "type": fabric_type,
            # We always declare columns nullable at the manifest level.
            # This avoids accidental incompatibilities when NULLs show up
            # in production data and mirrors the current journal behaviour.
            "nullable": True,
        }

        # Attach precision/scale for decimals when available so downstream
        # consumers can make more informed decisions, but keep behaviour
        # optional and non-breaking.
        if isinstance(dtype, pl.Decimal):
            # Guard against older Polars versions that might not expose
            # precision/scale as attributes.
            precision = getattr(dtype, "precision", None)
            scale = getattr(dtype, "scale", None)
            if precision is not None:
                column_def["precision"] = precision
            if scale is not None:
                column_def["scale"] = scale

        columns.append(column_def)

    return columns


__all__ = ["map_polars_dtype_to_fabric", "build_fabric_schema_columns"]
