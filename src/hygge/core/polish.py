"""
Lightweight, opt-in polishing utilities for last-mile Store transforms.

Polisher is intentionally small and comfortable:
- Column normalization (remove special chars, PascalCase, optional space removal)
- Deterministic row-level hash ID generation
- Generic constant columns (e.g., __rowMarker__)
- Load timestamps (e.g., __LastLoadedAt__)

All behavior is configured via `PolishConfig` and applied per-Store.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import polars as pl
from pydantic import BaseModel, Field, field_validator, model_validator


class ColumnRules(BaseModel):
    """Simple column normalization rules."""

    remove_special: bool = Field(
        default=False,
        description=(
            "Remove parentheses (and contents) and non-alnum chars " "except spaces/_/-"
        ),
    )
    remove_spaces: bool = Field(
        default=False,
        description="Remove spaces from column names if true.",
    )
    case: Optional[str] = Field(
        default=None,
        description=(
            "Case conversion: 'pascal' (PascalCase), 'camel' (camelCase), "
            "'snake' (snake_case), or None (no conversion)."
        ),
    )

    @field_validator("case")
    @classmethod
    def validate_case(cls, v):
        """Validate case value."""
        if v is not None and v not in ["pascal", "camel", "snake"]:
            raise ValueError(
                f"case must be one of: 'pascal', 'camel', 'snake', or None. Got: {v}"
            )
        return v


class HashIdRule(BaseModel):
    """Configuration for a row-level hash ID."""

    name: str = Field(..., description="Name of the hash column to add.")
    from_columns: List[str] = Field(
        ..., description="Columns to concatenate (as strings) before hashing."
    )
    algo: str = Field(
        default="sha256",
        description="Hash algorithm (default: sha256).",
    )
    hex: bool = Field(
        default=True,
        description="If true, store hex digest (Utf8); otherwise raw bytes (Binary).",
    )


class ConstantRule(BaseModel):
    """Configuration for a constant-valued column."""

    name: str = Field(..., description="Name of the constant column to add.")
    value: Any = Field(..., description="Constant value for all rows.")


class TimestampRule(BaseModel):
    """Configuration for a load timestamp column."""

    name: str = Field(..., description="Name of the timestamp column to add.")
    source: str = Field(
        default="now_utc",
        description="Time source: 'now_utc' (default) or 'now_local'.",
    )
    type: str = Field(
        default="datetime",
        description="Storage type: 'datetime' (default) or 'string'.",
    )
    format: Optional[str] = Field(
        default=None,
        description="Optional strftime format if type='string'.",
    )


class RowMarkerAlias(BaseModel):
    """Back-compat alias for row marker configuration."""

    enabled: bool = Field(
        default=False,
        description="When true, behaves like a ConstantRule for __rowMarker__.",
    )
    value: int = Field(
        default=4,
        description="Row marker value: 0=Insert,1=Update,2=Delete,4=Upsert.",
    )


class PolishConfig(BaseModel):
    """
    Configuration for Polisher.

    This is Store-scoped and intentionally minimal.
    """

    columns: ColumnRules = Field(
        default_factory=ColumnRules,
        description="Column normalization rules.",
    )
    hash_ids: List[HashIdRule] = Field(
        default_factory=list,
        description="Row-level hash ID definitions.",
    )
    constants: List[ConstantRule] = Field(
        default_factory=list,
        description="Constant-valued columns (e.g., __rowMarker__).",
    )
    timestamps: List[TimestampRule] = Field(
        default_factory=list,
        description="Timestamp columns (e.g., __LastLoadedAt__).",
    )
    # Backwards-compatible alias – maps to constants during validation.
    add_row_marker: Optional[RowMarkerAlias] = Field(
        default=None,
        description="Legacy alias for a __rowMarker__ constant. Prefer `constants`.",
    )

    @model_validator(mode="after")
    def apply_row_marker_alias(self) -> "PolishConfig":
        """
        Map add_row_marker alias into constants for backwards compatibility.

        If enabled and no explicit __rowMarker__ constant is present,
        append a ConstantRule for __rowMarker__.
        """
        alias = self.add_row_marker
        if alias and alias.enabled:
            has_row_marker = any(c.name == "__rowMarker__" for c in self.constants)
            if not has_row_marker:
                self.constants.append(
                    ConstantRule(name="__rowMarker__", value=alias.value)
                )
        return self


@dataclass
class Polisher:
    """
    Lightweight, per-Store polishing helper.

    Responsibilities:
    - Apply configured column normalization
    - Add hash IDs
    - Add constant columns
    - Add timestamp columns
    """

    config: PolishConfig

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply all configured polish steps to the DataFrame."""
        if df is None or not isinstance(df, pl.DataFrame) or df.is_empty():
            return df

        # 1) Column normalization
        df = self._apply_columns(df)

        # 2) Hash IDs
        if self.config.hash_ids:
            df = self._apply_hash_ids(df, self.config.hash_ids)

        # 3) Constants
        if self.config.constants:
            df = self._apply_constants(df, self.config.constants)

        # 4) Timestamps
        if self.config.timestamps:
            df = self._apply_timestamps(df, self.config.timestamps)

        return df

    def _normalize_to_words(self, name: str) -> List[str]:
        """
        Normalize a column name into a list of words.

        Handles spaces, underscores, hyphens, camelCase, and PascalCase boundaries.
        Examples:
        - "EmployeeNumber" -> ["Employee", "Number"]
        - "XMLParser" -> ["XML", "Parser"]
        - "HTTPSConnection" -> ["HTTPS", "Connection"]
        - "employeeNumber" -> ["employee", "Number"]
        - "XMLHTTPRequest" -> ["XML", "HTTP", "Request"]
        """
        # First, normalize delimiters to spaces
        base = name.replace("_", " ").replace("-", " ")

        # Split on camelCase boundaries (lowercase to uppercase)
        # e.g., "employeeNumber" -> "employee Number"
        base = re.sub(r"(?<=[a-z])([A-Z])", r" \1", base)

        # Split on PascalCase boundaries: uppercase-to-uppercase transition
        # when the second uppercase is followed by lowercase
        # This handles acronyms followed by words: "XMLParser" -> "XML Parser"
        # Pattern: split before [A-Z][a-z] when preceded by [A-Z]
        # e.g., "XMLParser": L (uppercase) before P (uppercase) + "arser" (lowercase)
        base = re.sub(r"(?<=[A-Z])([A-Z][a-z])", r" \1", base)

        # Split on whitespace and filter empty parts
        parts = base.split()
        return [p for p in parts if p]

    def _to_pascal_case(self, name: str) -> str:
        """Convert to PascalCase: 'employee number' -> 'EmployeeNumber'."""
        words = self._normalize_to_words(name)
        return "".join(word.capitalize() for word in words)

    def _to_camel_case(self, name: str) -> str:
        """Convert to camelCase: 'employee number' -> 'employeeNumber'."""
        words = self._normalize_to_words(name)
        if not words:
            return name
        return words[0].lower() + "".join(word.capitalize() for word in words[1:])

    def _to_snake_case(self, name: str) -> str:
        """Convert to snake_case: 'Employee Number' -> 'employee_number'."""
        words = self._normalize_to_words(name)
        return "_".join(word.lower() for word in words)

    def _apply_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        rules = self.config.columns
        # Check if any transformation is needed
        needs_transform = any(
            [
                rules.remove_special,
                rules.remove_spaces,
                rules.case is not None,
            ]
        )
        if not needs_transform:
            return df

        mapping: Dict[str, str] = {}
        for col in df.columns:
            new_name = col

            if rules.remove_special:
                # Extract words from parentheses before removing them
                # e.g., "Effective-Date (UTC)" -> extract "UTC" first
                paren_matches = re.findall(r"\(([^)]+)\)", new_name)
                paren_words = []
                for match in paren_matches:
                    # Split the content by spaces and add as separate words
                    words = match.split()
                    paren_words.extend(words)

                # Remove parentheses + contents and other special chars
                # except spaces/_/-
                new_name = re.sub(
                    r"\s*\([^)]*\)|[^a-zA-Z0-9\s_-]",
                    "",
                    new_name,
                ).strip()

                # Append extracted words from parentheses
                if paren_words:
                    new_name = f"{new_name} {' '.join(paren_words)}"

            if rules.remove_spaces:
                new_name = new_name.replace(" ", "")

            # Case conversion
            if rules.case:
                if rules.case == "pascal":
                    new_name = self._to_pascal_case(new_name)
                elif rules.case == "camel":
                    new_name = self._to_camel_case(new_name)
                elif rules.case == "snake":
                    new_name = self._to_snake_case(new_name)

            # Avoid empty names – fall back to original
            if not new_name:
                new_name = col

            mapping[col] = new_name

        # Only rename if something actually changed
        if any(src != dst for src, dst in mapping.items()):
            df = df.rename(mapping)
        return df

    def _apply_hash_ids(
        self, df: pl.DataFrame, rules: Iterable[HashIdRule]
    ) -> pl.DataFrame:
        for rule in rules:
            missing = [c for c in rule.from_columns if c not in df.columns]
            if missing:
                # Comfort over correctness: skip misconfigured rule
                # rather than fail the flow.
                continue

            concat = pl.concat_str(
                [pl.col(c).cast(pl.Utf8) for c in rule.from_columns],
                separator="|",
            )

            algo = rule.algo or "sha256"

            def _digest(val: Optional[str]) -> Any:
                s = "" if val is None else str(val)
                h = hashlib.new(algo)
                h.update(s.encode("utf-8"))
                return h.hexdigest() if rule.hex else h.digest()

            dtype = pl.Utf8 if rule.hex else pl.Binary
            df = df.with_columns(
                concat.map_elements(_digest, return_dtype=dtype).alias(rule.name)
            )

            # Place hash column first for visibility (comfortable default)
            cols = [rule.name] + [c for c in df.columns if c != rule.name]
            df = df.select(cols)

        return df

    def _apply_constants(
        self, df: pl.DataFrame, rules: Iterable[ConstantRule]
    ) -> pl.DataFrame:
        existing = set(df.columns)
        new_exprs = []
        new_names: List[str] = []

        for rule in rules:
            if rule.name in existing:
                # Respect existing column; do not override silently.
                continue
            new_exprs.append(pl.lit(rule.value).alias(rule.name))
            new_names.append(rule.name)

        if not new_exprs:
            return df

        df = df.with_columns(new_exprs)
        # Simpler default: append new constant columns at the end.
        # Stores with strict ordering (e.g., Open Mirroring) can re-order after.
        return df

    def _apply_timestamps(
        self, df: pl.DataFrame, rules: Iterable[TimestampRule]
    ) -> pl.DataFrame:
        existing = set(df.columns)
        new_exprs = []

        for rule in rules:
            if rule.name in existing:
                continue

            # Time source
            if rule.source == "now_local":
                now = datetime.now()
            else:
                # Default: UTC
                now = datetime.now(timezone.utc)

            if rule.type == "string":
                if rule.format:
                    value = now.strftime(rule.format)
                else:
                    value = now.isoformat()
            else:
                value = now

            new_exprs.append(pl.lit(value).alias(rule.name))

        if not new_exprs:
            return df

        df = df.with_columns(new_exprs)
        return df


def apply_polish(df: pl.DataFrame, config: Optional[PolishConfig]) -> pl.DataFrame:
    """
    Convenience helper for stores that want one-shot behavior.

    Stores that do not need a long-lived Polisher instance can call this directly.
    """
    if not config:
        return df
    polisher = Polisher(config=config)
    return polisher.apply(df)
