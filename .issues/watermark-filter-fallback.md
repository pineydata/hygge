# Watermark Filter Fallback: Contradicts Documentation and Requirements

## Problem

The `_build_watermark_filter()` method in `MssqlHome` allows integer watermarks to fall back to `watermark_column` when `primary_key` is missing or invalid. This contradicts the documented requirement that watermarks require **both** `primary_key` and `watermark_column`, and masks configuration errors.

## Current Behavior

### Implementation Issue

**Location**: `src/hygge/homes/mssql/home.py:296-313`

```python
if watermark_type == "int":
    # ... validation ...
    safe_primary_key = (
        self._validate_identifier(primary_key, "primary_key")
        if primary_key
        else None
    )
    column = safe_primary_key or safe_watermark_column  # ❌ FALLBACK
    if not column:
        return None
    return f"{column} > {numeric_value}"
```

**Problem**: The code falls back to `watermark_column` if `primary_key` is missing or invalid, allowing integer watermarks to work without a primary key.

### Test Issue

**Location**: `tests/unit/hygge/homes/test_mssql_home_coverage.py:372-392`

```python
def test_build_watermark_filter_integer_falls_back_to_watermark_column(self):
    """Test integer filter falls back to watermark_column if no primary_key."""
    # ...
    watermark = {
        "watermark": "1050",
        "watermark_type": "int",
        "watermark_column": "sequence_id",
        # No primary_key
    }
    # ...
    assert filter_clause == "sequence_id > 1050"  # ✅ Falls back
```

**Problem**: The test explicitly validates the fallback behavior, reinforcing the incorrect implementation.

## Contradiction with Documentation and Requirements

### 1. Documentation Says Both Are Required

**Location**: `src/hygge/core/flow/config.py:98`

```python
watermark: Optional[Dict[str, str]] = Field(
    default=None,
    description=(
        "Watermark configuration for incremental loads. "
        "Applies to all entities unless overridden at entity level. "
        "Requires 'primary_key' and 'watermark_column'."  # ✅ BOTH REQUIRED
    ),
)
```

### 2. Flow Tracking Requires Both

**Location**: `src/hygge/core/flow/flow.py:380-381`

```python
if not primary_key or not watermark_column:
    return  # Doesn't track watermark if either is missing
```

**Behavior**: Flow watermark tracking requires both fields. If either is missing, watermark tracking is skipped entirely.

### 3. Implementation Allows Fallback

**Location**: `src/hygge/homes/mssql/home.py:310`

```python
column = safe_primary_key or safe_watermark_column  # ❌ Allows fallback
```

**Behavior**: The filter builder allows integer watermarks to work with only `watermark_column`, contradicting the requirement.

## Why This Is Problematic

1. **Contradicts documented requirements**: Documentation explicitly states both are required
2. **Masks configuration errors**: Missing `primary_key` should fail fast, not silently fall back
3. **Inconsistent behavior**: Flow tracking requires both, but filter building allows one
4. **Potential incorrect queries**: Using `watermark_column` instead of `primary_key` for integer filters may produce incorrect results (integer primary keys are typically more reliable for incremental loads)
5. **Test validates wrong behavior**: The test explicitly validates the fallback, making it harder to fix

## Use Cases

1. **Configuration validation**: Users should get clear errors when required fields are missing
2. **Consistent behavior**: Filter building should match Flow tracking requirements
3. **Correct incremental loads**: Integer watermarks should use primary keys for reliable filtering

## Proposed Solution

### Fix Implementation

**Location**: `src/hygge/homes/mssql/home.py:296-313`

Require `primary_key` for integer watermarks and fail fast if missing:

```python
if watermark_type == "int":
    try:
        numeric_value = int(watermark_value)
    except ValueError:
        self.logger.warning(
            f"Invalid integer watermark value: {watermark_value}"
        )
        return None

    if not primary_key:
        self.logger.error(
            "Integer watermark requires 'primary_key' but it was not provided. "
            "Watermark configuration must include both 'primary_key' and 'watermark_column'."
        )
        return None  # Fail fast instead of falling back

    safe_primary_key = self._validate_identifier(primary_key, "primary_key")
    if not safe_primary_key:
        return None  # Invalid primary_key (SQL injection attempt or invalid identifier)

    return f"{safe_primary_key} > {numeric_value}"
```

### Fix Test

**Location**: `tests/unit/hygge/homes/test_mssql_home_coverage.py:372-392`

Update the test to verify that missing `primary_key` causes failure:

```python
def test_build_watermark_filter_integer_requires_primary_key(self):
    """Test integer filter requires primary_key and fails if missing."""
    # ...
    watermark = {
        "watermark": "1050",
        "watermark_type": "int",
        "watermark_column": "sequence_id",
        # No primary_key
    }

    filter_clause = home._build_watermark_filter(watermark)

    # Should return None (fail fast) instead of falling back
    assert filter_clause is None
```

### Alternative: Keep Fallback but Document It

If there's a legitimate use case for integer watermarks without primary keys, we should:

1. **Update documentation** to clarify when fallback is acceptable
2. **Add logging** to warn when fallback occurs
3. **Update Flow tracking** to allow watermark tracking without primary key (if that's the intent)
4. **Document the trade-offs** of using `watermark_column` vs `primary_key` for integer filters

However, this seems unlikely given the documented requirement and Flow tracking behavior.

## File Locations

- **Implementation**: `src/hygge/homes/mssql/home.py:296-313`
- **Test (coverage)**: `tests/unit/hygge/homes/test_mssql_home_coverage.py:372-392`
- **Documentation**: `src/hygge/core/flow/config.py:98`
- **Flow tracking**: `src/hygge/core/flow/flow.py:380-381`

## Implementation Plan

1. **Fix implementation** to require `primary_key` for integer watermarks
2. **Update test** to verify fail-fast behavior instead of fallback
3. **Verify consistency** with Flow tracking requirements
4. **Check for other homes** that might have similar fallback logic (Parquet, SQLite, etc.)

## Testing Considerations

- Verify that missing `primary_key` causes filter building to return `None`
- Verify that invalid `primary_key` (SQL injection attempt) causes filter building to return `None`
- Verify that valid `primary_key` produces correct filter clause
- Ensure error messages are clear and helpful

## Related Issues

- See `watermark-tracker-extraction.md` for related watermark architecture
- See `test-coverage-improvements.md` for context on test coverage work that surfaced this issue

## Priority

**Medium** - This is a correctness issue that could lead to incorrect incremental loads, but it may not affect all users (only those using integer watermarks without primary keys). The fallback behavior has been present, so this is more about fixing incorrect behavior than a breaking change.
