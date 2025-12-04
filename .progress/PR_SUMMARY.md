---
title: PR Summary - Fix Watermark Filter Fallback: Consistent Filtering Behavior
tags: [bug, fix]
---

## Overview

- Fixed integer watermark filtering to always use `watermark_column` for WHERE clauses, making behavior consistent with datetime and string watermarks
- Removed incorrect `primary_key` preference logic that caused issues with composite keys, non-integer primary keys, and non-sequential primary keys
- Updated tests to validate correct filtering behavior and renamed test methods to reflect actual implementation

## Key Changes

### Watermark Filter Consistency

- `src/hygge/homes/mssql/home.py`:
  - Removed `primary_key` preference logic for integer watermark filters
  - Always use `watermark_column` for filtering (consistent with datetime/string behavior)
  - `primary_key` still required for Flow tracking (validation, deduplication) but not used in WHERE clause
  - Added comments explaining design: filter always uses the column we tracked

### Test Updates

- `tests/unit/hygge/homes/test_mssql_home_coverage.py`:
  - Updated `test_build_watermark_filter_integer` to expect `watermark_column` instead of `primary_key`
  - Renamed `test_build_watermark_filter_integer_falls_back_to_watermark_column` to `test_build_watermark_filter_integer_uses_watermark_column` to reflect correct behavior

- `tests/unit/hygge/homes/test_mssql_home.py`:
  - Renamed `test_read_with_watermark_integer_uses_primary_key` to `test_read_with_watermark_integer_uses_watermark_column`
  - Updated `test_build_watermark_filter_invalid_primary_key` to reflect that invalid `primary_key` doesn't affect filtering (validation happens in Flow tracking)

### Documentation

- `.issues/__TECHNICAL_REVIEW_SUMMARY.md`:
  - Updated roadmap to reflect completed watermark filter fallback fix
  - Moved issue from "Active Issues" to completed work section

## Testing

- All tests passing: `pytest` (all watermark filter tests updated and passing)
- Verified consistent behavior across datetime, string, and integer watermark types
- No breaking changes - fixes incorrect behavior that was causing issues with edge cases

---

**Note**: Remember to add appropriate GitHub labels to this PR (`bug` or `fix`) for proper categorization in release notes.
