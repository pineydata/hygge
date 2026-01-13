---
title: PR Summary - Add --dry-run flag for previewing flows
tags: [enhancement, feature]
---

## Overview

- Add `--dry-run` flag to preview flow execution without moving data or connecting to sources/destinations
- Users can verify configuration before running actual data movement
- Supports both concise (one-line per flow) and verbose (detailed) output via `--verbose` flag
- Refactored coordinator to eliminate ~40 lines of duplicated setup logic

## Key Changes

### CLI & Preview Output

- `src/hygge/cli.py`:
  - Added `--dry-run` flag to `hygge go` command
  - Implemented concise and verbose formatting for preview results
  - Shows flow name, source → destination types, incremental/full load mode, and warnings
  - Fail-fast on required fields instead of silent fallbacks

### Flow Preview Logic

- `src/hygge/core/flow/flow.py`:
  - Added `Flow.preview()` method that extracts config info without I/O
  - Returns home/store types, paths, incremental settings, and detected warnings
  - True dry-run: no connections, no data reads, config-only inspection

### Coordinator Orchestration

- `src/hygge/core/coordinator.py`:
  - Extracted `_prepare_for_execution()` to eliminate DRY violation between `preview()` and `run()`
  - Added `preview()` method to orchestrate multi-flow previews
  - Reduced code by 27 lines through refactoring

### Documentation

- `README.md`:
  - Added "Preview What Would Run" section with usage examples
  - Shows both concise and verbose output formats
- `.gitignore`:
  - Ignored test demo project directory

### Tests

- `tests/unit/hygge/test_dry_run.py`:
  - 6 new tests covering flow preview, coordinator preview, and formatting
  - Tests verify no-connection behavior and config-only inspection

## Testing

- All tests passing: `pytest tests/unit/hygge/test_dry_run.py` (6 tests)
- Existing test suite continues to pass
- Pre-commit hooks passing (ruff, formatting, etc.)

## Philosophy Alignment

- **Safety & Trust**: Preview before execution reduces risk
- **Convention over Configuration**: Zero config needed, just add `--dry-run`
- **Comfort Over Complexity**: Simple flag, natural output
- **Fail Fast**: No silent fallbacks, clear error messages

---

**Remember to add GitHub labels**: `enhancement`, `feature` for proper release notes categorization
