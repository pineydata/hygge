---
title: PR Summary - Enhanced CLI for a Warmer Experience
tags: [enhancement, feature]
---

## Overview

- Improved CLI argument syntax with comma-separated flow names
- Enhanced `hygge debug` with warm messaging and path validation
- Added comprehensive testing for CLI improvements

## Key Changes

### CLI Arguments

- `src/hygge/cli.py`:
  - Changed `--flow` and `--entity` to accept comma-separated values
  - Updated help text with clear examples: `--flow flow1,flow2,flow3`
  - No quotes needed, shell-friendly syntax

### Enhanced `hygge debug`

- `src/hygge/cli.py`:
  - Added warm welcome message and emoji indicators
  - Improved configuration validation output
  - Added path validation for parquet homes/stores with actionable guidance
  - Enhanced connection testing with better error messages
  - Added success summary with clear next steps

### Tests

- `tests/unit/hygge/test_cli.py`:
  - Added tests for comma-separated flow/entity syntax
  - Added tests for warm messaging and path validation
  - Updated existing tests for new output format

## Testing

- All tests passing: `pytest tests/unit/hygge/test_cli.py` (20 tests)
- New test coverage for CLI enhancements and path validation

---

**Note**: Please add GitHub labels `enhancement` and `feature` to this PR for proper release notes categorization.
