---
title: PR Summary - Narrative Progress & Completion Messages
tags: [enhancement, feature]
---

## Overview

- Added warm, narrative progress messages with emojis and file paths during data movement
- Enhanced completion summaries with celebratory success messages and compassionate error guidance
- Streamlined code to maintain hygge balance - removed complexity, kept warmth

## Key Changes

### Progress Messages

- `src/hygge/messages/progress.py`:
  - Changed milestone messages to use "moved" language with 📊 emoji for warmth
- `src/hygge/core/store.py`:
  - Added optional `path` parameter to `_log_write_progress()` to show where data is written
- `src/hygge/stores/parquet/store.py`, `adls/store.py`, `sqlite/store.py`, `mssql/store.py`, `openmirroring/store.py`:
  - Updated all stores to pass file/table paths to progress logger for concrete context

### Flow Journey Logging

- `src/hygge/core/flow/flow.py`:
  - Added `_log_journey_start()` to log data journey at DEBUG level (source → destination)
  - Added batch completion messages with running totals

### Completion Summaries

- `src/hygge/messages/summary.py`:
  - Split `generate_summary()` into focused methods for success and error cases
  - Success: Celebratory messages with "✨ All done!" and detailed flow results
  - Errors: Compassionate "⚠️ Some flows need attention" with helpful next steps
  - Broke large methods into smaller helpers for better maintainability

### Tests

- `tests/unit/hygge/messages/test_completion_narrative.py`, `test_narrative_progress.py`: New behavioral tests for narrative improvements
- `tests/unit/hygge/messages/test_summary.py`: Replaced 30+ brittle tests checking exact wording with 9 focused behavioral tests
- `tests/unit/hygge/messages/test_progress.py`: Updated assertions to match new message format

### Documentation

- `.issues/hygge-feels-hyggesque.md`: Updated to mark narrative progress as complete

## Testing

All tests passing: 29 message tests collected, all passing

## Hygge Balance

This PR maintains hygge's principle of **comfort without complexity**:
- Net reduction: ~205 lines removed (simplified tests, streamlined flow methods)
- Warmth added where it matters: user-visible progress and summaries
- No maintenance burden: behavioral tests, not brittle message checks

---

**Note**: Remember to add `enhancement` and `feature` labels to the PR for proper release note categorization.
