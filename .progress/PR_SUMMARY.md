---
title: PR Summary - Mirror Journal Batching
tags: [enhancement, performance]
---

## Overview

- Batched mirrored journal updates to reduce landing zone churn, publishing once per successful entity completion instead of after each entity run notification.
- Added deferred publish mechanism with dirty flag tracking to accumulate entity run notifications before mirroring to Fabric.
- Ensures mirror publishes after each successful entity, providing timely updates while reducing transient empty snapshots.

## Key Changes

### Mirrored Journal Writer

- `src/hygge/core/journal.py`:
  - Modified `MirroredJournalWriter.append()` to set a `_dirty` flag instead of immediately mirroring, deferring the actual publish operation.
  - Added `MirroredJournalWriter.publish()` method that performs the full-drop rewrite of the mirrored table only when dirty, reloading the canonical journal parquet snapshot.
  - Added error handling in `publish()` to preserve dirty flag on failure, ensuring retry on next publish attempt.
  - Added `Journal.publish_mirror()` public method to trigger mirror publishing, safe to call even if mirroring is disabled (no-op).
  - Added "Mirrored journal refreshed" log message when publish completes for observability.

### Flow Execution

- `src/hygge/core/flow/flow.py`:
  - Added `publish_mirror()` call after successful `_record_entity_run()`, ensuring mirror is published once per successful entity completion.

### Tests

- `tests/unit/hygge/core/test_journal.py`:
  - Added `TestMirroredJournalBatching` suite with three tests: verifies multiple appends batch into single publish, confirms publish idempotency, and validates empty journal handling.

## Testing

- All tests passing: `pytest` (36 tests in test_journal.py, all passing)
- New batching tests verify the deferred publish mechanism works correctly and handles edge cases.

---

**Note**: Remember to add appropriate GitHub labels to this PR (e.g., `enhancement` and `performance`) for proper categorization in release notes.
