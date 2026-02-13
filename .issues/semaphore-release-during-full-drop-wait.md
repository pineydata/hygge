# Release Coordinator Semaphore During full_drop Wait

**Status:** Next PR after full_drop simplification
**Effort:** 1–2 days
**Files:** `src/hygge/core/coordinator.py`, `src/hygge/core/flow/flow.py`

## Problem

The coordinator's semaphore holds a slot for the entire flow — including the 120s wait in `finish()` during full_drop. Since the Home connection is idle after extraction, the semaphore blocks other entities from starting extraction during the wait.

### Impact at 68 entities (concurrency 8)

- **Without optimization:** ~9 batches × 120s wait = **~18 min of idle waiting**
- **With optimization:** waits overlap with extraction = **~2 min total wait** (last batch only)
- **Savings: ~16 min per run**

## Approach

Split `_execute_flow()` so the semaphore is released after producer/consumer complete but before `finish()`:

```
acquire semaphore
  → extract (Home → _tmp)
release semaphore              ← other entities can start extracting
  → finish() { delete folder → wait 120s → move _tmp → production }
```

### Key complexity: retry boundary

If `finish()` fails after the semaphore is released, the data is already safely in `_tmp`. Re-extracting is wasteful. This needs a "resume from `_tmp`" path — skip extraction if `_tmp` already has files, go straight to `finish()`.

### Sketch

```python
# coordinator.py
async def _run_entity(self, entity):
    async with self._semaphore:
        await flow.extract()        # Home → _tmp
    await flow.publish()             # delete → wait → move _tmp → production
```

```python
# flow.py
async def extract(self):
    """Producer/consumer: Home reads → Store writes to _tmp."""
    ...

async def publish(self):
    """Move staged data to production (includes full_drop wait)."""
    await self.store.finish()
```

## Why This Is the Next PR

The full_drop simplification PR establishes the correct delete-wait-move sequence. This PR optimizes the *concurrency* of that sequence without changing the logic. Clean separation of concerns.

## Risks

- **Retry semantics change:** Currently the entire flow (extract + publish) retries together. With the split, only publish failures need retry, and they should skip re-extraction.
- **Journal timing:** Journal entries are recorded after `finish()` — need to ensure the semaphore release doesn't affect journal writes.
- **Error propagation:** If publish fails, the coordinator needs to track it properly even though the semaphore was already released.

## Related

- `.progress/IMPLEMENTATION_GUIDE.md` — "Next: Connection Release During Wait" section
- [openmirroring-refactor.md](openmirroring-refactor.md) — broader store cleanup
