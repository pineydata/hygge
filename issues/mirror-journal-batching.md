---
title: Mirror Journal Batching
---

### Problem

- Mirrored journal writer reloads `.hygge_journal/journal.parquet` after every entity completes. Each call triggers a full-drop rewrite of the mirrored table, so flows with multiple entities churn the landing zone repeatedly and risk transient empty snapshots in Fabric.

### Desired Behaviour

- Accumulate entity run notifications and publish the mirrored snapshot once per flow run (or coordinator run). For example:
  - Set a “dirty” flag when `record_entity_run` succeeds.
  - Defer the actual mirror until `Flow.finish()` or the coordinator’s `finish()` hook.

### Considerations

- Ensure the deferred publish still fires on success and failure (cleanup path).
- Keep the canonical parquet the single source of truth; the mirror continues to full-drop rewrite from that snapshot.
- Log a clear “Mirrored journal refreshed” message at the batching point for observability.
