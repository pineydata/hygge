"""
Journal for tracking flow execution metadata and watermarks.

The journal provides structured, durable tracking of entity runs with:
- Run-based architecture (one row per entity run)
- Single-file parquet storage (journal.parquet)
- Deterministic hash-based run IDs
- Optional watermark tracking for incremental loads
- Schema evolution support
"""
from ..journal import Journal, JournalConfig

__all__ = ["Journal", "JournalConfig"]
