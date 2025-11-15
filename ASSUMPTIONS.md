# Hygge Assumptions and Boundaries

**Status: Discovery Phase** - This document outlines assumptions and boundaries that need to be discovered and validated as part of the CLI simplification work. The boundaries documented here are not fully worked out - this is an exercise to determine what's possible with Click and what boundaries make sense for hygge.

**Related Work:** See `issues/cli-simplification.md` for the broader CLI simplification effort.

## 1. One Coordinator Per Project

### Current Behavior

**Assumption:** One coordinator instance per hygge project is expected at a time.

**Why:**
- Journal writes use `asyncio.Lock()` which only provides synchronization within a single process
- Journal file writes use atomic replace (temp file → replace), but no file-level locking
- Multiple coordinators writing to the same journal simultaneously can cause race conditions
- Connection pools are per-coordinator instance (separate pools = separate connections)

**Current Protection:**
- Journal writes use `asyncio.Lock()` for single-process thread safety
- File writes use atomic replace pattern (temp file then replace)
- No file-level locking between processes
- No explicit prevention of multiple coordinators

**Risks:**
- If multiple coordinators run simultaneously:
  - Journal writes could race (read-modify-write pattern without file locking)
  - Connection pools are separate (could exhaust database connections)
  - Destination writes could conflict (especially with `full_drop` mode)

### Recommendations

1. **Document the assumption explicitly** in README and coordinator docs
2. **Add optional file locking** for journal writes (e.g., `fcntl` on Unix, `msvcrt` on Windows)
3. **Add coordinator lock file** to prevent multiple coordinators from running simultaneously
4. **Future:** Support multi-coordinator scenarios with distributed locking (e.g., database locks, file locks, or external coordination service)

### Example Lock File Implementation

```python
# In Coordinator.__init__()
lock_file = Path(project_dir) / ".hygge.lock"
if lock_file.exists():
    # Check if lock is stale (process died)
    # If stale, remove it
    # If active, raise error
    raise ConfigError("Another coordinator is already running")
```

## 2. Per-Entity Run Type Overrides

**Discovery Question:** What boundaries should we set for per-entity run type overrides, and what's possible with Click?

### Current Behavior

**Current Assumption:** Run type flags (`--incremental`, `--full-drop`) apply globally to all flows/entities.

**What We Know:**
- Click processes all flags first, then arguments
- Flags are boolean, not associated with specific entities
- Run type is applied at the flow level, not entity level

**Current Syntax:**
```bash
# All flows in incremental mode
hygge go --incremental

# Specific flows in incremental mode
hygge go --flow salesforce --incremental

# Specific entities (all in same mode)
hygge go --entity salesforce.e1 --entity salesforce.e2 --incremental
```

**Known Limitation:**
- Cannot specify different run types per entity in a single command
- Syntax like `--entity e1 -i --entity e2 -d` doesn't work (Click processes flags globally)

**What Needs Discovery:**
- What syntax patterns are possible with Click?
- What would feel natural for hygge users?
- What's the simplest approach that still provides flexibility?

### Exploration: What's Possible with Click?

**Goal:** Discover what syntax patterns Click supports and what would feel natural for hygge.

#### Option 1: Extended Entity Syntax

Allow run type in entity specification:

```bash
# Incremental for e1, full_drop for e2
hygge go --entity salesforce.e1:incremental --entity salesforce.e2:full_drop

# Shorthand (default to incremental if not specified)
hygge go --entity salesforce.e1:i --entity salesforce.e2:d
```

**To Discover:**
- Is this syntax clear and intuitive?
- How does it parse with Click's argument handling?
- Does it conflict with other entity parsing logic?

#### Option 2: Per-Entity Flags

```bash
# This doesn't work with Click's flag processing
hygge go --entity salesforce.e1 --incremental --entity salesforce.e2 --full-drop
```

**What We Know:**
- Click processes all flags first, then arguments
- Cannot associate flags with specific arguments
- Would require custom argument parsing (is this worth it?)

**To Discover:**
- Is there a Click pattern that could make this work?
- Would custom parsing be maintainable?

#### Option 3: Use --var for Per-Entity Overrides

```bash
# Override run_type for specific entities via --var
hygge go --entity salesforce.e1 --entity salesforce.e2 \
  --var flow.salesforce.entities[0].run_type=incremental \
  --var flow.salesforce.entities[1].run_type=full_drop
```

**What We Know:**
- Verbose and error-prone
- Requires knowing entity indices
- Not intuitive

**To Discover:**
- Is this acceptable as the "power user" option?
- Should we support entity names in --var paths?

#### Option 4: Separate Commands (Simplest)

```bash
# Run entities separately with different run types
hygge go --entity salesforce.e1 --incremental
hygge go --entity salesforce.e2 --full-drop
```

**To Discover:**
- Is this acceptable for users?
- Does it violate the "one coordinator per project" assumption?

### Discovery Tasks

As part of `cli-simplification.md` implementation:

1. **Test Click's capabilities:**
   - Can we parse `flow.entity:run_type` format?
   - Can we associate flags with specific arguments?
   - What are Click's limitations?

2. **User experience exploration:**
   - What feels most natural for hygge?
   - What's the simplest that still provides flexibility?
   - What's the "happy path" vs "power user" path?

3. **Boundary decisions:**
   - Do we need per-entity run types at all?
   - Is global run type sufficient for most use cases?
   - What's the minimum viable feature set?

### Recommendation

**Defer decision** until CLI simplification implementation:
- Implement global run type overrides first (simplest)
- Test and validate with real use cases
- Discover what's actually needed vs what seems nice to have
- Then decide on per-entity syntax if needed

## 3. Other Boundaries

### Journal Concurrency

- **Single-process:** Journal writes are thread-safe within a single process
- **Multi-process:** No protection against concurrent writes from multiple processes
- **Future:** Add file locking or use database-backed journal for multi-coordinator support

### Connection Pools

- **Per-coordinator:** Each coordinator instance creates its own connection pools
- **No sharing:** Connection pools are not shared between coordinator instances
- **Resource limits:** Multiple coordinators can exhaust database connections

### Destination Writes

- **Full-drop mode:** Multiple coordinators writing to same destination can conflict
- **Incremental mode:** Generally safe (append-only), but can cause data duplication if not coordinated
- **Future:** Add destination-level locking or coordination

## Discovery Scope

As part of `cli-simplification.md` implementation, we need to:

1. **Discover Click's capabilities:**
   - What syntax patterns are possible?
   - What are Click's limitations?
   - What requires custom parsing?

2. **Define boundaries:**
   - What's the minimum viable feature set?
   - What's "nice to have" vs "essential"?
   - What feels natural for hygge users?

3. **Validate assumptions:**
   - Is single-coordinator assumption acceptable?
   - Do users need per-entity run types?
   - What are the real use cases?

## Implementation Priority

1. **High:** Document single-coordinator assumption (as-is, with risks)
2. **High:** Discover and document Click boundaries during CLI simplification
3. **Medium:** Add file locking for journal writes (if needed)
4. **Medium:** Implement per-entity run type syntax (if discovered to be needed)
5. **Low:** Add coordinator lock file (if multi-coordinator becomes a requirement)
6. **Low:** Support multi-coordinator scenarios with distributed locking (future work)

## References

- **CLI Simplification:** `issues/cli-simplification.md` - Main work item for discovering and implementing boundaries
- Coordinator implementation: `src/hygge/core/coordinator.py`
- Journal implementation: `src/hygge/core/journal.py`
- CLI implementation: `src/hygge/cli.py`

## Next Steps

1. **During CLI simplification implementation:**
   - Test Click's capabilities with different syntax patterns
   - Validate assumptions with real use cases
   - Document discovered boundaries
   - Update this document with findings

2. **After discovery:**
   - Make boundary decisions based on what's possible and what's needed
   - Implement chosen approach
   - Document final boundaries in README and CLI help
