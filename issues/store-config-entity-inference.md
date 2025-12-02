# Store Config Inference from Entity Config

## Problem

**Open Mirroring stores** (and only Open Mirroring) require `key_columns` (plural, list) in their store configuration. The validation error occurs when `key_columns` is missing from the flow-level store config, even though the original design supports per-entity store config overrides.

**Note**: This requirement is **isolated to Open Mirroring** - no other stores (Parquet, ADLS, OneLake, MSSQL, SQLite) require `key_columns`. Open Mirroring needs it for its metadata.json file that specifies which columns form the unique key for merge/upsert operations.

### Original Design Pattern

The original design supports per-entity store config overrides via the `store` section in entity configs:

```yaml
# Entity config can override store config
name: "users"
store:
  key_columns: ["id"]  # Override flow-level store config
```

The coordinator merges entity store configs with flow store configs (see `Coordinator._create_entity_flow()` lines 599-609).

### The Problem

1. **Validation happens too early**: `FlowConfig` validates store config when created (in workspace), before entity configs are considered
2. **Open Mirroring requires `key_columns`**: The field is marked as required (`...`) in `OpenMirroringStoreConfig`
3. **Flow-level config is incomplete**: User's flow.yml doesn't have `key_columns` because they want to set it per-entity

### What Should Work (According to Original Design)

```yaml
# flow.yml - store config without key_columns (should be OK if entities provide it)
store:
  type: open_mirroring
  # ... other config ...
  # key_columns can be provided per-entity

# entities/users.yml - entity provides key_columns
name: "users"
store:
  key_columns: ["id"]
```

### Error Encountered

```
Failed to load flow edw: 1 validation error for FlowConfig
store.key_columns
  Field required [type=missing, input_value={'type': 'open_mirroring'...}, input_type=dict]
```

### Example Flow Config

```yaml
# flows/edw/flow.yml
name: "edw"
run_type: full_drop
home:
  type: "mssql"
  connection: "edw-prod"
  table: "dbo.{entity}"
store:
  type: open_mirroring
  account_url: "${ONELAKE_ACCOUNT_URL}"
  filesystem: "${FABRIC_WORKSPACE_GUID}"
  mirror_name: "${MIRRORED_DB_GUID}"
  row_marker: 4
  # key_columns missing - should be inferred from entity config
```

### Entity Config Pattern

Entities typically define `key_column` in defaults or per-entity:

```yaml
# flows/edw/defaults (or entity-specific)
defaults:
  key_column: "id"  # Singular, string
  batch_size: 10000
```

But Open Mirroring needs:
```yaml
store:
  key_columns: ["id"]  # Plural, list
```

## Current State

### What Works
- Entity configs can override store config via `entity.store.key_columns` (original design)
- Flow-level store configs can explicitly define `key_columns`
- Entity store config merging works (lines 599-609 in coordinator)

### What Doesn't Work
- **Validation happens too early**: `FlowConfig` validation requires `key_columns` at flow level, even though entities can provide it
- **Required field blocks per-entity pattern**: `key_columns` is marked as required (`...`), so flow-level config must have it
- **User's flow fails validation**: Flow.yml doesn't have `key_columns` because they want to set it per-entity

## Solution (Following Original Design)

The original design already supports this! The fix is simple:

1. **Make `key_columns` optional** in `OpenMirroringStoreConfig` (default: `None`)
2. **Validate at store creation time** (not at config validation time)
3. **Entity configs can override** via `entity.store.key_columns` (already works)

This follows the existing pattern:
- Flow-level store config can be incomplete
- Entity configs can override via `entity.store.*` section
- Validation happens when store is actually created (in coordinator)

## Proposed Solution (Following Original Design)

### Simple Fix

The original design already supports per-entity store config overrides. We just need to:

1. **Make `key_columns` optional** in `OpenMirroringStoreConfig`
   ```python
   key_columns: Optional[List[str]] = Field(
       default=None,
       description="Required for Open Mirroring. Can be set at flow level or per-entity via entity.store.key_columns"
   )
   ```

2. **Validate at store creation** (not at config validation)
   ```python
   # In OpenMirroringStore.__init__()
   if config.key_columns is None or len(config.key_columns) == 0:
       raise StoreError("key_columns is required for Open Mirroring...")
   ```

3. **Entity configs can override** (already works via lines 599-609)
   ```yaml
   # entities/users.yml
   name: "users"
   store:
     key_columns: ["id"]  # List format
     # OR
     key_columns: "id"    # String format - auto-converted to ["id"]
   ```

4. **String-to-list conversion** (convenience feature)
   - `key_columns: "id"` → automatically becomes `["id"]`
   - `key_columns: ["id"]` → stays as `["id"]`
   - Makes single-column keys more convenient

### Why This Works

- ✅ Follows existing entity.store override pattern (already implemented)
- ✅ No inference needed - just use the existing merge mechanism
- ✅ Validation happens at the right time (when store is created)
- ✅ Clear error if neither flow nor entity provides `key_columns`

## Implementation Details

### Required Changes

1. **OpenMirroringStoreConfig**: Make `key_columns` optional with string-to-list conversion
   ```python
   key_columns: Optional[List[str]] = Field(
       default=None,
       description=(
           "Required for Open Mirroring (metadata.json keyColumns). "
           "Can be set at flow level or per-entity via entity.store.key_columns. "
           "Accepts both string (single column) or list (multiple columns). "
           "Note: This is Open Mirroring specific - no other stores require key_columns."
       )
   )

   @field_validator("key_columns", mode="before")
   @classmethod
   def normalize_key_columns(cls, v):
       """Convert string to list for convenience."""
       if v is None:
           return None
       if isinstance(v, str):
           return [v]  # Convert "Id" -> ["Id"]
       if isinstance(v, list):
           return v  # Already a list
       raise ValueError(f"key_columns must be a string or list of strings, got {type(v)}")
   ```

2. **OpenMirroringStore**: Validate `key_columns` is set in `__init__`
   ```python
   # In OpenMirroringStore.__init__()
   if config.key_columns is None or len(config.key_columns) == 0:
       entity_info = f" for entity '{entity_name}'" if entity_name else ""
       raise StoreError(
           f"key_columns is required for Open Mirroring store{entity_info}. "
           f"Please specify key_columns in the store config or entity.store.key_columns."
       )
   ```

3. **No coordinator changes needed** - existing entity.store merge (lines 599-609) already handles this

## Testing Considerations

- Test flow-level `key_columns` works (both list and string formats)
- Test entity-level `entity.store.key_columns` override works (both list and string formats)
- Test string-to-list conversion: `"id"` → `["id"]`
- Test error when neither flow nor entity provides `key_columns`
- Test with multiple entities (different key columns per entity)
- Test with non-entity flows (requires flow-level `key_columns`)
- Test invalid types (e.g., `key_columns: 123` should fail)

## Related Issues

- Follows "convention over configuration" principle
- Aligns with entity config merging patterns
- **Open Mirroring specific**: No other stores require `key_columns`, so this solution is scoped to Open Mirroring only

## Questions to Resolve

1. **Should `key_columns` be optional at flow level?**
   - Yes (recommended) - allows per-entity configuration
   - No - requires flow-level config, entities can override

2. **When should validation happen?**
   - At store creation (recommended) - allows per-entity configs
   - At config validation (current) - catches errors earlier but blocks per-entity pattern

3. **Naming mismatch: `key_column` vs `key_columns`**
   - Current: Entity configs have `key_column` (singular, string) in defaults
   - README says it's for "watermark tracking" but watermark actually uses `primary_key`
   - `key_column` doesn't appear to be used anywhere in the codebase
   - **Decision**: Only support explicit `entity.store.key_columns: ["id"]` pattern
   - Users must use the correct format - no automatic conversion from `key_column` to `key_columns`

## Next Steps

### Phase 1: Fix Validation (Required)
1. **Make `key_columns` optional** in `OpenMirroringStoreConfig`
2. **Move validation to store creation** (in `OpenMirroringStore.__init__()`)
3. **Update tests** to reflect optional `key_columns` at flow level
4. **Document the pattern**: Entity configs can override store config via `entity.store.key_columns`

### Phase 2: Documentation (Required)
5. **Update examples**: Show explicit `entity.store.key_columns` pattern
6. **Clarify `key_column` purpose**: Document that `key_column` in defaults is not used for Open Mirroring - use `entity.store.key_columns` instead
7. **Update README**: Fix misleading comment about `key_column` being for watermark tracking

## Risk & Complexity Assessment

### Complexity: **Low** ⭐⭐

**Code Changes:**
- **2 small changes** in `OpenMirroringStoreConfig` and `OpenMirroringStore`
- **~15 lines of code** total (includes string-to-list validator)
- **No coordinator changes** - uses existing entity.store merge mechanism
- **Isolated to Open Mirroring** - no impact on other stores

**Test Changes:**
- Update 2-3 existing tests that check `key_columns` is required
- Add 2-3 new tests for optional `key_columns` scenarios
- Add 1-2 tests for string-to-list conversion
- **~60-110 lines of test code**

**Documentation:**
- Update docstrings and examples
- **~20-30 lines**

**Total Effort Estimate:** 2-4 hours (includes string-to-list conversion)

### Risk: **Low-Medium** ⚠️

**Low Risk Factors:**
- ✅ **Isolated change** - only affects Open Mirroring store
- ✅ **Backward compatible** - existing configs with `key_columns` still work
- ✅ **Simple validation logic** - straightforward None/empty check
- ✅ **Uses existing patterns** - entity.store merge already works
- ✅ **Clear error messages** - validation happens at store creation with helpful errors

**Medium Risk Factors:**
- ⚠️ **Validation timing change** - errors caught later (at store creation vs config validation)
  - **Mitigation**: Error messages are clear and point to solution
- ⚠️ **Test updates required** - existing tests expect `key_columns` to be required
  - **Mitigation**: Tests are straightforward to update
- ⚠️ **Edge cases** - empty list `[]` vs `None` vs missing vs string
  - **Mitigation**: Validation checks `None`, empty list, and converts string to list

**Potential Issues:**
1. **Existing flows without entity configs** - will fail at store creation (expected behavior)
2. **Empty list edge case** - `key_columns: []` should fail validation (handled)
3. **Type confusion** - `None` vs empty list (handled by validation)
4. **String vs list** - `key_columns: "id"` vs `key_columns: ["id"]` (handled by validator)

**Scenario Analysis:**

**Scenario 1: Flow-level has `key_columns`, no entities**
```yaml
# flow.yml
store:
  type: open_mirroring
  key_columns: ["id"]  # ✅ List format
  # OR
  key_columns: "id"    # ✅ String format - auto-converted to ["id"]
```
- ✅ **Works**: Store gets `key_columns` from flow config
- ✅ **Backward compatible**: Existing flows continue to work
- ✅ **String support**: `key_columns: "id"` automatically becomes `["id"]`

**Scenario 2: Flow-level has `key_columns`, entities override**
```yaml
# flow.yml
store:
  type: open_mirroring
  key_columns: ["id"]  # Flow-level default (or "id" as string)

# entities/users.yml
store:
  key_columns: ["user_id"]  # Entity-specific override (or "user_id" as string)
```
- ✅ **Works**: Entity override takes precedence (existing merge behavior)
- ✅ **Expected**: Per-entity customization works as designed
- ✅ **String support**: Both flow-level and entity-level support string format

**Scenario 3: Flow-level has no `key_columns`, entities provide it**
```yaml
# flow.yml
store:
  type: open_mirroring
  # key_columns not provided

# entities/users.yml
store:
  key_columns: ["id"]  # Entity provides it (list)
  # OR
  key_columns: "id"    # Entity provides it (string) - auto-converted to ["id"]
```
- ✅ **Works**: Entity provides `key_columns`, merged into store config
- ✅ **This is the use case we're fixing**
- ✅ **String support**: `key_columns: "id"` automatically becomes `["id"]`

**Scenario 4: Flow-level has no `key_columns`, entities don't provide it**
```yaml
# flow.yml
store:
  type: open_mirroring
  # key_columns not provided

# entities/users.yml (or no entities)
# No store.key_columns override
```
- ❌ **Fails at store creation**: Clear error message guides user
- ✅ **Expected behavior**: `key_columns` is required, must be provided somewhere

**Scenario 5: Flow has entities list, but entities don't have store configs**
```yaml
# flow.yml
store:
  type: open_mirroring
  # key_columns not provided
entities:
  - users  # Simple string entity, no config file

# No entities/users.yml file, or file has no store section
```
- ❌ **Fails at store creation**: Entity doesn't provide `key_columns`
- ✅ **Expected behavior**: User must either:
  - Add `key_columns` to flow-level store config, OR
  - Create entity config file with `store.key_columns`

**Risk Mitigation:**
- ✅ Validation happens early in `__init__()` before any operations
- ✅ Clear error messages guide users to solution
- ✅ Existing entity.store merge mechanism is well-tested
- ✅ Isolated to one store type - easy to rollback if needed
- ✅ All scenarios handled: Flow-level, entity-level, or both work correctly
- ✅ Backward compatible: Existing flows with flow-level `key_columns` continue to work

### Recommendation

**Proceed with implementation** - Low complexity, manageable risk, solves real user problem.

**Implementation Order:**
1. Update config (make optional) + validation (store creation)
2. Update tests
3. Test with real flow configs
4. Update documentation

## Recommendation

**Solution: Explicit Pattern Only**

1. **Fix validation timing** (required):
   - Make `key_columns` optional at flow level (allows per-entity configs)
   - Validate when store is created (not during config validation)
   - Use existing `entity.store.*` override mechanism (already implemented)

2. **Use explicit pattern only**:
   - Only support `entity.store.key_columns: ["id"]` (explicit override)
   - Users must use the correct format - no automatic conversion
   - No inference/conversion from `key_column` to `key_columns`
   - Clearer and follows existing architecture

The explicit pattern (`entity.store.key_columns`) is clear, follows the existing architecture, and requires no additional inference logic.
