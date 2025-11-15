# Next Steps: Incremental Config Validation & Documentation

## Status

✅ **Done:**
- Remote journal backends (ADLS/OneLake storage fully implemented)
- `incremental` field exists in `ADLSStoreConfig` (inherited by `OneLakeStoreConfig` and `OpenMirroringStoreConfig`)
- Stores respect `incremental` override via `configure_for_run` method

## Still Needed

1. **Coordinator Validation**
   - Add validation in coordinator that warns when Flow `run_type` and store `incremental` behavior diverge
   - Consider whether to warn or fail when they mismatch (currently stores silently override)
   - Example: `run_type: incremental` but `store.incremental: false` should trigger a warning

2. **Documentation**
   - Document how Flow/journal incremental settings map to store behavior (truncate vs delta apply)
   - Clarify the relationship between:
     - Flow `run_type` (incremental vs full_drop)
     - Store `incremental` override (Optional[bool])
     - Journal watermark tracking
     - Actual store behavior (truncate vs append)
