# Enable Manual Single/Multiple Flow Execution

## Problem

When a flow fails during execution, there's no easy way to retry just that failed flow. Currently:

- Failed flows are logged in the journal (if enabled) with `event_type="fail"`
- The CLI has a `--flow` option that is **stubbed but not implemented** (see `cli.py:235`)
- Users must either:
  - Rerun all flows: `hygge go` (inefficient, potentially re-processing successful flows)
  - Manually filter flows in configuration files (cumbersome)

## Current Behavior

```bash
# This is what users want to do:
hygge go --flow salesforce_Involvement

# But currently outputs:
# "Single flow execution not yet implemented"
```

From `src/hygge/cli.py:233-236`:
```python
if flow:
    click.echo(f"Starting flow: {flow}")
    # TODO: Implement single flow execution
    click.echo("Single flow execution not yet implemented")
```

## Use Cases

1. **Manual Retry After Failure**: After a flow fails, retry just that flow:
   ```bash
   hygge go --flow salesforce_Involvement
   ```

2. **Selective Execution**: Run only specific flows during development/testing:
   ```bash
   hygge go --flow flow1 --flow flow2
   ```

3. **Entity Flow Support**: Support entity flows (e.g., `salesforce_Involvement` where `salesforce` is the base flow and `Involvement` is the entity)

## Proposed Solution

### Phase 1: Single Flow Execution

1. Add `flow_filter: Optional[List[str]]` parameter to `Coordinator.__init__()`
2. Modify `Coordinator._create_flows()` to filter flows by name:
   - Match base flow names (e.g., `salesforce`)
   - Match entity flow names (e.g., `salesforce_Involvement`)
   - Support exact matches and base flow name matches
3. Update CLI `go` command to accept `--flow` option (can be called multiple times)
4. Pass flow filter to Coordinator

### Implementation Example

```python
# In Coordinator.__init__()
def __init__(
    self,
    config_path: Optional[str] = None,
    flow_overrides: Optional[Dict[str, Any]] = None,
    flow_filter: Optional[List[str]] = None,  # NEW
):
    ...
    self.flow_filter = flow_filter or []  # List of flow names to execute

# In _create_flows()
def _create_flows(self) -> None:
    """Create Flow instances from configuration."""
    self.flows = []

    for base_flow_name, flow_config in self.config.flows.items():
        # If filter specified, skip flows not in filter
        if self.flow_filter:
            # Check if base flow name matches OR any entity flow would match
            if not self._should_include_flow(base_flow_name, flow_config):
                continue
        # ... rest of existing logic
```

### Phase 2: Multiple Flow Execution (Future)

- Support multiple `--flow` flags: `hygge go --flow flow1 --flow flow2`
- Or comma-separated: `hygge go --flow flow1,flow2`

## Related Context

- This is separate from journal work but complements it
- Journal tracks failures, but manual retry requires this CLI feature
- See `JOURNAL_NEXT_STEPS.md` "Failed Flow Retry Strategy" section for discussion
- Future enhancement: Automatic retry (opt-in) would build on this foundation

## Testing Considerations

- Test exact flow name matching
- Test entity flow name matching (e.g., `salesforce_Involvement`)
- Test base flow name matching (e.g., `salesforce` runs all entities)
- Test with flow that doesn't exist (should error gracefully)
- Test with no flows matching filter (should error gracefully)
- Test integration with existing flow_overrides functionality

## Priority

**High** - This blocks practical manual retry workflow and is already partially documented in README as "coming soon".
