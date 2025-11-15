# CLI Simplification: Separate Parsing from Orchestration

## Problem

The `go` command in `cli.py` has convoluted logic that mixes CLI parsing with orchestration:

- Creates a temporary coordinator to determine base flow names (for run_type overrides)
- Applies run_type overrides after parsing (should be in Coordinator)
- Flow filter logic is embedded in CLI (should be extracted to helper functions)
- Complex flow override parsing that should be in Coordinator
- The `--flow` option parsing is implemented but could be cleaner

This makes the CLI harder to maintain and test, and violates separation of concerns.

## Current Behavior

The CLI currently:
1. Parses `--flow` and `--entity` options to build `flow_filter` (implemented)
2. Parses `--var` options to build `flow_overrides` (implemented)
3. Creates a temporary coordinator to determine base flow names for run_type overrides (complex)
4. Applies run_type overrides after parsing (should be in Coordinator)
5. Creates actual coordinator and runs flows

The flow filtering is already implemented in Coordinator (`_should_include_flow()`), but the CLI parsing and run_type override logic is overly complex.

## Use Cases

1. **Manual Retry After Failure**: After a flow fails, retry just that flow:
   ```bash
   hygge go --flow salesforce_Involvement
   ```

2. **Selective Execution**: Run only specific flows during development/testing:
   ```bash
   hygge go --flow flow1 --flow flow2
   ```

3. **Run Type Overrides**: Apply run_type to all flows or specific flows:
   ```bash
   hygge go --incremental  # All flows in incremental mode
   hygge go --flow salesforce --incremental  # Just salesforce in incremental mode
   ```

4. **Simpler CLI**: CLI should only parse arguments and pass structured options to Coordinator
5. **Better Testability**: CLI parsing logic can be tested independently
6. **Clearer Responsibilities**: Coordinator handles orchestration, CLI handles parsing

## Proposed Solution

### Phase 1: Move Run Type Override Logic to Coordinator

The Coordinator should handle run_type overrides directly via a `default_run_type` parameter:

```python
# In Coordinator.__init__()
def __init__(
    self,
    config_path: Optional[str] = None,
    flow_overrides: Optional[Dict[str, Any]] = None,
    flow_filter: Optional[List[str]] = None,
    default_run_type: Optional[str] = None,  # NEW: Coordinator-level default
):
    ...
    self.default_run_type = default_run_type
```

```python
# In Coordinator._create_flows() or when using Flow.from_config()
# Apply default_run_type if specified and not overridden
if self.default_run_type:
    if base_flow_name not in self.flow_overrides:
        self.flow_overrides[base_flow_name] = {}
    if "run_type" not in self.flow_overrides[base_flow_name]:
        self.flow_overrides[base_flow_name]["run_type"] = self.default_run_type
```

### Phase 2: Extract Parsing Helper Functions

Move complex parsing logic to helper functions for better testability and clarity.

**File Location:** Update `src/hygge/cli.py` (existing file) - add helper functions at module level

```python
# In src/hygge/cli.py (add at module level, before @hygge.command())
def _parse_flow_filter(flow: tuple, entity: tuple) -> List[str]:
    """Parse flow filter from CLI arguments.

    Supports:
    - Base flow names: --flow salesforce
    - Entity flow names: --flow salesforce_Involvement
    - Comma-separated: --flow flow1,flow2
    - Entity format: --entity salesforce.Involvement
    """
    flow_filter = []

    # Parse --flow options (can be comma-separated)
    for flow_str in flow:
        flows = [f.strip() for f in flow_str.split(",") if f.strip()]
        flow_filter.extend(flows)

    # Parse --entity options (format: flow.entity)
    for entity_str in entity:
        entities = [e.strip() for e in entity_str.split(",") if e.strip()]
        for entity_spec in entities:
            if "." not in entity_spec:
                raise click.BadParameter(
                    f"Invalid --entity format: {entity_spec}. "
                    f"Use flow.entity (e.g., salesforce.Involvement)"
                )
            flow_name, entity_name = entity_spec.split(".", 1)
            entity_flow_name = f"{flow_name}_{entity_name}"
            flow_filter.append(entity_flow_name)

    return flow_filter


def _parse_flow_overrides(var: tuple) -> Dict[str, Any]:
    """Parse flow overrides from CLI --var options.

    Format: flow.<flow_name>.field=value
    Example: --var flow.salesforce.run_type=incremental
    """
    flow_overrides = {}

    for var_str in var:
        if "=" not in var_str:
            raise click.BadParameter(
                f"Invalid --var format: {var_str}. "
                f"Use flow.<flow_name>.field=value"
            )

        key, value = var_str.split("=", 1)
        key_parts = key.split(".")

        if len(key_parts) < 3 or key_parts[0] != "flow":
            raise click.BadParameter(
                "--var must start with 'flow.<flow_name>.'"
            )

        flow_name = key_parts[1]
        field_parts = key_parts[2:]

        if flow_name not in flow_overrides:
            flow_overrides[flow_name] = {}

        # Build nested dict
        current = flow_overrides[flow_name]
        for part in field_parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        current[field_parts[-1]] = _parse_var_value(value)

    return flow_overrides


def _parse_var_value(value: str) -> Any:
    """Parse a variable value, handling booleans, numbers, and strings."""
    # Try boolean
    if value.lower() in ("true", "false"):
        return value.lower() == "true"

    # Try number
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        pass

    # Default to string
    return value
```

### Phase 3: Simplify CLI go Command

The CLI should only parse arguments and pass structured options to Coordinator:

```python
@hygge.command()
@click.option("--flow", "-f", multiple=True, help="...")
@click.option("--entity", "-e", multiple=True, help="...")
@click.option("--incremental", "-i", is_flag=True, help="...")
@click.option("--full-drop", "-d", is_flag=True, help="...")
@click.option("--var", multiple=True, help="...")
@click.option("--concurrency", "-c", type=int, help="...")
def go(flow: tuple, entity: tuple, incremental: bool, full_drop: bool, var: tuple, concurrency: Optional[int], ...):
    """Execute flows in the current workspace."""
    logger = get_logger("hygge.cli.go")

    # Validate flags
    if incremental and full_drop:
        click.echo("Error: Cannot specify both --incremental and --full-drop")
        sys.exit(1)

    # Parse flow filter
    flow_filter = _parse_flow_filter(flow, entity) if (flow or entity) else None

    # Parse flow overrides
    flow_overrides = _parse_flow_overrides(var) if var else None

    # Determine default run_type
    if incremental:
        default_run_type = "incremental"
    elif full_drop:
        default_run_type = "full_drop"
    else:
        default_run_type = None

    # Create coordinator with parsed options
    # Note: After coordinator-refactoring, this will use Workspace.find()
    coordinator = Coordinator(
        flow_overrides=flow_overrides,
        flow_filter=flow_filter,
        default_run_type=default_run_type,
    )

    # Apply concurrency override if provided
    if concurrency is not None:
        coordinator.options["concurrency"] = concurrency

    # Run flows
    try:
        asyncio.run(coordinator.run())
        if flow_filter:
            flow_list = ", ".join(flow_filter)
            click.echo(f"Completed flows: {flow_list}")
        else:
            click.echo("All flows completed successfully!")
    except ConfigError as e:
        click.echo(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}")
        logger.error(f"Error running flows: {e}")
        sys.exit(1)
```

## File Locations

- **CLI Helpers**: `src/hygge/cli.py` (existing file - add helper functions)
- **Coordinator**: `src/hygge/core/coordinator.py` (existing file - add default_run_type parameter)
- **Tests**: `tests/unit/hygge/test_cli.py` (existing file - extend tests)

## Implementation Plan

1. **Move run_type override logic to Coordinator** (highest impact)
   - Update `src/hygge/core/coordinator.py` to add `default_run_type` parameter
   - Apply default_run_type in flow creation (when using Flow.from_config() or in _create_flows())
   - Remove temporary coordinator creation from CLI
   - Update `src/hygge/cli.py` to pass default_run_type instead of applying overrides

2. **Extract parsing helper functions** (medium impact)
   - Add `_parse_flow_filter()` helper function to `src/hygge/cli.py`
   - Add `_parse_flow_overrides()` helper function to `src/hygge/cli.py`
   - Add `_parse_var_value()` helper function to `src/hygge/cli.py`
   - Add unit tests for helper functions in `tests/unit/hygge/test_cli.py`

3. **Simplify CLI go command** (medium impact)
   - Update `src/hygge/cli.py` go command to remove temporary coordinator creation
   - Use helper functions for parsing
   - Pass structured options to Coordinator
   - Remove complex run_type override logic

## Testing Considerations

- Unit tests for parsing helper functions:
  - `_parse_flow_filter()` - test base flow names, entity flow names, comma-separated, entity format
  - `_parse_flow_overrides()` - test nested dict building, value parsing
  - `_parse_var_value()` - test boolean, number, string parsing
- Integration tests to verify CLI still works correctly:
  - Test flow filtering (exact matches, base flow matches, entity flow matches)
  - Test run_type overrides (--incremental, --full-drop)
  - Test flow overrides (--var)
  - Test combinations of options
- Test edge cases:
  - Empty filters
  - Invalid formats
  - Flow that doesn't exist (should error gracefully)
  - No flows matching filter (should error gracefully)
- Ensure existing CLI tests continue to pass

## Discovery: CLI Boundaries and Assumptions

As part of this work, we need to discover and validate CLI boundaries:

**See:** `ASSUMPTIONS.md` for detailed exploration of:
- Per-entity run type overrides (what's possible with Click?)
- Single-coordinator assumption (is this acceptable?)
- What syntax patterns feel natural for hygge?

**Discovery Tasks:**
1. Test Click's capabilities with different syntax patterns
2. Validate assumptions with real use cases
3. Document discovered boundaries
4. Make boundary decisions based on what's possible and what's needed

**Outcome:** Clear boundaries documented in `ASSUMPTIONS.md` and implemented in CLI.

## Related Issues

- See `coordinator-refactoring.md` for related Coordinator refactoring (Workspace, Flow class methods)
- See `ASSUMPTIONS.md` for boundary discovery and validation
- This work complements the coordinator refactoring by simplifying the CLI interface

## Priority

**High** - This will make the CLI easier to maintain and test, reduce coupling between CLI and Coordinator internals, enable manual flow execution workflows, and establish clear boundaries for CLI capabilities.
