# Workspace Extraction Checklist

**Related Issues:**
- `issues/coordinator-refactoring.md` - Phase 1: Extract Workspace Discovery and Loading
- `issues/cli-simplification.md` - Phase 1: Move Run Type Override Logic to Coordinator, Phase 3: Simplify CLI go command

## Phase 1: Extract Workspace (Foundation)
**See:** `issues/coordinator-refactoring.md` Phase 1

- [ ] Create `src/hygge/core/workspace.py`
- [ ] Implement `Workspace.find()` - finds hygge.yml by walking up directories
- [ ] Implement `Workspace.from_path()` - creates Workspace from hygge.yml path
- [ ] Implement `Workspace._read_workspace_config()` - reads hygge.yml
- [ ] Implement `Workspace._find_flows()` - finds flows in flows/ directory
- [ ] Implement `Workspace._read_flow_config()` - reads individual flow.yml
- [ ] Implement `Workspace._read_entities()` - reads entity definitions
- [ ] Implement `Workspace._expand_env_vars()` - expands environment variables
- [ ] Implement `Workspace.prepare()` - returns CoordinatorConfig with flows

**Testing Phase 1:**
- [ ] Add unit tests in `tests/unit/hygge/core/test_workspace.py`
- [ ] Test `Workspace.find()` - finding hygge.yml (various directory structures)
- [ ] Test `Workspace.prepare()` - loading flows successfully
- [ ] Test `Workspace._find_flows()` - finding flows in flows/ directory
- [ ] Test `Workspace._read_flow_config()` - reading individual flow.yml
- [ ] Test `Workspace._read_entities()` - reading entity definitions
- [ ] Test edge cases (no hygge.yml, no flows, invalid configs)
- [ ] Verify Workspace can discover and load existing project configs
- [ ] Verify existing tests still pass

## Phase 2: Update CLI to Use Workspace (Remove Temp Coordinator)
**See:** `issues/cli-simplification.md` Phase 1 & Phase 3

- [ ] Update CLI `go` command to use `Workspace.find()` instead of temp coordinator
- [ ] Replace `temp_coordinator._load_config()` with `workspace.prepare()`
- [ ] Use `config.flows` from Workspace for run_type override logic (lines 333, 338, 341)
- [ ] Remove temp coordinator creation (lines 303-309 in cli.py)
- [ ] Simplify run_type override logic to use workspace config

**Testing Phase 2:**
- [ ] Update CLI tests to verify Workspace integration
- [ ] Test flow filtering (`--flow`, `--entity`) works with Workspace
- [ ] Test run_type overrides (`--incremental`, `--full-drop`) work with Workspace
- [ ] Test flow overrides (`--var`) work with Workspace
- [ ] Test combinations of options (flow filter + run_type, etc.)
- [ ] Test edge cases (invalid flow names, no matching flows, etc.)
- [ ] Integration test: CLI → Workspace → Coordinator flow
- [ ] Verify existing CLI tests still pass

## Phase 3: Refactor Coordinator to Use Workspace (Cleanup)
**See:** `issues/coordinator-refactoring.md` Phase 1 (cleanup after CLI integration)

- [ ] Update `Coordinator.__init__()` to accept `CoordinatorConfig` or use `Workspace.prepare()`
- [ ] Move `_find_project_config()` logic to Workspace (delete from Coordinator)
- [ ] Move `_load_project_config()` logic to Workspace (delete from Coordinator)
- [ ] Move `_load_project_flows()` logic to Workspace (delete from Coordinator)
- [ ] Move `_load_flow_config()` logic to Workspace (delete from Coordinator)
- [ ] Move `_load_entities()` logic to Workspace (delete from Coordinator)
- [ ] Move `_expand_env_vars()` logic to Workspace (delete from Coordinator)
- [ ] Remove `_load_config()` method from Coordinator (now uses Workspace)
- [ ] Update Coordinator tests to use Workspace
- [ ] Update imports throughout codebase if needed

**Testing Phase 3:**
- [ ] Test Coordinator uses Workspace for config loading
- [ ] Verify all existing Coordinator functionality still works
- [ ] Test Coordinator still works with flow_filter and flow_overrides
- [ ] Test Coordinator still creates flows correctly
- [ ] Test Coordinator still runs flows correctly
- [ ] Integration test: Workspace → Coordinator → Flow execution
- [ ] Verify existing Coordinator tests still pass

## Phase 4: Handle Legacy Config Patterns (If Needed)
**See:** `issues/coordinator-refactoring.md` Phase 1 (legacy config loading)

- [ ] Move `_load_single_file_config()` logic to Workspace (if still needed)
- [ ] Move `_load_directory_config()` logic to Workspace (if still needed)
- [ ] Document which config patterns are supported

**Testing Phase 4:**
- [ ] Test legacy single-file config pattern still works
- [ ] Test legacy directory config pattern still works
- [ ] Verify backward compatibility with existing configs
- [ ] Verify existing tests still pass

## Verification (After All Phases)

- [ ] Flow filtering (`--flow`, `--entity`) works end-to-end
- [ ] Run type overrides (`--incremental`, `--full-drop`) work end-to-end
- [ ] Flow overrides (`--var`) work end-to-end
- [ ] No temp coordinator hack in CLI
- [ ] Coordinator is simpler (fewer methods, uses Workspace)
- [ ] All existing functionality preserved
- [ ] Documentation updated if needed
