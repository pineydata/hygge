# Coordinator Refactoring: Extract Complex Responsibilities

## Problem

The `Coordinator` class has grown to 1,341 lines and handles too many concerns:

- Configuration loading (project, directory, single-file, legacy modes)
- Entity flow creation with complex path merging and config merging
- Connection pool management
- Journal instantiation and caching
- Flow orchestration with progress tracking
- Hygge-style summary generation (inspired by dbt)

This violates the KISS principle and makes the codebase harder to maintain, test, and extend.

## Current Behavior

All coordinator logic is in a single class:
- `_load_config()` handles multiple config loading patterns (240+ lines across related methods)
- `_create_entity_flow()` handles entity-specific flow creation with deep merging (170+ lines)
- `_run_flows()` handles orchestration, progress tracking, and summary generation (200+ lines)
- Configuration merging logic is scattered across multiple methods

## Use Cases

1. **Easier Testing**: Isolated components can be tested independently
2. **Better Maintainability**: Changes to config loading don't affect flow orchestration
3. **Clearer Responsibilities**: Each class has a single, well-defined purpose
4. **Future Extensibility**: New config sources or orchestration strategies are easier to add

## Proposed Solution

### Phase 1: Extract Workspace Discovery and Loading

Create a `Workspace` class that represents where you work with hygge.

**File Location:** `src/hygge/core/workspace.py` (new file)

Note: A workspace is where you work with hygge. It's where `hygge.yml` lives (that cozy marker that says "hygge lives here"), and where your flows live in the `flows/` directory. This is your cozy corner for data movement - where everything comes together comfortably.

```python
# In src/hygge/core/workspace.py
class Workspace:
    """Your workspace - where you work with hygge.

    This is your cozy corner for data movement. It knows where hygge.yml
    lives (that marker that says "hygge lives here"), where your flows
    live in flows/, and brings everything together comfortably. This is
    where you work with hygge - simple, comfortable, and clear.
    """

    @classmethod
    def find(cls, start_path: Optional[Path] = None) -> "Workspace":
        """Find your workspace by looking for hygge.yml.

        Walks up from start_path (or current directory) looking for
        hygge.yml - that cozy marker that says "this is your workspace".
        When found, returns the workspace. If not found, raises a friendly
        error with helpful suggestions.
        """
        if start_path is None:
            start_path = Path.cwd()

        current = Path(start_path).resolve()
        places_we_looked = []

        while current != current.parent:
            hygge_file = current / "hygge.yml"
            places_we_looked.append(str(hygge_file))
            if hygge_file.exists():
                # Found it! This is your workspace
                return cls.from_path(hygge_file)
            current = current.parent

        # Friendly, helpful error message
        error_msg = f"""
Couldn't find your workspace (looking for hygge.yml)

Looked in:
{chr(10).join(f"  - {path}" for path in places_we_looked)}

To create a new workspace, run:
  hygge init <name>
"""
        raise ConfigError(error_msg)

    @classmethod
    def from_path(cls, hygge_yml: Path) -> "Workspace":
        """Create a Workspace from where hygge.yml lives."""
        # Read hygge.yml to get workspace name and settings
        with open(hygge_yml, "r") as f:
            workspace_data = yaml.safe_load(f) or {}

        name = workspace_data.get("name", hygge_yml.parent.name)
        flows_dir = workspace_data.get("flows_dir", "flows")

        return cls(hygge_yml, name, flows_dir)

    def __init__(self, hygge_yml: Path, name: str, flows_dir: str = "flows"):
        """Create your workspace - where you work with hygge."""
        self.hygge_yml = hygge_yml
        self.root = hygge_yml.parent
        self.name = name
        self.flows_dir = flows_dir
        self.flows_path = self.root / flows_dir
        self.config: Dict[str, Any] = {}
        self.connections: Dict[str, Any] = {}
        self.options: Dict[str, Any] = {}

    def prepare(self) -> CoordinatorConfig:
        """Prepare your workspace - bring your flows together and get ready to run.

        Reads hygge.yml, finds all flows where they live in flows/,
        and brings everything together. This is where your workspace
        comes together and gets ready to move some data comfortably.
        """
        # Read your workspace config
        self._read_workspace_config()

        # Find all flows where they live
        flows = self._find_flows()

        # Everything's ready to go
        return CoordinatorConfig(
            flows=flows,
            connections=self.connections,
            journal=self.config.get("journal"),
        )

    def _read_workspace_config(self) -> None:
        """Read your workspace configuration from hygge.yml."""
        with open(self.hygge_yml, "r") as f:
            self.config = yaml.safe_load(f) or {}

        # Expand environment variables
        self.config = self._expand_env_vars(self.config)

        # Extract connections and options
        self.connections = self.config.get("connections", {})
        self.options = self.config.get("options", {})

    def _find_flows(self) -> Dict[str, FlowConfig]:
        """Find all flows where they live in flows/.

        Looks through flows/ directory for flow directories (each
        containing a flow.yml), and brings them all together. This
        is where we find your flows and bring them home.
        """
        if not self.flows_path.exists():
            raise ConfigError(f"Flows directory not found: {self.flows_path}")

        flows = {}

        # Look through each directory in flows/
        for flow_dir in self.flows_path.iterdir():
            if flow_dir.is_dir() and (flow_dir / "flow.yml").exists():
                flow_name = flow_dir.name
                try:
                    # Read this flow from where it lives
                    flow_config = self._read_flow_config(flow_dir)
                    flows[flow_name] = flow_config
                except Exception as e:
                    raise ConfigError(
                        f"Couldn't read flow '{flow_name}': {str(e)}"
                    )

        if not flows:
            raise ConfigError(
                f"No flows found in {self.flows_path}. "
                f"Create a flow directory with a flow.yml file to get started."
            )

        return flows

    def _read_flow_config(self, flow_dir: Path) -> FlowConfig:
        """Read a flow configuration from where it lives."""
        flow_file = flow_dir / "flow.yml"
        with open(flow_file, "r") as f:
            flow_data = yaml.safe_load(f)

        # Expand environment variables
        flow_data = self._expand_env_vars(flow_data)

        # Find entities if they exist
        entities_dir = flow_dir / "entities"
        if entities_dir.exists():
            defaults = flow_data.get("defaults", {})
            entities = self._read_entities(entities_dir, defaults)
            flow_data["entities"] = entities

        return FlowConfig(**flow_data)

    def _read_entities(self, entities_dir: Path, defaults: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Read entity definitions from where they live."""
        entities = []

        for entity_file in entities_dir.glob("*.yml"):
            with open(entity_file, "r") as f:
                entity_data = yaml.safe_load(f)

            # Expand environment variables
            entity_data = self._expand_env_vars(entity_data)

            # Merge with defaults
            entity_data = {**defaults, **entity_data}
            entities.append(entity_data)

        return entities

    def _expand_env_vars(self, data: Any) -> Any:
        """Expand environment variables in configuration data."""
        # (Implementation from Coordinator)
        ...
```

**Benefits:**
- Feels natural - a workspace is where you work, simple and clear
- Not enterprise-y - it's your workspace, not a "project" or formal structure
- Natural, warm language - "find your workspace", "where you work with hygge"
- Represents your cozy corner for data movement - where everything comes together
- Friendly error messages that help instead of just reporting problems
- Discovery feels like finding your space, not loading config
- The code reads like hygge - comfortable, inviting, and clear

### Phase 2: Extract Flow Creation Logic

Extract flow creation logic to class methods on `Flow`. Flows already exist in your workspace (defined in flow.yml files), so these methods bring them to life from their configuration, making them ready to run.

**File Location:** `src/hygge/core/flow.py` (existing file - add class methods to Flow)

**Decision:** Use class methods on `Flow` rather than a separate builder class. Flows know how to create themselves from their configuration - it's about flows coming to life, not "building" them.

**Methods to add to Flow class:**

- `Flow.from_config()` - Brings a flow to life from its FlowConfig, creating the Home and Store, wiring up journals and connection pools, and making everything ready to run.
- `Flow.from_entity()` - Brings an entity flow to life, merging entity configuration with flow configuration, and making everything ready to run.

**Helper methods (private):**

- `_merge_entity_config()` - Merges entity configuration with flow configuration
- `_get_journal()` - Gets or creates journal instance
- `_apply_overrides()` - Applies overrides to flow configuration

**Benefits:**
- Natural language: `Flow.from_config()`, `Flow.from_entity()`
- No separate builder class - flows know how to create themselves
- Keeps Flow-related logic together in `flow.py`

### Phase 3: Extract Progress

Extract progress tracking to a `Progress` class that handles coordinator-level progress tracking.

**File Location:** `src/hygge/logging/progress.py` (new file)

Note: Progress is an observability utility that supports Coordinator, similar to `logger.py`. It handles progress tracking and milestone logging. Lives in the `logging/` submodule alongside other observability utilities.

```python
# In src/hygge/logging/progress.py
class Progress:
    """Tracks progress across multiple flows."""

    def __init__(self, milestone_interval: int = 1_000_000):
        self.total_rows_progress = 0
        self.last_milestone_rows = 0
        self.milestone_interval = milestone_interval
        self.milestone_lock = asyncio.Lock()

    async def update(self, rows: int) -> None:
        """Update progress and log milestones."""
        ...
```

**Benefits:**
- Separates progress tracking from orchestration
- Easier to test milestone logic
- Can be extended for different progress reporting strategies
- Lives in `logging/` submodule alongside other observability utilities

### Phase 4: Extract Summary

Extract summary generation to a `Summary` class that creates hygge-style execution summaries.

**File Location:** `src/hygge/logging/summary.py` (new file)

Note: Summary is an observability utility that formats and logs execution summaries, similar to `logger.py`. It supports Coordinator's reporting needs. Lives in the `logging/` submodule alongside other observability utilities.

**Scope consideration:** While dbt provides good inspiration for execution summaries, this should be distinctly hygge. As part of this work, we should answer: **What makes a summary feel hygge?** What language, formatting, and presentation style reflects hygge's values of comfort, clarity, and natural flow? This is about making summaries that feel cozy and helpful, not just informative.

```python
# In src/hygge/logging/summary.py
class Summary:
    """Generates hygge-style execution summaries."""

    def generate_summary(
        self,
        flow_results: List[Dict[str, Any]],
        start_time: float,
        logger: Logger,
    ) -> None:
        """Generate and log execution summary."""
        ...
```

**Benefits:**
- Separates summary generation from orchestration
- Easier to test summary formatting
- Can be extended for different summary formats
- Lives in `logging/` submodule alongside other observability utilities
- Opportunity to create hygge-specific summary style that reflects comfort and clarity

## File Locations

### New Submodule: `logging/`
Create a new `src/hygge/logging/` submodule for observability utilities:
- **Logger**: `src/hygge/logging/logger.py` (move from `utility/logger.py`)
  - Human-readable output formatting
  - Color formatting and console/file handlers
- **Progress**: `src/hygge/logging/progress.py` (new file)
  - Progress tracking and milestone logging
  - Coordinator-level row counting
- **Summary**: `src/hygge/logging/summary.py` (new file)
  - Hygge-style execution summary formatting (inspired by dbt but distinctly hygge)
  - Results aggregation and formatting

**Rationale:** These three components all handle observability/output formatting. Grouping them in a `logging/` submodule makes their purpose clear and keeps observability concerns together.

### New Files
- **Workspace**: `src/hygge/core/workspace.py` (new file)
  - Your workspace - where you work with hygge
  - `Workspace.find()` - finds your workspace (looks for hygge.yml)
  - `workspace.prepare()` - brings everything together, finds flows where they live
  - Feels natural: it's your workspace, where you work with hygge

### Existing Files (Add Methods)
- **Flow**: `src/hygge/core/flow.py` (existing file - add class methods)
  - `Flow.from_config()` - brings a flow to life from its configuration
  - `Flow.from_entity()` - brings an entity flow to life
  - No separate builder class - flows know how to create themselves

### Existing Files (Move/Refactor)
- **Logger**: `src/hygge/logging/logger.py` (move from `utility/logger.py`)
  - Move logger.py to new logging submodule
  - Update imports throughout codebase
- **Coordinator**: `src/hygge/core/coordinator.py` (existing file - refactor)
  - Replace config loading methods with `Workspace.find()` and `workspace.prepare()` - feels natural
  - Replace flow creation methods with `Flow.from_config()` and `Flow.from_entity()` - flows come to life
  - Use extracted classes (Progress, Summary)
  - Update imports to use new logging submodule

### Tests
- `tests/unit/hygge/core/test_workspace.py` (new file)
  - Test `Workspace.find()` - finding your workspace
  - Test `workspace.prepare()` - finding flows where they live
  - Test legacy config loading patterns
- `tests/unit/hygge/core/test_flow.py` (existing file - extend with Flow.from_config() and Flow.from_entity() tests)
- `tests/unit/hygge/logging/test_progress.py` (new file)
- `tests/unit/hygge/logging/test_summary.py` (new file)
- `tests/unit/hygge/logging/test_logger.py` (move from `tests/unit/hygge/utility/test_logger.py` if it exists)

### Note on Journal
**Journal stays in `core/`** - It's conceptually different from logging utilities:
- Journal persists structured metadata to storage (parquet files) for operational use
- It's used for watermarks, run history, and incremental processing
- It's a "metadata Store" - a first-class abstraction parallel to Home/Store
- It's not about human-readable output, it's about durable metadata storage

## Implementation Plan

1. **Extract Workspace class** (highest impact, lowest risk)
   - Create `src/hygge/core/workspace.py` with `Workspace` class
   - Move workspace discovery (`_find_project_config`) to `Workspace.find()` - finds your workspace
   - Move workspace config loading (`_load_project_config`) to `Workspace._read_workspace_config()` - reads hygge.yml
   - Move flow discovery (`_load_project_flows`, `_load_flow_config`, `_load_entities`) to `Workspace._find_flows()` - finds flows where they live
   - Move legacy config loading (`_load_single_file_config`, `_load_directory_config`) to Workspace methods
   - Update Coordinator to use `Workspace.find()` and `workspace.prepare()` - feels natural
   - Add unit tests in `tests/unit/hygge/core/test_workspace.py`
   - Verify existing tests still pass

2. **Extract flow creation to Flow class methods** (high impact, medium risk)
   - Add `Flow.from_config()` class method to `src/hygge/core/flow.py`
   - Add `Flow.from_entity()` class method to `src/hygge/core/flow.py`
   - Add helper methods: `_merge_entity_config()`, `_get_journal()`, `_apply_overrides()`
   - Move flow creation logic from Coordinator to Flow class methods
   - Update Coordinator to use `Flow.from_config()` and `Flow.from_entity()` - feels natural
   - Add unit tests in `tests/unit/hygge/core/test_flow.py` (extend existing tests)
   - Verify existing tests still pass

3. **Create logging submodule and move/extract components** (medium impact, medium risk)
   - Create `src/hygge/logging/` directory and `__init__.py`
   - Move `src/hygge/utility/logger.py` to `src/hygge/logging/logger.py`
   - Create `src/hygge/logging/progress.py`
   - Create `src/hygge/logging/summary.py`
   - Move progress tracking from Coordinator to Progress
   - Move summary generation from Coordinator to Summary
   - **Define hygge summary style** - What makes a summary feel hygge? (See Phase 4 scope consideration)
   - Update all imports throughout codebase to use new logging submodule
   - Add unit tests in `tests/unit/hygge/logging/`
   - Verify existing tests still pass

4. **Update Coordinator to use logging submodule** (low impact, low risk)
   - Update Coordinator imports to use logging submodule
   - Update Coordinator to use Progress and Summary
   - Verify existing tests still pass

## Testing Considerations

- Unit tests for each extracted class
- Integration tests to verify Coordinator still works correctly
- Ensure existing tests continue to pass
- Add tests for edge cases in config merging

## Related Issues

- See `cli-simplification.md` for related CLI refactoring
- See `watermark-tracker-extraction.md` for related Flow refactoring

## Priority

**High** - This is foundational work that will make the codebase more maintainable and easier to extend. The Coordinator class is already complex and will only get worse as features are added.
