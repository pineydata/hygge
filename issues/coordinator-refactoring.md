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

**File Location:** `src/hygge/messages/progress.py` (new file)

Note: Progress is a messaging utility that supports Coordinator, similar to `logger.py`. It handles progress tracking and milestone messages. Lives in the `messages/` submodule alongside other messaging utilities.

```python
# In src/hygge/messages/progress.py
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
- Lives in `messages/` submodule alongside other messaging utilities

### Phase 4: Extract Summary

Extract summary generation to a `Summary` class that creates hygge-style execution summaries.

**File Location:** `src/hygge/messages/summary.py` (new file)

Note: Summary is a messaging utility that formats and logs execution summaries, similar to `logger.py`. It supports Coordinator's reporting needs. Lives in the `messages/` submodule alongside other messaging utilities.

**Scope consideration:** While dbt provides good inspiration for execution summaries, this should be distinctly hygge. As part of this work, we should answer: **What makes a summary feel hygge?** What language, formatting, and presentation style reflects hygge's values of comfort, clarity, and natural flow? This is about making summaries that feel cozy and helpful, not just informative.

```python
# In src/hygge/messages/summary.py
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
- Lives in `messages/` submodule alongside other messaging utilities
- Opportunity to create hygge-specific summary style that reflects comfort and clarity

## File Locations

### New Submodule: `messages/`
Create a new `src/hygge/messages/` submodule for messaging utilities:
- **Logger**: `src/hygge/messages/logger.py` (move from `utility/logger.py`)
  - Human-readable output formatting
  - Color formatting and console/file handlers
- **Progress**: `src/hygge/messages/progress.py` (new file)
  - Progress tracking and milestone messages
  - Coordinator-level row counting
- **Summary**: `src/hygge/messages/summary.py` (new file)
  - Hygge-style execution summary formatting (inspired by dbt but distinctly hygge)
  - Results aggregation and formatting

**Rationale:** These three components all handle messaging/output formatting. Grouping them in a `messages/` submodule makes their purpose clear, avoids namespace collision with Python's `logging` module, and keeps messaging concerns together. The name "messages" feels more hygge - warm, clear, and natural.

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
- **Logger**: `src/hygge/messages/logger.py` (move from `utility/logger.py`)
  - Move logger.py to new messages submodule
  - Update imports throughout codebase
- **Coordinator**: `src/hygge/core/coordinator.py` (existing file - refactor)
  - Replace config loading methods with `Workspace.find()` and `workspace.prepare()` - feels natural
  - Replace flow creation methods with `Flow.from_config()` and `Flow.from_entity()` - flows come to life
  - Use extracted classes (Progress, Summary)
  - Update imports to use new messages submodule

### Tests
- `tests/unit/hygge/core/test_workspace.py` (new file)
  - Test `Workspace.find()` - finding your workspace
  - Test `workspace.prepare()` - finding flows where they live
  - Test legacy config loading patterns
- `tests/unit/hygge/core/test_flow.py` (existing file - extend with Flow.from_config() and Flow.from_entity() tests)
- `tests/unit/hygge/messages/test_progress.py` (new file)
- `tests/unit/hygge/messages/test_summary.py` (new file)

### Note on Journal
**Journal stays in `core/`** - It's conceptually different from messaging utilities:
- Journal persists structured metadata to storage (parquet files) for operational use
- It's used for watermarks, run history, and incremental processing
- It's a "metadata Store" - a first-class abstraction parallel to Home/Store
- It's not about human-readable output, it's about durable metadata storage

## Implementation Plan

### ✅ Phase 1: Extract Workspace class (COMPLETED & STABLE)

**Status:** ✅ Complete & Stable - All workspace extraction completed, tested, and verified

**Completed Work:**
- ✅ Created `src/hygge/core/workspace.py` with `Workspace` class
- ✅ Implemented `Workspace.find()` - finds workspace by locating `hygge.yml`
- ✅ Implemented `Workspace.from_path()` - creates workspace from explicit path
- ✅ Implemented `_read_workspace_config()` - reads `hygge.yml` and expands env vars
- ✅ Implemented `_find_flows()` - discovers flows in `flows/` directory
- ✅ Implemented `_read_flow_config()` - loads individual `flow.yml` files
- ✅ Implemented `_read_entities()` - loads entity definitions from `entities/` subdirectories
- ✅ Implemented `_expand_env_vars()` - expands `${VAR_NAME}` and `${VAR_NAME:-default}` patterns
- ✅ Implemented `prepare()` - returns `WorkspaceConfig` with all flows loaded
- ✅ Removed all legacy config loading patterns (single-file, directory-based)
- ✅ Updated Coordinator to use `Workspace` for all config loading
- ✅ Updated CLI to use `Workspace.find()` directly (removed temp coordinator hack)
- ✅ Added comprehensive unit tests in `tests/unit/hygge/core/test_workspace.py`
- ✅ Converted all Coordinator tests to workspace pattern
- ✅ Converted all integration tests to workspace pattern
- ✅ All tests passing (511 tests)

**Result:** Coordinator simplified by removing 10+ configuration methods. Workspace pattern (`hygge.yml` + `flows/`) is now the only supported configuration approach. Clear separation: Workspace handles configuration, Coordinator handles orchestration.

**Stability Status:** ✅ **STABLE** - All tests passing. Ready for Phase 2.

### 🔄 Phase 2: Extract Flow Creation Logic (READY TO START)

**Status:** 📋 Planned - Not started yet

**Future Work:**
- Extract flow creation logic to class methods on `Flow`
- Add `Flow.from_config()` class method to `src/hygge/core/flow.py`
- Add `Flow.from_entity()` class method to `src/hygge/core/flow.py`
- Add helper methods: `_merge_entity_config()`, `_get_journal()`, `_apply_overrides()`
- Move flow creation logic from Coordinator to Flow class methods
- Update Coordinator to use `Flow.from_config()` and `Flow.from_entity()`
- Add unit tests in `tests/unit/hygge/core/test_flow.py`

**Status:** 📋 Ready to start - Phase 1 is stable. Current flow creation in Coordinator works well, but extraction would further simplify Coordinator.

### ✅ Phase 3: Extract Progress Tracking (COMPLETED & STABLE)

**Status:** ✅ Complete & Stable - All progress tracking extraction completed, tested, and verified

**Completed Work:**
- ✅ Created `src/hygge/messages/progress.py` with `Progress` class
- ✅ Extracted progress tracking logic from Coordinator
- ✅ Implemented milestone tracking with configurable interval (default: 1M rows)
- ✅ Updated Coordinator to use `Progress` class
- ✅ Added comprehensive unit tests in `tests/unit/hygge/messages/test_progress.py`
- ✅ All tests passing

**Result:** Coordinator simplified by removing progress tracking implementation. Progress class handles milestone messages with thread-safe updates. Clear separation: Progress handles messaging, Coordinator handles orchestration.

### ✅ Phase 4: Extract Summary Generation (COMPLETED & STABLE)

**Status:** ✅ Complete & Stable - All summary generation extraction completed, tested, and verified

**Completed Work:**
- ✅ Created `src/hygge/messages/summary.py` with `Summary` class
- ✅ Defined hygge summary style - cozy, clear, and helpful (inspired by dbt but distinctly hygge)
- ✅ Extracted summary generation logic from Coordinator
- ✅ Implemented hygge-style formatting with comfortable spacing and natural language
- ✅ Updated Coordinator to use `Summary` class
- ✅ Added comprehensive unit tests in `tests/unit/hygge/messages/test_summary.py`
- ✅ All tests passing

**Hygge Summary Style:** Summaries feel cozy and helpful, not just informative. They use natural language ("Finished running X flows in Y time"), comfortable spacing, and clear status indicators. Reflects hygge's values of comfort, clarity, and natural flow.

**Result:** Coordinator simplified by removing summary generation implementation. Summary class handles execution summaries with hygge-style formatting. Clear separation: Summary handles messaging, Coordinator handles orchestration.

### ✅ Phase 5: Create Messages Submodule (COMPLETED & STABLE)

**Status:** ✅ Complete & Stable - Messages submodule created, consolidated, and verified

**Completed Work:**
- ✅ Created `src/hygge/messages/` directory and `__init__.py`
- ✅ Moved `src/hygge/utility/logger.py` to `src/hygge/messages/logger.py`
- ✅ Consolidated Progress and Summary into messages submodule
- ✅ Updated all 13 imports throughout codebase to use new messages submodule
- ✅ Renamed submodule from `logging` to `messages` to avoid namespace collision with Python's built-in `logging` module
- ✅ Removed old `src/hygge/utility/logger.py` file
- ✅ All tests passing (570 tests)

**Result:** Clean separation of concerns with all messaging utilities in `messages/` submodule:
- `logger.py` - Human-readable output formatting with colors
- `progress.py` - Progress tracking and milestone messages
- `summary.py` - Hygge-style execution summary formatting

All three components handle messaging/output formatting and live together in a cohesive submodule. The name "messages" is warmer and clearer than "logging", and avoids namespace collision. Clear separation from core data movement logic.

## Testing Considerations

- Unit tests for each extracted class
- Integration tests to verify Coordinator still works correctly
- Ensure existing tests continue to pass
- Add tests for edge cases in config merging

## Related Issues

- See `cli-simplification.md` for related CLI refactoring
- See `watermark-tracker-extraction.md` for related Flow refactoring

## Priority

**✅ Phase 1 Complete & Stable** - Workspace extraction completed, tested, and verified. All tests passing. Coordinator simplified by removing 10+ configuration methods. Clear separation: Workspace handles configuration, Coordinator handles orchestration.

**✅ Phases 3-5 Complete & Stable** - Progress tracking, summary generation, and messages submodule extraction completed, tested, and verified. All 570 tests passing. Coordinator simplified by removing progress tracking and summary generation implementations. Clear separation: Messages utilities handle output formatting, Coordinator handles orchestration. Submodule renamed to `messages/` to avoid namespace collision with Python's built-in `logging` module - feels more hygge too!

**🔄 Phase 2:** **Ready to Start** - Flow creation logic extraction is ready to proceed. Current flow creation in Coordinator works well, but extraction would further simplify Coordinator.
