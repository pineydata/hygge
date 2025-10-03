# hygge Testing Strategy

## Current State
- **⚠️ Core architecture tests** - Flow, Coordinator, Factory tests exist but need verification after refactor
- **⚠️ Configuration tests** - 81 tests exist but need verification after config consolidation
- **⚠️ Integration tests** - YAML config tests exist but need verification after refactor
- **⚠️ Error scenario tests** - Error tests exist but need verification after refactor
- **⚠️ Async testing infrastructure** - Timeout protection exists but needs verification
- **❌ Implementation-level tests** - ParquetHome, ParquetStore specific testing needed
- **❌ End-to-end data movement tests** - Real parquet-to-parquet workflows
- **❌ Performance benchmarks** - Memory usage and processing speed validation

**CRITICAL**: All existing tests need verification after major architecture refactor (flattened structure, config consolidation, clean naming)

## hygge Testing Philosophy

Following hygge's "Reliability Over Speed" principle:

1. **Test immediately after functionality** - Write tests as soon as you implement features
2. **Focus on behavior that matters** - Test user experience and data integrity
3. **Verify defaults "just work"** - Ensure smart defaults function correctly
4. **Test the happy path first** - Ensure basic functionality works before edge cases
5. **Test error scenarios** - Verify graceful failure handling
6. **Integration over unit tests** - Focus on end-to-end behavior that users care about

## Testing Structure

### 1. Core Component Tests (`tests/unit/hygge/core/`) ⚠️ NEEDS VERIFICATION

#### Flow Tests (`test_flow.py`) ⚠️
- ⚠️ Flow orchestration and producer-consumer pattern (needs verification after refactor)
- ⚠️ Async generator and consumer error handling (needs verification after refactor)
- ⚠️ Timeout protection and task cleanup (needs verification after refactor)
- ⚠️ Progress tracking and performance metrics (needs verification after refactor)

#### Coordinator Tests (`test_coordinator.py`) ⚠️
- ⚠️ YAML configuration loading and validation (needs verification after refactor)
- ⚠️ Flow creation and parallel execution (needs verification after refactor)
- ⚠️ Error handling and flow failure management (needs verification after refactor)
- ⚠️ Integration with Factory pattern (needs verification after refactor)

#### Factory Tests (`test_factory.py`) ⚠️
- ⚠️ Home and Store instantiation (needs verification after refactor)
- ⚠️ Type registration and validation (needs verification after refactor)
- ⚠️ Error handling for unsupported types (needs verification after refactor)

#### Home/Store Base Tests (`test_home.py`, `test_store.py`) ⚠️
- ⚠️ Base class functionality and interfaces (needs verification after refactor)
- ⚠️ Configuration validation and defaults (needs verification after refactor)
- ⚠️ Error handling patterns (needs verification after refactor)

### 2. Implementation-Level Tests (NEEDED)

#### ParquetHome Tests (`tests/unit/hygge/homes/test_parquet_home.py`)
```python
class TestParquetHome:
    def test_parquet_home_reads_single_file(self):
        """Test that ParquetHome can read single parquet files."""

    def test_parquet_home_reads_directory(self):
        """Test that ParquetHome can read directory of parquet files."""

    def test_parquet_home_batch_processing(self):
        """Test that ParquetHome processes data in batches."""

    def test_parquet_home_error_handling(self):
        """Test that ParquetHome handles missing files gracefully."""

    def test_parquet_home_config_consolidation(self):
        """Test that ParquetHomeConfig works with merged config."""
```

#### ParquetStore Tests (`tests/unit/hygge/stores/test_parquet_store.py`)
```python
class TestParquetStore:
    def test_parquet_store_writes_data(self):
        """Test that ParquetStore can write parquet files."""

    def test_parquet_store_staging_mechanism(self):
        """Test that ParquetStore stages data before final write."""

    def test_parquet_store_atomic_writes(self):
        """Test that ParquetStore uses atomic file operations."""

    def test_parquet_store_compression_settings(self):
        """Test that ParquetStore applies compression settings."""

    def test_parquet_store_config_consolidation(self):
        """Test that ParquetStoreConfig works with merged config."""
```

### 3. Configuration Tests (`tests/unit/hygge/core/`) ⚠️ NEEDS VERIFICATION

#### Config Validation Tests ⚠️
- ⚠️ FlowConfig validation and smart defaults (needs verification after refactor)
- ⚠️ HomeConfig and StoreConfig validation (needs verification after refactor)
- ⚠️ CoordinatorConfig validation and flow management (needs verification after refactor)
- ⚠️ Simple vs advanced configuration parsing (needs verification after refactor)
- ⚠️ Error handling and helpful error messages (needs verification after refactor)
- ⚠️ Config consolidation testing (merged configs work properly) (needs verification)

### 4. Integration Tests (`tests/integration/`) ⚠️ NEEDS VERIFICATION

#### Configuration Integration Tests ⚠️
- ⚠️ YAML config to execution pipeline validation (needs verification after refactor)
- ⚠️ Simple vs advanced configuration workflows (needs verification after refactor)
- ⚠️ Mixed configuration styles and performance testing (needs verification after refactor)
- ⚠️ Unicode handling and error scenarios (needs verification after refactor)
- ⚠️ User configuration errors and validation (needs verification after refactor)

#### End-to-End Flow Tests (NEEDED)
```python
class TestParquetToParquetFlows:
    def test_complete_data_movement_workflow(self):
        """Test complete parquet-to-parquet data movement with real data."""

    def test_multiple_flows_coordination(self):
        """Test multiple flows running together with Coordinator."""

    def test_flow_with_large_dataset(self):
        """Test flow with large dataset (memory management)."""

    def test_flow_error_recovery(self):
        """Test flow recovery from various error scenarios."""
```

### 5. Error Scenario Tests (`tests/error_scenarios/`) ⚠️ NEEDS VERIFICATION

#### Configuration Error Tests ⚠️
- ⚠️ Malformed YAML handling (needs verification after refactor)
- ⚠️ Invalid type validation (needs verification after refactor)
- ⚠️ Missing required fields (needs verification after refactor)
- ⚠️ User configuration errors (typos, case sensitivity) (needs verification after refactor)
- ⚠️ Conflicting options validation (needs verification after refactor)

#### Data Movement Error Tests (NEEDED)
```python
class TestDataMovementErrors:
    def test_file_not_found_errors(self):
        """Test handling of missing parquet files."""

    def test_permission_errors(self):
        """Test handling of permission issues."""

    def test_disk_space_errors(self):
        """Test handling of disk space issues."""

    def test_data_corruption_handling(self):
        """Test handling of corrupted parquet files."""

    def test_partial_failure_recovery(self):
        """Test recovery from partial failures."""
```

### 5. Performance Tests (`tests/performance/`)

#### Benchmark Tests (`test_benchmarks.py`)
```python
class TestBenchmarks:
    def test_small_dataset_performance(self):
        """Benchmark performance with small datasets."""

    def test_large_dataset_performance(self):
        """Benchmark performance with large datasets."""

    def test_memory_usage(self):
        """Test memory usage patterns."""

    def test_concurrent_flow_performance(self):
        """Test performance with multiple concurrent flows."""
```

## Test Data Management

### Test Data Structure
```
tests/
├── data/
│   ├── small/
│   │   ├── users.parquet (1,000 rows)
│   │   └── orders.parquet (5,000 rows)
│   ├── medium/
│   │   ├── users.parquet (100,000 rows)
│   │   └── orders.parquet (500,000 rows)
│   └── large/
│       ├── users.parquet (1,000,000 rows)
│       └── orders.parquet (5,000,000 rows)
├── fixtures/
│   ├── test_configs/
│   │   ├── simple_flow.yaml
│   │   ├── advanced_flow.yaml
│   │   └── multiple_flows.yaml
│   └── test_databases/
│       └── test.db
```

### Test Configuration
```python
# conftest.py
import pytest
import tempfile
import shutil
from pathlib import Path

@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)

@pytest.fixture
def test_data_dir():
    """Path to test data directory."""
    return Path(__file__).parent / "data"

@pytest.fixture
def simple_config():
    """Simple test configuration."""
    return {
        "flows": {
            "test_flow": {
                "home": "data/small/users.parquet",
                "store": "data/output"
            }
        }
    }
```

## Implementation Priority

### Phase 1: Post-Refactor Verification (CRITICAL - IMMEDIATE)
1. **Test suite execution** - Verify all existing tests pass after major refactor
2. **Import verification** - Ensure all imports work with new structure
3. **Config consolidation testing** - Verify merged config classes function properly
4. **Fix any broken tests** - Address any test failures caused by refactor
5. **Validate test coverage** - Ensure no test coverage was lost during refactor

### Phase 2: Implementation-Level Testing (NEXT)
1. **ParquetHome tests** - File/directory reading, batch processing, error handling
2. **ParquetStore tests** - Writing, staging, atomic operations, compression
3. **Config consolidation tests** - Verify merged ParquetHomeConfig/ParquetStoreConfig

### Phase 3: End-to-End Testing (FOLLOWING)
1. **Real data movement tests** - Complete parquet-to-parquet workflows
2. **Performance benchmarks** - Memory usage and processing speed
3. **Error recovery tests** - File system and data integrity scenarios

## Testing Tools

### Required Dependencies
```toml
[project.optional-dependencies]
test = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
    "pytest-cov>=4.0.0",
    "pytest-benchmark>=4.0.0",
    "pytest-mock>=3.10.0",
    "faker>=18.0.0",  # For generating test data
]
```

### Test Configuration
```toml
# pyproject.toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = [
    "--strict-markers",
    "--strict-config",
    "--cov=src/hygge",
    "--cov-report=term-missing",
    "--cov-report=html",
    "--cov-fail-under=80"
]
markers = [
    "unit: Unit tests",
    "integration: Integration tests",
    "performance: Performance tests",
    "error_scenarios: Error scenario tests"
]
```

## Success Metrics

### Coverage Targets
- **Unit tests**: 90%+ coverage for core components
- **Integration tests**: 80%+ coverage for end-to-end flows
- **Error scenarios**: 70%+ coverage for failure modes

### Performance Targets
- **Small datasets** (< 10K rows): < 1 second
- **Medium datasets** (< 100K rows): < 10 seconds
- **Large datasets** (< 1M rows): < 60 seconds
- **Memory usage**: < 100MB for 1M row datasets

### Reliability Targets
- **Error handling**: 100% of error scenarios handled gracefully
- **Data integrity**: 100% of data preserved through flows
- **Configuration**: 100% of valid configs work as expected

## Getting Started

1. **Install test dependencies**: `pip install -e ".[test]"`
2. **Run post-refactor verification**: `pytest tests/ -v` (verify all tests pass after major refactor)
3. **Run specific test suites**:
   - `pytest tests/unit/hygge/core/ -v` (core architecture tests)
   - `pytest tests/integration/ -v` (configuration integration tests)
   - `pytest tests/error_scenarios/ -v` (error scenario tests)
4. **Check coverage**: `pytest --cov=src/hygge --cov-report=html`
5. **Next phase**: Add implementation-level tests for ParquetHome and ParquetStore

## Current Architecture

The framework now uses:
- **Clean naming**: `Coordinator`, `Flow`, `Factory`, `Home`, `Store` + their configs
- **Maximum cohesion**: Each file contains both implementation and configuration
- **Flattened structure**: No nested directories, clean imports
- **Consolidated configs**: `ParquetHomeConfig` and `ParquetStoreConfig` merged into their implementation files

This testing strategy ensures hygge follows its "Reliability Over Speed" principle while maintaining the comfort and simplicity that makes it hygge.
