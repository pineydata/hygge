# Test Coverage Improvements: Visibility and Gaps

## Problem

The project states "test immediately after functionality" as a principle, but:

- No test coverage metrics or reporting
- Unclear what areas are well-tested vs under-tested
- Many integration tests but unclear unit test coverage
- Tests seem focused on MSSQL and Parquet; other stores may be under-tested
- No CI validation of test coverage

This makes it hard to know if the codebase is well-tested and where gaps exist.

## Current Behavior

### Test Structure

- Integration tests in `tests/integration/`
- Unit tests in `tests/unit/hygge/`
- Tests cover MSSQL and Parquet extensively
- Other stores (ADLS, OneLake, OpenMirroring) have some tests but coverage is unclear

### No Coverage Metrics

- No pytest-cov or coverage.py configuration
- No coverage reports in CI
- No visibility into what's tested vs what's not

## Use Cases

1. **Visibility**: Know what areas are well-tested vs under-tested
2. **CI Validation**: Ensure test coverage doesn't degrade over time
3. **Gap Identification**: Identify areas that need more tests
4. **Confidence**: Confidence that changes don't break existing functionality

## Proposed Solution

### Phase 1: Add Test Coverage Reporting

Add pytest-cov to generate coverage reports.

**File Locations:**
- **Configuration**: `pyproject.toml` (existing file - update), `.coveragerc` or add to `pyproject.toml` (new section)
- **CI**: `.github/workflows/test.yml` or similar (new file, if CI not already set up)

```python
# In pyproject.toml
[project.optional-dependencies]
dev = [
    "pytest>=8.3.3",
    "pytest-asyncio>=0.24.0",
    "pytest-cov>=4.0.0",  # Already present
    "pytest-timeout>=2.2.0",
    "ruff>=0.2.1",
    "pre-commit>=4.0.0",
]

# In pytest.ini
[pytest]
# ... existing config ...
addopts =
    --cov=src/hygge
    --cov-report=term-missing
    --cov-report=html
    --cov-report=xml
    --cov-fail-under=70
```

### Phase 2: Add Coverage Configuration

Configure coverage to exclude test files and focus on source code:

```python
# In .coveragerc or pyproject.toml
[tool.coverage.run]
source = ["src/hygge"]
omit = [
    "*/tests/*",
    "*/test_*",
    "*/__pycache__/*",
    "*/__init__.py",
]

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "def __repr__",
    "raise AssertionError",
    "raise NotImplementedError",
    "if __name__ == .__main__.:",
    "if TYPE_CHECKING:",
    "@abstractmethod",
]
```

### Phase 3: Add Coverage to CI

Add coverage reporting to CI (when CI is set up):

```yaml
# In .github/workflows/test.yml (example)
- name: Run tests with coverage
  run: |
    pytest --cov=src/hygge --cov-report=xml --cov-report=term

- name: Upload coverage to Codecov
  uses: codecov/codecov-action@v3
  with:
    file: ./coverage.xml
    flags: unittests
    name: codecov-umbrella
```

### Phase 4: Identify Coverage Gaps

Run coverage report and identify gaps:

```bash
# Generate coverage report
pytest --cov=src/hygge --cov-report=html

# Review coverage report
open htmlcov/index.html
```

Focus areas for improvement:
- Coordinator complex methods (`_create_entity_flow`, `_run_flows`)
- CLI parsing logic
- Error handling paths
- Store optional methods
- Journal error handling
- Watermark tracking logic
- **Large data volumes** - Test flows with 100M+ rows (approaching midmarket limits)
- **Concurrent execution** - Test 10+ concurrent flows stress scenarios
- **Connection pool exhaustion** - Test behavior when connection pools are exhausted
- **Edge cases in error handling** - Test exception chaining, retry logic with different exception types

### Phase 5: Add Missing Tests

Add tests for identified gaps:

1. **Coordinator Tests**
   - Test config loading patterns (project, directory, single-file)
   - Test entity flow creation with path merging
   - Test flow filtering logic
   - Test progress tracking
   - Test summary generation

2. **CLI Tests**
   - Test flow filter parsing
   - Test flow override parsing
   - Test run_type override logic
   - Test error handling

3. **Error Handling Tests**
   - Test specific exception types
   - Test retry logic with different exception types
   - Test journal error handling
   - Test store/home error handling

4. **Store Tests**
   - Test optional methods (configure_for_run, cleanup_staging, etc.)
   - Test error scenarios
   - Test edge cases

5. **Watermark Tests**
   - Test watermark tracking logic
   - Test schema validation
   - Test watermark serialization
   - Test edge cases (missing columns, unsupported types)

## File Locations

- **Coverage Configuration**: `pyproject.toml` (existing file - add `[tool.coverage.*]` sections)
- **Test Configuration**: `pytest.ini` (existing file - update `addopts`)
- **CI Configuration**: `.github/workflows/test.yml` or similar (new file, if CI not already set up)
- **Test Files**: Various existing test files in `tests/unit/` and `tests/integration/` - extend as needed
- **Coverage Reports**: `htmlcov/` (generated directory), `coverage.xml` (generated file)

## Implementation Plan

1. **Add test coverage reporting** (highest impact)
   - Update `pyproject.toml` to add `[tool.coverage.run]` and `[tool.coverage.report]` sections
   - Update `pytest.ini` to add coverage options to `addopts`
   - Run `pytest --cov=src/hygge --cov-report=html` to generate coverage reports
   - Review `htmlcov/index.html` to identify gaps

2. **Add coverage to CI** (high impact)
   - Create or update CI workflow file (e.g., `.github/workflows/test.yml`)
   - Add coverage reporting step
   - Set coverage threshold (e.g., 70%) in `pytest.ini`
   - Configure CI to fail if coverage drops below threshold

3. **Add missing tests** (medium impact)
   - Add tests for identified gaps in existing test files:
     - `tests/unit/hygge/core/test_coordinator.py` - test complex Coordinator methods
     - `tests/unit/hygge/test_cli.py` - test CLI parsing logic
     - `tests/unit/hygge/core/test_flow.py` - test error handling paths
     - `tests/unit/hygge/core/test_store.py` - test optional methods
     - `tests/unit/hygge/core/test_journal.py` - test journal error handling
   - Focus on complex methods first
   - Add tests for error handling paths

4. **Maintain coverage** (ongoing)
   - Review coverage reports regularly
   - Add tests for new features
   - Maintain coverage threshold

## Testing Considerations

- Coverage reports should focus on source code, not test code
- Coverage threshold should be reasonable (70% is a good starting point)
- Coverage should be maintained over time
- Focus on critical paths and error handling

## Related Issues

- See `coordinator-refactoring.md` for related Coordinator testing
- See `cli-simplification.md` for related CLI testing
- See `watermark-tracker-extraction.md` for related watermark testing
- See `error-handling-standardization.md` for related error handling testing

## Technical Review Findings

**From Technical Review (2025):**
- Current test coverage is good (1200+ tests), but some edge cases may be under-tested
- Integration tests exist but could benefit from stress testing at midmarket scale
- Large data volume tests (100M+ rows) would increase confidence
- Concurrent flow execution stress tests would verify Coordinator behavior under load
- Connection pool exhaustion scenarios should be tested

**Impact at Midmarket Scale:**
- Midmarket orgs need confidence that hygge handles their data volumes reliably
- Stress tests verify the framework works at scale without over-engineering for global enterprise
- Edge case testing prevents production surprises

## Priority

**High** - This is foundational work that will help maintain code quality and identify gaps in test coverage. It should be done early to establish a baseline. The technical review identified specific gaps (large volumes, concurrency, connection pools) that should be prioritized.
