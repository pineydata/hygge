# Run Tests with Coverage

Run the test suite with coverage reporting to identify tested vs untested code.

## Command

```bash
pytest --cov=src/hygge --cov-report=term-missing --cov-report=html --cov-report=xml
```

## Coverage Reports Generated

- **Terminal output**: Shows coverage percentage and missing lines
- **HTML report**: `htmlcov/index.html` - Interactive browser view
- **XML report**: `coverage.xml` - For CI/tooling integration

## Usage

### Basic Coverage Run
```bash
pytest --cov=src/hygge --cov-report=html
```

### Full Coverage with All Reports
```bash
pytest --cov=src/hygge --cov-report=term-missing --cov-report=html --cov-report=xml
```

### View HTML Report
After running, open `htmlcov/index.html` in your browser to see:
- Overall coverage percentage
- Coverage by file
- Line-by-line coverage highlighting
- Missing lines identification

## Coverage Configuration

Coverage is configured in `pyproject.toml`:
- **Source**: `src/hygge` (only source code, not tests)
- **Exclusions**: Test files, `__pycache__`, `__init__.py`, common patterns
- **No threshold enforcement**: Coverage is informational, not blocking

## hygge Testing Philosophy

- **Test immediately after functionality**: Write tests as you develop
- **Focus on behavior that matters**: Test user experience and data integrity
- **Coverage as a tool**: Use coverage to identify gaps, not as a gate
- **No CI enforcement**: Coverage thresholds are not enforced in CI to avoid gaming

## Current Coverage Status

Run the command to see current coverage. Focus areas for improvement:
- Low coverage areas: `azure_onelake.py`, `openmirroring/store.py`, `adls/store.py`
- Error handling paths: Journal, connection pools, retry logic
- Edge cases: Large data volumes, concurrent execution, connection exhaustion

## Notes

- Coverage reports are generated locally, not in CI
- No fail-under threshold - coverage is informational only
- Use coverage to guide testing, not as a strict requirement
- Focus on testing behavior that matters to users
