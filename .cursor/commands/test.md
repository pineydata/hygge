# Generate Tests

Generate comprehensive tests for the selected code following hygge's testing philosophy.

## Context
- hygge emphasizes "Test immediately after functionality"
- Focus on behavior that matters to users, not implementation details
- Test data integrity and user experience
- Follow existing test patterns in `tests/` directory

## Test Structure
- Use pytest with fixtures from `tests/conftest.py`
- Follow patterns from `tests/integration/` and `tests/unit/`
- Test the happy path first, then error scenarios
- Use descriptive test names: `test_<what>_<expected_behavior>`

## Test Patterns
- **Integration tests**: Test complete workflows end-to-end
- **Unit tests**: Test individual components in isolation
- **Fixtures**: Use `temp_dir`, `sample_data`, etc. from conftest.py
- **Data**: Use Polars DataFrames for test data (hygge's core stack)

## hygge-Specific Guidelines
- Test user experience: Does it work as expected?
- Verify data integrity: Is data preserved correctly?
- Test smart defaults: Do minimal configs "just work"?
- Test error scenarios: Graceful failure handling
- Use async/await patterns for I/O operations

## Output
Generate complete test files with:
1. Proper imports (pytest, polars, hygge modules)
2. Fixtures for test data and temporary directories
3. Happy path tests first
4. Error scenario tests
5. Clear docstrings explaining what each test verifies
6. Assertions that verify behavior, not implementation

## Example Test Structure
```python
"""
Tests for [component name].

Following hygge's testing principles:
- Test behavior that matters to users
- Focus on data integrity and reliability
- Verify [primary use case]
"""
import pytest
from pathlib import Path
import polars as pl

from hygge.[module] import [Component]

@pytest.fixture
def temp_test_dir(temp_dir):
    """Create temporary directory for test."""
    return temp_dir / "test_data"

def test_[component]_[happy_path]():
    """Test [what] works correctly."""
    # Arrange
    # Act
    # Assert
```

Generate tests that follow this structure and hygge's philosophy of comfort, reliability, and natural data movement.
