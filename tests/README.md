# hygge Testing Suite

## Overview

This comprehensive testing suite follows hygge's "Reliability Over Speed" principle, focusing on user experience and data integrity rather than technical perfection.

## Testing Philosophy

Following hygge's core values:
- **Comfort**: Tests should feel natural and reliable
- **Simplicity**: Clear, maintainable test code
- **Reliability**: Robust test scenarios that catch real issues
- **Flow**: Smooth testing experience that aids development

## Test Structure

```
tests/
├── conftest.py                          # Shared fixtures and utilities
├── unit/                               # Unit tests
│   └── hygge/core/
│       ├── configs/                     # Configuration tests
│       │   ├── test_configs.py          # Main config validation
│       │   ├── test_home_config.py      # Home config tests
│       │   ├── test_store_config.py     # Store config tests
│       │   ├── test_settings.py         # Settings tests
│       │   └── test_flow_config.py      # Flow parsing tests
│       ├── home.py                      # Home base class tests
│       ├── store.py                     # Store base class tests
│       └── homes/
│           ├── test_parquet.py          # Parquet home tests
│           └── test_sql.py              # SQL home tests
├── integration/                         # Integration tests
│   └── test_config_integration.py      # End-to-end config tests
├── error_scenarios/                     # Error handling tests
│   └── test_configuration_errors.py    # Error scenario coverage
└── README.md                           # This file
```

## Running Tests

### All Tests
```bash
pytest tests/ -v
```

### By Category
```bash
# Unit tests only
pytest tests/unit/ -v

# Integration tests
pytest tests/integration/ -v

# Error scenarios
pytest tests/error_scenarios/ -v

# Configuration tests specifically
pytest tests/unit/hygge/core/configs/ -v
```

### With Coverage
```bash
pytest tests/ --cov=src/hygge --cov-report=html --cov-report=term-missing
```

## Test Coverage

### Core Components Tested

#### ✅ Configuration System (Complete)
- **HyggeSettings**: Default values, validation, type checking
- **FlowConfig**: Simple vs advanced parsing, smart defaults
- **HomeConfig**: Type validation, required fields, options
- **StoreConfig**: Type validation, required fields, options
- **Integration**: YAML parsing, end-to-end validation

#### ✅ Error Scenarios (Complete)
- Invalid configuration formats
- Missing required fields
- Type mismatches
- File system errors
- Network connectivity issues
- Performance edge cases
- User configuration errors

#### 🔄 Core Data Movement (In Progress)
- Home reading functionality
- Store writing functionality
- Flow orchestration
- Data integrity preservation
- Error recovery

## Key Test Patterns

### Configuration Testing
```python
def test_simple_config_parsing():
    """Test Rails-inspired simple configuration."""
    config = FlowConfig(
        home="data/users.parquet",
        store="data/lake/users"
    )
    assert config.home_config.path == "data/users.parquet"
    assert config.home_config.type == "parquet"  # Smart default
```

### Error Scenario Testing
```python
def test_invalid_home_type():
    """Test graceful handling of invalid configuration."""
    with pytest.raises(Exception):
        FlowConfig(home={"type": "invalid"}, store="output")
```

### Integration Testing
```python
def test_yaml_to_flow_execution(yaml_config):
    """Test complete configuration pipeline."""
    yaml_content = yaml.dump(yaml_config)
    parsed_config = yaml.safe_load(yaml_content)

    # Should validate without error
    flow_config = FlowConfig(**parsed_config["flows"]["test_flow"])
    assert flow_config.home_config.path is not None
```

## Test Fixtures

### Data Fixtures
- `small_sample_data`: 10 rows for quick tests
- `medium_sample_data`: 1000 rows for realistic scenarios
- `sample_parquet_file`: Pre-created parquet file

### Configuration Fixtures
- `simple_flow_config`: Rails-inspired simple config
- `advanced_flow_config`: Full-featured config with options
- `multiple_flows_config`: Multi-flow configuration
- `sql_config`: SQL data source configuration
- `invalid_config`: Validation error cases

### Utility Fixtures
- `temp_dir`: Temporary directory management
- `test_data_manager`: Data creation and cleanup
- `config_validator`: Configuration validation helpers
- `assertion_helpers`: Common test assertions

## Testing Guidelines

### Writing New Tests

1. **Follow the testing philosophy**: Focus on user experience and data integrity
2. **Test behavior, not implementation**: Verify the end result, not internal details
3. **Use descriptive test names**: `test_simple_config_works` over `test_config1`
4. **Group related tests**: Use test classes for related functionality
5. **Test edge cases**: Include boundary conditions and error scenarios

### Test Organization

1. **Unit tests**: Test individual components in isolation
2. **Integration tests**: Test component interactions
3. **Error scenarios**: Test graceful failure handling
4. **Performance tests**: Verify acceptable behavior under load

### Mock Strategy

- **Mock external dependencies**: File system, databases, networks
- **Don't mock hygge components**: Test the actual implementation
- **Use fixtures**: Prefer fixtures over complex mocks when possible

## Success Metrics

### Coverage Targets
- **Configuration system**: 95%+ coverage (✅ Achieved)
- **Core data movement**: 85%+ coverage (🎯 Target)
- **Error scenarios**: 80%+ coverage (🎯 Target)

### Reliability Targets
- **Test execution time**: < 30 seconds for full suite
- **False positives**: < 5% test flakiness
- **Error detection**: Catch configuration errors before runtime

## Continuous Integration

The test suite integrates with CI/CD to ensure:
- All PRs pass tests before merge
- Test coverage remains above minimum thresholds
- Performance tests catch regressions
- Error scenarios remain covered

## Debugging Tests

### Common Issues

**Import errors**: Ensure `tests/conftest.py` properly adds `src/` to Python path

**Fixture errors**: Check fixture dependencies and ensure proper cleanup

**Test isolation**: Use `temp_dir` fixture for temporary data, avoid shared state

### Debug Commands

```bash
# Run single test with verbose output
pytest tests/unit/hygge/core/configs/test_settings.py::TestHyggeSettings::test_default_settings_values -v -s

# Run tests with debugging
pytest tests/ --pdb --capture=no

# Show test collection issues
pytest tests/ --collect-only
```

## Contributing Tests

When adding new functionality:

1. **Write tests immediately**: Don't commit code without tests
2. **Coverage first**: Ensure new code is thoroughly tested
3. **Update documentation**: Keep this README current
4. **Add fixtures**: Create reusable fixtures for new test patterns
5. **Test error cases**: Include error scenarios and edge cases

## Future Enhancements

### Planned Testing Additions
- **Performance benchmarks**: Data movement speed tests
- **Memory usage tests**: Ensure efficient memory usage
- **Concurrent execution tests**: Parallel flow handling
- **End-to-end UI tests**: Full workflow validation

### Testing Tools Integration
- **Property-based testing**: Hypothesis for comprehensive input testing
- **Mutation testing**: Ensure tests actually validate behavior
- **Visual test reporting**: Better test result visualization

This testing suite embodies hygge's philosophy of comfort and reliability, ensuring that the framework works correctly for users while remaining maintainable for developers.
