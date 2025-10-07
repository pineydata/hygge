# hygge CI/CD Pipeline

This directory contains the GitHub Actions workflows that enforce hygge's TDD approach and ensure reliable, comfortable data movement.

## Workflows

### TDD Validation (`tdd-validation.yml`)
**Triggers**: Pull requests and pushes to `main`/`develop`

**Purpose**: Enforces hygge's core principle of "write new code and implement tests"

**What it does**:
- Runs tests on Python 3.11 and 3.12
- Enforces 85% minimum test coverage
- Validates code style with ruff
- Uploads coverage reports to Codecov

**Coverage Gates**:
- ✅ **85% minimum coverage** (currently 89%)
- ✅ **All tests must pass**
- ✅ **Code style validation**

### Status Checks (`status-checks.yml`)
**Triggers**: Pull request events

**Purpose**: Additional validation for TDD compliance

**What it does**:
- Validates commit messages follow TDD conventions
- Ensures test coverage thresholds are met
- Provides clear feedback on CI requirements

## Local Development

### Pre-commit Hooks
Install and run pre-commit hooks to catch issues before pushing:

```bash
# Install pre-commit
pip install pre-commit

# Install hooks
pre-commit install

# Run on all files
pre-commit run --all-files
```

### Manual Testing
Run the same checks locally that CI runs:

```bash
# Run tests with coverage
pytest tests/ --cov=src/hygge --cov-report=term-missing --cov-fail-under=85

# Run linting
ruff check src/ tests/
ruff format --check src/ tests/

# Run the coverage checker script
python scripts/check_test_coverage.py
```

## Coverage Requirements

### Current Status
- **Overall Coverage**: 89% ✅
- **Required Minimum**: 85% ✅
- **Tests Passing**: 158/158 ✅

### Coverage by Component
- **Core Components**: 92-95% coverage
- **Parquet Home**: 100% coverage
- **Parquet Store**: 85% coverage
- **Utility Modules**: 91-100% coverage

## TDD Enforcement

### Commit Message Requirements
Commit messages should indicate tests were written:
- ✅ `feat: add new feature with tests`
- ✅ `fix: resolve issue and add tests`
- ❌ `feat: add new feature` (no mention of tests)

### Test Coverage Gates
- **New code**: Must maintain or improve overall coverage
- **Critical paths**: Core data movement logic requires high coverage
- **Integration tests**: Required for new features

## Philosophy Alignment

This CI/CD setup serves hygge's core values:

- **Comfort**: Automated testing means developers can trust hygge "just works"
- **Reliability**: Robust validation prevents breaking changes
- **Flow**: Fast feedback keeps development smooth and frictionless
- **Simplicity**: Clear, automated gates without complex configuration

## Next Steps

### Phase 2: Advanced Features
- Breaking change detection
- Performance regression testing
- API compatibility checks
- Security scanning

### Phase 3: Production Ready
- Automated release management
- Dependency vulnerability scanning
- Advanced monitoring and alerting

## Troubleshooting

### Common Issues

**Coverage below 85%**:
- Add tests for uncovered code paths
- Focus on critical data movement logic
- Ensure integration tests cover end-to-end scenarios

**Linting failures**:
- Run `ruff check --fix` to auto-fix issues
- Follow hygge's code style guidelines
- Ensure consistent formatting

**Test failures**:
- Run tests locally first: `pytest tests/`
- Check async test patterns
- Verify test fixtures and data setup

## Success Metrics

- **Zero breaking changes** in production releases
- **All PRs require passing tests** before merge
- **100% of new features** have corresponding tests
- **Fast feedback** (< 10 minutes CI pipeline time)

---

*hygge's CI/CD pipeline ensures that data movement remains comfortable, reliable, and predictable.*
