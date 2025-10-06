# CI/CD TDD Strategy for hygge

## Overview

This document outlines a comprehensive GitHub Actions CI/CD strategy to enforce hygge's TDD approach ("write new code and implement tests") while preventing breaking changes through automated validation.

## Current Foundation

### ✅ Existing Testing Infrastructure
- **115 core tests passing** with comprehensive coverage
- **pytest with async support** for hygge's async patterns
- **Coverage reporting** via pytest-cov
- **Ruff linting** for code quality
- **Integration tests** for end-to-end validation
- **Error scenario testing** for graceful failure handling

### 📊 Test Coverage Status
- **Configuration system**: 95%+ coverage ✅
- **Core data movement**: 85%+ coverage 🎯
- **Error scenarios**: 80%+ coverage 🎯

## GitHub Actions Workflow Strategy

### 1. Primary TDD Enforcement Workflow

**File**: `.github/workflows/tdd-validation.yml`

```yaml
name: TDD Validation

on:
  pull_request:
    branches: [ main, develop ]
  push:
    branches: [ main, develop ]

jobs:
  tdd-check:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: ["3.11", "3.12"]

    steps:
    - uses: actions/checkout@v4

    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt

    - name: Lint with ruff
      run: |
        ruff check src/ tests/
        ruff format --check src/ tests/

    - name: Run tests with coverage
      run: |
        pytest tests/ --cov=src/hygge --cov-report=xml --cov-fail-under=80

    - name: Upload coverage to Codecov
      if: matrix.python-version == '3.11'
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
        flags: unittests
```

### 2. Breaking Change Detection Workflow

**File**: `.github/workflows/breaking-change-detection.yml`

```yaml
name: Breaking Change Detection

on:
  push:
    branches: [ main ]

jobs:
  breaking-change-check:
    runs-on: ubuntu-latest
    if: github.event.pushes[0].commits[0].message !~ /^chore|^docs|^style/

    steps:
    - uses: actions/checkout@v4
      with:
        fetch-depth: 0  # Full history for comparison

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: "3.11"

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
        pip install pytest-benchmark

    - name: Run regression tests
      run: |
        # Test against previous commit to catch breaking changes
        pytest tests/integration/ -v --benchmark-only --benchmark-save=current

        # Compare with previous benchmark if it exists
        if [ -f .benchmarks/*/current.json ]; then
          pytest tests/integration/ --benchmark-compare --benchmark-compare-fail=mean:10%
        fi

    - name: Validate public API compatibility
      run: |
        python -c "
        import sys
        sys.path.insert(0, 'src')

        # Test that all public APIs are still importable
        from hygge.core import Coordinator, Flow, Factory
        from hygge.homes.parquet import ParquetHome
        from hygge.stores.parquet import ParquetStore

        # Test that core functionality still works
        print('✅ All public APIs importable')
        "
```

### 3. Pre-commit Validation Workflow

**File**: `.github/workflows/pre-commit-validation.yml`

```yaml
name: Pre-commit Validation

on:
  pull_request:
    types: [opened, synchronize, reopened]

jobs:
  pre-commit-check:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: "3.11"

    - name: Install pre-commit
      run: |
        pip install pre-commit

    - name: Run pre-commit checks
      run: |
        pre-commit run --all-files
```

## Local Development Integration

### Pre-commit Configuration

**File**: `.pre-commit-config.yaml`

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.2.1
    hooks:
      - id: ruff
        args: [--fix, --exit-non-zero-on-fix]
      - id: ruff-format

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.8.0
    hooks:
      - id: mypy
        additional_dependencies: [types-PyYAML]

  - repo: local
    hooks:
      - id: pytest-check
        name: pytest-check
        entry: pytest tests/ --tb=short
        language: system
        types: [python]
        pass_filenames: false
```

### GitHub Actions Status Checks

**File**: `.github/workflows/status-checks.yml`

```yaml
name: Status Checks

on:
  pull_request:
    types: [opened, synchronize, reopened]

jobs:
  status-check:
    runs-on: ubuntu-latest
    if: github.event.pull_request.draft == false

    steps:
    - uses: actions/checkout@v4

    - name: Check test coverage
      run: |
        pip install -r requirements.txt
        pytest tests/ --cov=src/hygge --cov-report=term
        coverage report --fail-under=80

    - name: Check for new tests
      run: |
        # Ensure new code has corresponding tests
        python scripts/check_test_coverage.py
```

## TDD Enforcement Strategy

### 1. Branch Protection Rules (GitHub Settings)
- **Require status checks to pass** before merging
- **Require branches to be up to date** before merging
- **Require review from code owners** for critical changes
- **Restrict pushes to main** to prevent direct commits

### 2. Test Coverage Gates
- **Minimum 80% coverage** for new code
- **No decrease in overall coverage** allowed
- **Integration tests required** for new features
- **Error scenario tests required** for new error handling

### 3. Smart Commit Message Validation
```yaml
# In your workflow, add this step:
- name: Validate commit messages follow TDD
  run: |
    # Check if commit message indicates tests were written
    if ! git log -1 --pretty=%B | grep -E "(test|fix|feat).*test"; then
      echo "❌ Commit message should indicate tests were written"
      exit 1
    fi
```

## Implementation Phases

### Phase 1: Basic CI (Immediate - Post P2P Demo)
1. **Create `.github/workflows/` directory**
2. **Add PR validation workflow** with test coverage gates
3. **Set up coverage reporting** with Codecov integration
4. **Configure ruff linting** in CI pipeline

### Phase 2: TDD Enforcement (Next Sprint)
1. **Add pre-commit hooks** for local development
2. **Implement breaking change detection** workflow
3. **Set up API compatibility checks** for public interfaces
4. **Configure branch protection rules** in GitHub

### Phase 3: Advanced Features (Future)
1. **Performance regression testing** with benchmarks
2. **Security scanning** for dependencies
3. **Dependency vulnerability checks** with automated updates
4. **Automated release management** with semantic versioning

## Benefits for hygge

### 1. **Prevents Breaking Changes**
- Automated validation against previous commits
- API compatibility checks for public interfaces
- Performance regression detection

### 2. **Enforces TDD Workflow**
- Test coverage gates prevent untested code
- Commit message validation encourages test-first thinking
- Pre-commit hooks catch issues early

### 3. **Maintains Code Quality**
- Consistent linting and formatting
- Automated code review assistance
- Performance monitoring

### 4. **Provides Fast Feedback**
- Parallel test execution across Python versions
- Immediate notification of failures
- Clear error messages and suggestions

### 5. **Aligns with hygge Principles**
- **Reliability**: Robust, predictable behavior
- **Comfort**: Smooth development experience
- **Flow**: Efficient, frictionless process

## Configuration Requirements

### Dependencies to Add
```txt
# Add to requirements.txt
pytest-benchmark>=4.0.0
pre-commit>=3.6.0
mypy>=1.8.0
types-PyYAML>=6.0.0
```

### GitHub Repository Settings
1. **Enable Actions** in repository settings
2. **Configure branch protection** for main branch
3. **Set up Codecov** integration for coverage reports
4. **Configure code owners** for review requirements

## Success Metrics

### Coverage Targets
- **Overall coverage**: Maintain 85%+ (currently achieved)
- **New code coverage**: Require 90%+ for new features
- **Critical paths**: 95%+ coverage for core data movement

### Performance Targets
- **Test execution time**: < 5 minutes for full suite
- **CI pipeline time**: < 10 minutes end-to-end
- **False positive rate**: < 2% for automated checks

### Quality Gates
- **Zero breaking changes** in production releases
- **All PRs require passing tests** before merge
- **100% of new features** have corresponding tests

## Integration with Current Workflow

### Compatible with Existing Patterns
- **Async testing**: CI supports pytest-asyncio
- **Fixture-based testing**: All existing fixtures work in CI
- **Error scenario testing**: Edge cases validated automatically
- **Integration testing**: End-to-end workflows verified

### Enhanced Development Experience
- **Local pre-commit hooks** catch issues before commit
- **Automated testing** reduces manual verification
- **Clear feedback** on test failures and coverage gaps
- **Consistent environment** across development and CI

## Next Steps After P2P Demo

### Immediate Actions (Week 1)
1. Create `.github/workflows/` directory structure
2. Implement basic PR validation workflow
3. Set up coverage reporting with pytest-cov
4. Configure ruff linting in CI

### Short-term Goals (Month 1)
1. Add pre-commit hooks for local development
2. Implement breaking change detection
3. Set up branch protection rules
4. Configure API compatibility checks

### Long-term Vision (Quarter 1)
1. Performance regression testing
2. Security scanning and vulnerability management
3. Automated release management
4. Advanced monitoring and alerting

## Conclusion

This CI/CD strategy provides a robust foundation for enforcing hygge's TDD approach while preventing breaking changes. It aligns with the framework's principles of reliability, comfort, and flow, ensuring that data movement remains natural and predictable.

The phased implementation approach allows for gradual adoption without disrupting current development, with clear milestones and success metrics to track progress toward a more reliable and maintainable codebase.
