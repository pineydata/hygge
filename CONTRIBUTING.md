# Contributing to hygge

Thank you for your interest in contributing to hygge! This document provides guidelines for making hygge even more comfortable and reliable.

## Development Setup

1. Install uv:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Clone the repository:
```bash
git clone https://github.com/pineydata/hygge.git
cd hygge
```

3. Create a virtual environment and install dependencies:
```bash
uv venv
uv pip install -e ".[dev]"
```

## Development Workflow

1. Create a new branch for your feature:
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes, following hygge's philosophy:
- **Comfort**: APIs should feel natural and intuitive
- **Simplicity**: Clean, clear code over complex optimizations
- **Reliability**: Robust, predictable behavior
- **Flow**: Smooth, efficient implementation

3. Write tests immediately after implementing functionality:
```bash
pytest
```

4. Run linting:
```bash
ruff check .
```

5. Commit your changes:
```bash
git add .
git commit -m "Description of your changes"
```

6. Push your changes and create a pull request:
```bash
git push origin feature/your-feature-name
```

## Code Style

- We use ruff for linting
- Maximum line length is 88 characters
- Use type hints for all function arguments and return values
- Follow Google style for docstrings
- **Prioritize comfort and clarity** over cleverness

## Testing Philosophy

- **Focus on behavior that matters**: Test user experience and data integrity
- **Verify defaults "just work"**: Ensure smart defaults function correctly
- **Test the happy path first**: Ensure basic functionality works before edge cases
- **Test error scenarios**: Verify graceful failure handling
- **Integration over unit tests**: Focus on end-to-end behavior that users care about

## Documentation

- Update documentation for new features
- Include docstrings for new functions and classes
- Update README.md if necessary
- Add examples that demonstrate comfort and simplicity
- **Focus on user experience**: Does this make data movement more comfortable?

## Pull Request Process

1. Ensure all tests pass
2. Update documentation as needed
3. Add your changes to CHANGELOG.md
4. Request review from maintainers
5. Address review comments

## hygge's Core Question

Before submitting any changes, ask yourself:
- **Does this make data movement more comfortable?**
- **Does this feel natural and intuitive?**
- **Does this maintain hygge's reliability?**
- **Does this keep data flowing smoothly?**

## Questions?

If you have questions, please open an issue or reach out to the maintainers. We're here to help make hygge even more comfortable!
