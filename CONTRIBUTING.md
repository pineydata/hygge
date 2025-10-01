# Contributing to ELK

Thank you for your interest in contributing to ELK! This document provides guidelines and instructions for contributing.

## Development Setup

1. Install uv:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Clone the repository:
```bash
git clone https://github.com/yourusername/elk.git
cd elk
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

2. Make your changes, following our coding standards:
- Use type hints
- Follow PEP 8 style guide
- Add docstrings for new functions and classes
- Add tests for new functionality

3. Run tests:
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

## Testing

- Write unit tests for new functionality
- Write integration tests for new features
- Ensure all tests pass before submitting a pull request
- Maintain or improve code coverage

## Documentation

- Update documentation for new features
- Include docstrings for new functions and classes
- Update README.md if necessary
- Add examples for new features

## Pull Request Process

1. Ensure all tests pass
2. Update documentation as needed
3. Add your changes to CHANGELOG.md
4. Request review from maintainers
5. Address review comments

## Questions?

If you have questions, please open an issue or reach out to the maintainers.

