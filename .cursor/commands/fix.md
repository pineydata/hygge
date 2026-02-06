# Fix Linting Issues

Fix linting and formatting issues using ruff, following hygge's code quality standards.

## Linting Tool
- **Primary tool**: ruff (configured in `pyproject.toml`)
- **Line length**: 88 characters (Black-compatible)
- **Target Python**: 3.11+
- **Enabled rules**: pycodestyle (E), Pyflakes (F), isort (I)

## Fix Process

**Note: This command provides guidance and fixes code directly. Do NOT run terminal commands.**

1. **Identify linting issues** by analyzing the code against ruff rules
2. **Apply fixes directly** to the code files (imports, formatting, style)
3. **Preserve functionality** - only fix linting, never change logic

## Common Fixes

### Import Organization (isort)
- Group imports: stdlib, third-party, first-party (hygge)
- Sort imports alphabetically within groups
- Use `known-first-party = ["hygge"]` configuration

### Code Style (pycodestyle)
- Fix line length violations (88 chars)
- Fix whitespace issues
- Fix indentation problems

### Code Quality (Pyflakes)
- Remove unused imports
- Fix undefined names
- Fix unused variables (allow `_` prefix)

## hygge-Specific Considerations

- **Don't break functionality**: Only fix linting, not logic
- **Maintain backward compatibility**: Preserve existing APIs and behavior unless there's a clear discussion about breaking changes
- **Preserve hygge patterns**: Maintain existing code structure
- **Keep async patterns**: Don't change async/await usage
- **Maintain Polars usage**: Don't change Polars API calls
- **Preserve type hints**: Keep Pydantic and type annotations

## Output

After fixing:
1. List all issues that were fixed
2. List any issues that need manual attention
3. Verify code still follows hygge principles
4. Ensure tests still pass (if applicable)

## Manual Review Needed

Some issues may require manual review:
- Complex refactoring opportunities
- Logic changes (not just formatting)
- Architecture decisions
- **Breaking changes or backward compatibility concerns** - These require explicit discussion before implementation

Flag these for the developer to review rather than auto-fixing.
