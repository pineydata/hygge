# PyPI Publishing Process

Guide through the complete PyPI publishing process for hygge, including local preparation, TestPyPI testing, and production PyPI publishing. Prompt the user with commands to accept at each step.

## Publishing Workflow Overview

The publishing process involves three main phases:
1. **Prerequisites**: Set up PyPI accounts and API tokens
2. **TestPyPI**: Build and test on TestPyPI first (recommended)
3. **Production PyPI**: Publish to production PyPI

## Prerequisites

### Step 1: Create PyPI Accounts (if needed)

**TestPyPI** (for testing):
- URL: https://test.pypi.org/account/register/
- Create account if you don't have one

**PyPI** (production):
- URL: https://pypi.org/account/register/
- Create account if you don't have one

**Action**: Ask user if they have PyPI accounts. If not, provide links and wait for confirmation before proceeding.

### Step 2: Create API Tokens

**TestPyPI API Token:**
1. Go to: https://test.pypi.org/manage/account/token/
2. Click "Add API token"
3. Name: `hygge-testpypi` (or descriptive name)
4. Scope: "Entire account" (or project-specific if preferred)
5. Copy the token (starts with `pypi-`) - you'll only see it once!

**PyPI API Token:**
1. Go to: https://pypi.org/manage/account/token/
2. Click "Add API token"
3. Name: `hygge-pypi` (or descriptive name)
4. Scope: "Entire account" (or project-specific if preferred)
5. Copy the token (starts with `pypi-`) - you'll only see it once!

**Action**: Ask user if they have API tokens. If not, provide instructions and wait for confirmation. Remind them to save tokens securely.

### Step 3: Install Build Tools

Install required tools for building and publishing:

```bash
pip install build twine
```

**Action**: Prompt user to accept command: `pip install build twine`

### Step 4: Verify Package Configuration

Check that `pyproject.toml` is correctly configured:

- [ ] Package name is correct (currently `hygge`, will be `hygge-elk`)
- [ ] Version number is correct
- [ ] Description is accurate
- [ ] Author information is correct
- [ ] License is specified
- [ ] README is referenced

**Action**: Read `pyproject.toml` and verify configuration with user. Ask if package name should be updated to `hygge-elk` before proceeding.

## Phase 1: Build Package Locally

### Step 5: Clean Previous Builds

Remove any previous build artifacts:

```bash
rm -rf dist/ build/ *.egg-info
```

**Action**: Prompt user to accept command: `rm -rf dist/ build/ *.egg-info`

### Step 6: Build Package

Build source distribution and wheel:

```bash
python -m build
```

This creates:
- `dist/hygge-X.Y.Z.tar.gz` (source distribution)
- `dist/hygge-X.Y.Z-py3-none-any.whl` (wheel)

**Action**: Prompt user to accept command: `python -m build`

### Step 7: Verify Build Artifacts

Check that build artifacts were created:

```bash
ls -lh dist/
```

You should see:
- `.tar.gz` file (source distribution)
- `.whl` file (wheel)

**Action**: Prompt user to accept command: `ls -lh dist/` and verify artifacts exist

## Phase 2: Test on TestPyPI

### Step 8: Upload to TestPyPI

Upload to TestPyPI for testing:

```bash
twine upload --repository testpypi dist/*
```

**Credentials:**
- Username: `__token__`
- Password: Your TestPyPI API token (starts with `pypi-`)

**Action**: Prompt user to accept command: `twine upload --repository testpypi dist/*`. Remind them they'll be prompted for credentials (username: `__token__`, password: TestPyPI API token).

### Step 9: Verify TestPyPI Upload

Check that the package appears on TestPyPI:
- URL: https://test.pypi.org/project/hygge-elk/ (or current package name)

**Action**: Ask user to verify the package appears on TestPyPI. Provide the URL based on package name.

### Step 10: Test Installation from TestPyPI

Test installing from TestPyPI in a clean environment:

```bash
# Create a test virtual environment
python -m venv test_env
source test_env/bin/activate  # On Windows: test_env\Scripts\activate

# Install from TestPyPI
pip install --index-url https://test.pypi.org/simple/ hygge-elk

# Test the CLI
hygge --help

# Clean up
deactivate
rm -rf test_env
```

**Action**: Prompt user to accept commands step by step:
1. `python -m venv test_env`
2. `source test_env/bin/activate` (or Windows equivalent)
3. `pip install --index-url https://test.pypi.org/simple/ hygge-elk`
4. `hygge --help`
5. `deactivate`
6. `rm -rf test_env`

### Step 11: Verify Package Works

Test that the installed package works correctly:

- [ ] CLI command works: `hygge --help`
- [ ] Can import the module: `python -c "import hygge; print(hygge.__version__)"`
- [ ] Basic functionality works

**Action**: Prompt user to accept command: `python -c "import hygge; print(hygge.__version__)"` and ask if package works correctly.

## Phase 3: Publish to Production PyPI

### Step 12: Final Verification

Before publishing to production, verify:

- [ ] Package works correctly from TestPyPI
- [ ] Version number is correct
- [ ] All tests pass: `pytest`
- [ ] README renders correctly
- [ ] No sensitive information in package

**Action**: Ask user if they want to run tests. If yes, prompt: `pytest`. Then ask for confirmation that everything is ready for production publish.

### Step 13: Publish to Production PyPI

Upload to production PyPI:

```bash
twine upload dist/*
```

**Credentials:**
- Username: `__token__`
- Password: Your PyPI API token (starts with `pypi-`)

**Action**: Prompt user to accept command: `twine upload dist/*`. Remind them they'll be prompted for credentials (username: `__token__`, password: PyPI API token). **Important**: Confirm this is production PyPI before proceeding.

### Step 14: Verify Production Upload

Check that the package appears on PyPI:
- URL: https://pypi.org/project/hygge-elk/ (or current package name)

**Action**: Ask user to verify the package appears on PyPI. Provide the URL based on package name.

### Step 15: Test Production Installation

Test installing from production PyPI:

```bash
# Create a test virtual environment
python -m venv prod_test_env
source prod_test_env/bin/activate  # On Windows: prod_test_env\Scripts\activate

# Install from PyPI
pip install hygge-elk

# Test the CLI
hygge --help

# Clean up
deactivate
rm -rf prod_test_env
```

**Action**: Prompt user to accept commands step by step:
1. `python -m venv prod_test_env`
2. `source prod_test_env/bin/activate` (or Windows equivalent)
3. `pip install hygge-elk`
4. `hygge --help`
5. `deactivate`
6. `rm -rf prod_test_env`

## Post-Publishing

### Step 16: Update Documentation

If needed, update documentation with installation instructions:

```bash
pip install hygge-elk
```

**Action**: Ask user if documentation needs updating. If yes, help update README or docs.

### Step 17: Tag Release (if not already done)

If you haven't already created a git tag for this release:

```bash
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z
```

**Action**: Ask user if they want to create a git tag. If yes, prompt: `git tag -a vX.Y.Z -m "Release vX.Y.Z"` then `git push origin vX.Y.Z` (ask for version number first).

## Publishing Checklist

Before starting:
- [ ] PyPI accounts created (TestPyPI and PyPI)
- [ ] API tokens created and saved securely
- [ ] Build tools installed (`build`, `twine`)
- [ ] `pyproject.toml` is correctly configured
- [ ] Version number is correct
- [ ] All tests pass

During TestPyPI:
- [ ] Package built successfully
- [ ] Uploaded to TestPyPI
- [ ] Package appears on TestPyPI
- [ ] Installation from TestPyPI works
- [ ] Package functionality verified

During Production:
- [ ] Final verification complete
- [ ] Uploaded to PyPI
- [ ] Package appears on PyPI
- [ ] Installation from PyPI works
- [ ] Git tag created (if applicable)

## Common Issues & Solutions

### Issue: "Package already exists" on TestPyPI
**Solution**: TestPyPI allows re-uploads. You can upload the same version multiple times for testing.

### Issue: "Package already exists" on PyPI
**Solution**: PyPI doesn't allow re-uploads of the same version. You must:
- Bump the version number in `pyproject.toml`
- Rebuild the package
- Upload the new version

**Action**: If this happens, help user bump version and rebuild.

### Issue: "Invalid API token"
**Solution**:
- Verify you're using `__token__` as username
- Check that the token starts with `pypi-`
- Ensure token hasn't expired or been revoked
- Create a new token if needed

**Action**: Help user troubleshoot token issues.

### Issue: "Package name conflicts"
**Solution**:
- Check if package name is available on PyPI
- Consider using `hygge-elk` if `hygge` is taken
- Update `pyproject.toml` with new package name

**Action**: Help user check package name availability and update if needed.

### Issue: Build fails
**Solution**:
- Check `pyproject.toml` syntax
- Verify all required fields are present
- Ensure `build-system` is correctly configured
- Check for missing files referenced in `pyproject.toml`

**Action**: Help user troubleshoot build issues.

## Security Best Practices

1. **Never commit API tokens** to git
2. **Use API tokens**, not passwords, for authentication
3. **Use project-specific tokens** if possible (instead of account-wide)
4. **Rotate tokens** periodically
5. **Store tokens securely** (password manager, environment variables)

## Environment Variables (Optional)

You can store tokens as environment variables to avoid typing them:

```bash
# In ~/.bashrc or ~/.zshrc
export TESTPYPI_TOKEN="pypi-..."
export PYPI_TOKEN="pypi-..."

# Then use in commands:
twine upload --repository testpypi -u __token__ -p $TESTPYPI_TOKEN dist/*
twine upload -u __token__ -p $PYPI_TOKEN dist/*
```

**Action**: Ask user if they want to set up environment variables. If yes, help them configure.

## hygge-Specific Notes

- **Comfort over complexity**: Keep the publishing process simple and clear
- **Reliability**: Always test on TestPyPI first
- **Flow**: Publishing should feel smooth, not forced
- **Package name**: Currently `hygge`, will be `hygge-elk` for PyPI

## Process Summary

1. **Prerequisites**: Create accounts, get API tokens, install tools
2. **Build**: Clean, build package, verify artifacts
3. **TestPyPI**: Upload, verify, test installation
4. **Production**: Final verification, upload, verify, test installation
5. **Post**: Update docs, create git tag

**Key Principle**: Always test on TestPyPI first before publishing to production PyPI.

**Important**: Guide the user through each step, **prompting them with commands to accept** at each phase. Ask for confirmation before proceeding to the next step. Be helpful and check for common mistakes (wrong credentials, missing build artifacts, etc.).
