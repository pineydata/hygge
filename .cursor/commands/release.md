# Release Process

Guide through the complete release process for hygge, including local preparation, GitHub release creation, and post-release cleanup.

## Release Workflow Overview

The release process involves two main phases (plus optional post-release):
1. **Prep PR** (if main is protected): Create PR with version bump and README updates
2. **GitHub Release**: Create tag, push, create GitHub release with auto-generated notes
3. **Post-Release** (Optional): Update version back to dev version if you maintain dev versions

## GitHub Automatic Release Notes

**We use GitHub's automatic release notes** - no manual CHANGELOG.md maintenance required.

**Benefits:**
- ✅ **Always accurate**: Automatically includes all merged PRs since last release
- ✅ **Better categorization**: Groups by PR labels (enhancement, bug, breaking, etc.)
- ✅ **Automatic links**: Links to PRs and commits automatically
- ✅ **Contributor credits**: Automatically lists contributors
- ✅ **No maintenance**: No risk of forgetting to update changelog

**To get the best results**, use PR labels when merging PRs:
- `enhancement` or `feature` - for new features
- `bug` or `fix` - for bug fixes
- `breaking` - for breaking changes
- `documentation` - for docs updates

GitHub will automatically categorize and group changes based on these labels when generating release notes.

## Phase 1: Prep PR

Since your `main` branch is protected (requires PRs for all changes), create a prep PR with version and README updates before creating the release.

### Step 1: Determine New Version

Check current version and decide on new version (following [Semantic Versioning](https://semver.org/)):
- **MAJOR** (1.0.0): Breaking changes
- **MINOR** (0.5.0): New features, backward compatible
- **PATCH** (0.4.1): Bug fixes, backward compatible

**Current version location**: `pyproject.toml` (line 3)


**Action**: Ask user what version number they want to release (e.g., "0.6.0")

### Step 2: Create Release Prep Branch

Create a branch for the release prep:

```bash
git switch -c release/prep-vX.Y.Z
```

**Action**: Create release prep branch

### Step 3: Update Version in pyproject.toml

Update the version in `pyproject.toml`:

```python
# In pyproject.toml, line 3:
version = "0.4.0"  # Change to new version, e.g., "0.5.0"
```

**Action**: Update `pyproject.toml` with new version number

### Step 4: Review and Update README.md (if needed)

Review `README.md` to ensure it reflects new features and capabilities:

**Check for:**
- [ ] New features documented (if major features were added)
- [ ] Examples updated (if new capabilities need examples)
- [ ] Installation instructions current (if installation changed)
- [ ] Feature lists accurate (if new homes/stores were added)
- [ ] CLI commands documented (if new commands were added)

**Common updates:**
- Add new home/store types to "Supported Data Sources & Destinations"
- Add new CLI options to "CLI Commands" section
- Update examples if new patterns were introduced
- Add new workflows to "Common Workflows" section

**Action**: Review README.md and update if new features need documentation. If no changes needed, skip this step.

### Step 5: Commit and Push Prep Branch

Create a commit with the version updates:

```bash
git add pyproject.toml
# If README.md was updated, include it:
# git add README.md
git commit -m "chore: bump version to X.Y.Z"
git push origin release/prep-vX.Y.Z
```

**Action**: Commit and push prep branch

### Step 6: Create Prep PR

Create a pull request for the release prep:

1. **Title**: `chore: prepare release vX.Y.Z`
2. **Description**:
   ```markdown
   Prep PR for vX.Y.Z release.

   - Updated version in pyproject.toml
   - [Add any README updates if applicable]

   After merge, we'll create the release tag and GitHub release.
   ```
3. **Merge**: Merge the PR (or have it reviewed and merged)

**Action**: Create and merge prep PR

### Step 7: Pull Latest Main

After the prep PR is merged, pull the latest main:

```bash
git checkout main
git pull origin main
```

**Action**: Pull latest main with version bump

## Phase 2: GitHub Release

### Step 8: Verify Everything is Ready

Check that:
- [ ] All tests pass: `pytest`
- [ ] No uncommitted changes
- [ ] Version number is correct in `pyproject.toml`
- [ ] Prep PR is merged (if main is protected)
- [ ] You're on main branch with latest changes

**Action**: Verify readiness before proceeding to GitHub

### Step 9: Create and Push Git Tag

Create an annotated tag for the release:

```bash
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z
```

**Tag format**: `vX.Y.Z` (e.g., `v0.5.0`)

**Action**: Create and push tag

### Step 10: Create GitHub Release

Go to GitHub and create a release:

1. **Navigate to**: <https://github.com/pineydata/hygge/releases/new>
2. **Tag**: Select the tag you just pushed (`vX.Y.Z`)
3. **Title**: `vX.Y.Z` (or more descriptive if preferred)
4. **Generate Release Notes** (Recommended):
   - Click "Generate release notes" button
   - GitHub will automatically:
     - List all merged PRs since last release
     - Group by labels (if you use labels like `enhancement`, `bug`, `breaking`)
     - Include contributors
     - Link to PRs and commits
   - Review and edit the generated notes if needed
5. **OR Manual Description** (If you prefer):
   - Or manually write release notes if you prefer
6. **Publish**: Click "Publish release"

**Pro Tip**: To get better automatic release notes, use PR labels:
- `enhancement` or `feature` - for new features
- `bug` or `fix` - for bug fixes
- `breaking` - for breaking changes
- `documentation` - for docs updates

**Action**: Create GitHub release using automatic release notes (recommended) or manual description

## Phase 3: Post-Release (Optional)

**Note**: This phase is optional. If you don't maintain dev versions (e.g., `0.5.1.dev0`), you can skip this and leave the version at the release version until the next release.

### Step 11: Update Version Back to Dev (Optional)

If you want to maintain dev versions, update version in `pyproject.toml` to next dev version:

```python
# In pyproject.toml, line 3:
version = "0.5.0"  # Change to "0.5.1.dev0" or next planned version
```

**Convention**: Use `.dev0` suffix for development versions (e.g., `0.5.1.dev0`)

**When to skip**: If you don't maintain dev versions, leave the version at the release version (e.g., `0.5.0`) until the next release.

**Action**: Update version to dev version (or skip if not maintaining dev versions)

### Step 12: Commit Dev Version (Optional)

**If main is protected**, create another PR for the dev version bump:

```bash
git checkout -b release/dev-version-bump
git add pyproject.toml
git commit -m "chore: bump version to X.Y.Z.dev0 for development"
git push origin release/dev-version-bump
```

Then create a PR and merge it.

**If main is not protected**, commit directly:

```bash
git add pyproject.toml
git commit -m "chore: bump version to X.Y.Z.dev0 for development"
git push origin main
```

**Action**: Commit and push dev version (via PR if main is protected), or skip if not maintaining dev versions

## Release Checklist

Before starting, ensure:
- [ ] All tests pass
- [ ] All changes are committed
- [ ] You know what version number to release
- [ ] You understand if main is protected (requires prep PR)

During release:
- [ ] Prep PR created and merged (if main is protected)
- [ ] Version updated in `pyproject.toml`
- [ ] README.md reviewed and updated (if needed)
- [ ] Git tag created and pushed
- [ ] GitHub release created with auto-generated release notes

After release (optional):
- [ ] Version updated back to dev in `pyproject.toml` (if maintaining dev versions)
- [ ] Dev version commit/PR created and merged (if maintaining dev versions)

## Common Issues & Solutions

### Issue: Tag already exists
**Solution**: Delete local and remote tag, then recreate:
```bash
git tag -d vX.Y.Z
git push origin :refs/tags/vX.Y.Z
# Then recreate tag
```

### Issue: Forgot to update README
**Solution**:
- If prep PR is not merged yet: Update README in the prep PR branch
- If prep PR is already merged: Create a new PR with README updates, or update in the dev version PR

### Issue: Wrong version number
**Solution**: Update version, amend commit:
```bash
# Edit pyproject.toml
git add pyproject.toml
git commit --amend --no-edit
git push origin main --force-with-lease
# Delete and recreate tag if already pushed
```

## GitHub Automatic Release Notes

GitHub automatically generates release notes from:
- Merged pull requests since last release
- PR labels (if used) for categorization
- Contributors
- Links to PRs and commits

**To enable better categorization**, use PR labels:
- `enhancement` or `feature` → appears in "What's Changed" as new features
- `bug` or `fix` → appears as bug fixes
- `breaking` → highlighted as breaking changes
- `documentation` → appears as documentation updates

## hygge-Specific Notes

- **Comfort over complexity**: Keep release process simple and clear
- **Reliability**: Double-check version numbers
- **Flow**: Release process should feel smooth, not forced

## Quick Reference Commands

```bash
# Check current version
grep "^version" pyproject.toml

# Check git status
git status

# Run tests
pytest

# Create prep branch (if main is protected)
git checkout -b release/prep-vX.Y.Z

# Update version and commit
git add pyproject.toml README.md  # Include README.md if updated
git commit -m "chore: bump version to X.Y.Z"
git push origin release/prep-vX.Y.Z

# After prep PR is merged, create tag
git checkout main
git pull origin main
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z

# Create and push tag
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z
git push origin main

# Update to dev version (optional - only if maintaining dev versions)
# Edit pyproject.toml to X.Y.Z.dev0
git add pyproject.toml
git commit -m "chore: bump version to X.Y.Z.dev0 for development"
git push origin main
```

## Process Summary

**If main is protected:**
1. **Prep PR**: Create branch → Update version → Update README → Commit → Push → Create PR → Merge
2. **Release**: Pull main → Create tag → Push tag → Create GitHub release (with auto-generated notes)
3. **Post (Optional)**: Create branch → Update to dev version → Commit → Push → Create PR → Merge (only if maintaining dev versions)

**If main is not protected:**
1. **Local**: Update version → Update README → Commit → Push
2. **Release**: Create tag → Push tag → Create GitHub release (with auto-generated notes)
3. **Post (Optional)**: Update to dev version → Commit → Push (only if maintaining dev versions)

**Key Benefits**:
- No CHANGELOG.md maintenance - GitHub auto-generates release notes
- Prep PRs handle protected branches gracefully
- Always up-to-date release notes from merged PRs

**Remember**: The back-and-forth is normal - prepare locally, push to GitHub, then optionally update locally again for dev (if maintaining dev versions).

Guide the user through each step, asking for confirmation before proceeding to the next phase. Be helpful and check for common mistakes (wrong version format, forgetting prep PR if main is protected, etc.).
