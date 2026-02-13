# Release Process

Guide through the release process for hygge: prep PR → GitHub release → optional dev version bump.

**Main is protected** — all changes go through PRs.

## Prerequisites

Before starting:
- [ ] All tests pass (`pytest`)
- [ ] All changes committed and pushed
- [ ] You know the target version number

**Action**: Ask the user what version to release (following [semver](https://semver.org/): MAJOR.MINOR.PATCH).

**Current version location**: `pyproject.toml` line 3

## Phase 1: Prep PR

### Step 1: Create prep branch and bump version

```bash
git switch -c release/prep-vX.Y.Z
```

Update `version` in `pyproject.toml` to the new version.

**Action**: Create branch, update version.

### Step 2: Review README.md

Quick scan — does the README reflect what's shipping? New homes/stores, CLI commands, examples? Update if needed, skip if not.

**Action**: Review and update README.md if needed.

### Step 3: Commit, push, and create PR

```bash
git add pyproject.toml  # include README.md if updated
git commit -m "chore: bump version to X.Y.Z"
git push origin release/prep-vX.Y.Z
```

Create PR titled `chore: prepare release vX.Y.Z`. Merge it.

**Action**: Commit, push, create PR, merge.

### Step 4: Pull latest main

```bash
git checkout main
git pull origin main
```

## Phase 2: Tag & GitHub Release

### Step 5: Verify and tag

```bash
# Sanity check
grep "^version" pyproject.toml
pytest
git status

# Tag and push
git tag -a vX.Y.Z -m "Release vX.Y.Z"
git push origin vX.Y.Z
```

**Action**: Verify version is correct, run tests, create and push tag.

### Step 6: Create GitHub release

Create release at <https://github.com/pineydata/hygge/releases/new>:
1. Select the tag `vX.Y.Z`
2. Title: `vX.Y.Z`
3. Click **Generate release notes** (auto-includes merged PRs, contributors, labels)
4. Review/edit, then **Publish release**

**Action**: Create GitHub release with auto-generated notes.

## Phase 3: Dev Version Bump (Optional)

Skip this if you don't maintain `.dev0` versions between releases.

```bash
git switch -c release/dev-version-bump
# Update pyproject.toml version to X.Y.Z.dev0
git add pyproject.toml
git commit -m "chore: bump version to X.Y.Z.dev0 for development"
git push origin release/dev-version-bump
```

Create PR, merge.

**Action**: Bump to dev version via PR, or skip.

## PR Labels for Better Release Notes

Use these labels on PRs for automatic categorization in release notes:
- `enhancement` / `feature` — new features
- `bug` / `fix` — bug fixes
- `breaking` — breaking changes
- `documentation` — docs updates

## Troubleshooting

**Tag already exists:**
```bash
git tag -d vX.Y.Z
git push origin :refs/tags/vX.Y.Z
# Recreate tag
```

**Wrong version number:**
```bash
# Fix pyproject.toml, then:
git add pyproject.toml
git commit --amend --no-edit
git push origin release/prep-vX.Y.Z --force-with-lease
# Delete and recreate tag if already pushed
```

**Forgot README updates:**
- Prep PR not merged yet? Update in the prep branch.
- Already merged? New PR, or fold into the dev version bump PR.
