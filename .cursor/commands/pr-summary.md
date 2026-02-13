# Generate PR Summary

Generate a concise PR summary (40-60 lines) focusing on **what changed and why**, not every implementation detail.

**Output**: Write to `.progress/PR_SUMMARY.md` (overwrite existing).

## Format

```markdown
---
title: PR Summary - [Brief Description]
tags: [bug, fix]  # or [enhancement, feature], [breaking], [documentation]
---

## Overview

- [Main change 1 with user impact]
- [Main change 2 with user impact]
- [Main change 3 if applicable]

## Key Changes

### [Feature/Component Name]

- `path/to/file.py`:
  - [What changed and why]
  - [Key detail if relevant]

### [Another Feature/Component]

- `path/to/file.py`:
  - [Changes...]

### Tests

- `tests/path/to/test_file.py`:
  - [What is tested]
  - [Coverage improvements]

## Testing

- All tests passing: `pytest` ([X] tests collected, all passing)
- [Any specific test coverage notes]
```

## Guidelines

### Structure
- **Overview**: 2-4 bullet points (user-facing impact)
- **Key Changes**: 3-5 feature sections, 2-4 bullets per file
- **Testing**: 1-3 lines summarizing coverage
- **Total**: 40-60 lines — if longer, summarize more aggressively

### What to Include
- ✅ User-facing changes (what this enables/fixes)
- ✅ Architectural changes (new patterns, significant refactors)
- ✅ Error handling improvements
- ✅ Breaking changes
- ✅ Major test additions

### What to Skip
- ❌ Implementation details (unless architecturally significant)
- ❌ Minor refactors (cleanup, renaming, reorganization)
- ❌ Formatting changes (imports, whitespace)
- ❌ Every file changed (group related files)
- ❌ Every method added (summarize functionality)

### Summarization
1. **Combine similar changes**: "Updated all Store implementations to add cleanup_staging()" not listing each
2. **Focus on outcomes**: "Improved error messages" not "Updated 5 exception classes"
3. **Group by feature**: Multiple files for one feature? One section with grouped list
4. **Skip the obvious**: Don't mention "Added tests" if obvious from context

### Organization
- Group related changes together
- Use descriptive section headers (e.g., "Flow Retry & Error Handling")
- Limit to 3-5 major feature areas (group if more)
- Per file: 2-4 bullet points max
- Focus on **behavior changes**, not code structure

## PR Labels

Add appropriate tags to frontmatter and remind to add GitHub labels:
- `enhancement` / `feature` — new features
- `bug` / `fix` — bug fixes
- `breaking` — breaking changes
- `documentation` — docs updates

GitHub uses these to auto-categorize release notes.

## Process

1. Review changed files → group by feature
2. Identify user impact → what does this enable/fix?
3. Note testing → what tests added/modified?
4. Check hygge principles → comfort, reliability, clarity?
5. Generate summary → concise, clear, organized

**Remember**: Help reviewers quickly understand what changed and why. Keep it cozy, not comprehensive.
