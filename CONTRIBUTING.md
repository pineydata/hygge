# Contributing to hygge

## Branch Protection

This repository uses branch protection to ensure code quality and prevent accidental commits to main.

### 🚫 What's Blocked
- **Direct pushes to main** - All changes must go through Pull Requests
- **Merging without review** - All changes require code review
- **Broken builds** - All tests must pass before merging

### ✅ Required Workflow

1. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes and commit**
   ```bash
   git add .
   git commit -m "Your commit message"
   ```

3. **Push your feature branch**
   ```bash
   git push origin feature/your-feature-name
   ```

4. **Create a Pull Request**
   - Go to GitHub and create a PR from your feature branch to main
   - Add a clear description of your changes
   - Request review from team members

5. **Get code review and merge**
   - Address any feedback from reviewers
   - Ensure all tests pass
   - Merge via the PR (not directly to main)

### 🛡️ Why This Matters

- **Code Quality**: All changes are reviewed before merging
- **Team Collaboration**: Everyone can see and discuss changes
- **History**: Clean commit history with meaningful PR descriptions
- **Safety**: Prevents accidental commits and broken builds

### 🚨 Emergency Fixes

If you need to make an emergency fix to main:
1. **Create a hotfix branch**: `git checkout -b hotfix/emergency-fix`
2. **Make minimal changes** and commit
3. **Create PR immediately** with clear explanation
4. **Get expedited review** from team members
5. **Merge via PR** (never push directly to main)

### 📝 Commit Messages

Use clear, descriptive commit messages:
- ✅ `feat: add entity-first approach for multi-table flows`
- ✅ `fix: resolve async method calls in MSSQL store`
- ❌ `fix stuff`
- ❌ `updates`

### 🧪 Testing

Before creating a PR:
- [ ] Run linting: `ruff check src/ samples/`
- [ ] Run tests: `pytest tests/`
- [ ] Test your changes manually
- [ ] Update documentation if needed

---

**Remember: The main branch is protected for everyone's benefit. Always use the PR workflow!** 🚀
