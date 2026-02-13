# Code Review

Review code for DRY, KISS, YAGNI, and hygge principles. Be direct and candid.

## Review Checklist

### DRY (Don't Repeat Yourself)
- [ ] Similar code duplicated elsewhere? Extract to helper/base class?
- [ ] Overriding methods call `super()` instead of duplicating?
- [ ] Opportunities to reuse existing methods?

### KISS (Keep It Simple, Stupid)
- [ ] Simplest solution that solves the problem?
- [ ] Complex logic broken into smaller pieces?
- [ ] Readable by someone who didn't write it?
- [ ] Unnecessary abstractions or over-engineering?

### YAGNI (You Aren't Gonna Need It)
- [ ] Functionality added "just in case" vs. concrete need?
- [ ] Solves current problem, not hypothetical future?
- [ ] Premature optimization or unnecessary complexity?

### Second-Guess Fallbacks (CRITICAL)
**Second-guess fallbacks are the lurking missing commas of implementations** — hard to find and debug. Only allow in strictest circumstances.

- [ ] Fallback behaviors that "second guess" what should happen?
- [ ] Code silently falls back instead of failing clearly?
- [ ] Multiple code paths based on implicit assumptions?
- [ ] Would fallback be hard to debug if triggered unexpectedly?
- [ ] Fallback explicitly documented and justified?
- [ ] Could code fail fast and clearly instead?

**Red flags:**
- Silent fallbacks that mask real problems
- "Try this, but if it fails, try that" without clear error handling
- Implicit assumptions about what user "probably meant"
- Multiple layers of fallback logic
- Fallbacks that make debugging harder

**Acceptable (strict circumstances only):**
- Explicit, documented fallbacks with clear error logging
- Fallbacks part of public API contract
- Fallbacks that fail fast after attempting
- Fallbacks that are testable and predictable

**When in doubt: FAIL FAST. Let the error surface clearly.**

### hygge Principles
- [ ] **Convention over Configuration**: Smart defaults vs. complex configs?
- [ ] **Comfort Over Complexity**: API feels natural and comfortable?
- [ ] **Flow Over Force**: Data moves smoothly without friction?
- [ ] **Reliability Over Speed**: Behavior robust and predictable?
- [ ] **Clarity Over Cleverness**: Code simple and clear?

### Architecture & Compatibility
- [ ] Maintains consistency with existing patterns?
- [ ] Fits broader architecture?
- [ ] Uses `home`/`store` terminology (not `from`/`to`)?
- [ ] Follows Rails-inspired development style?
- [ ] **Backward compatible** unless explicit discussion of breaking changes?
- [ ] Existing APIs preserved?
- [ ] Breaking changes clearly documented?

### Code Quality
- [ ] Pydantic models for validation?
- [ ] Async/await for I/O operations?
- [ ] Polars for all data operations (not pandas)?
- [ ] Custom exceptions (`HomeError`, `StoreError`, `FlowError`)?
- [ ] Graceful error handling with retry decorators?

### Pythonic Standards
- [ ] **Type hints** on public APIs?
- [ ] **f-strings** for formatting (not `.format()` or `%`)?
- [ ] **pathlib.Path** instead of string paths?
- [ ] **Context managers** (`async with`) for resources?
- [ ] **EAFP** (try/except) vs. excessive preconditions?
- [ ] **Duck typing** (protocols) vs. `isinstance()` checks?
- [ ] **Comprehensions/generators** for transformations?
- [ ] **Pydantic/dataclasses** for data structures?
- [ ] **Enums** for constants (not magic strings)?
- [ ] **Property decorators** for computed attributes?

**Anti-patterns to avoid:**
- Overusing `isinstance()` — use duck typing
- Manual iteration with indices — use `enumerate()` or direct iteration
- String concatenation with `+` — use f-strings or `.join()`
- Catching bare `Exception` — catch specific exceptions
- Using `== None` — use `is None` or `is not None`
- Mutable default arguments — use `None` defaults with assignment in body
- Importing with `*` — use explicit imports
- Not using context managers — always use `with` for resources

## Review Process

1. **Search for similar patterns** before suggesting changes
2. **Identify duplication** and suggest extraction
3. **Question complexity** — simpler way?
4. **Hunt for second-guess fallbacks** — dangerous and hard to debug
5. **Verify hygge principles** — natural and comfortable?
6. **Check architecture** — fits existing structure?

## Review Output

Provide:
- **Strengths**: What's working well
- **Issues**: Specific violations of DRY/KISS/YAGNI or hygge principles
- **Fallback Warnings**: Second-guess fallbacks to remove or make explicit
- **Suggestions**: Concrete improvements with code examples
- **Questions**: Areas needing clarification
