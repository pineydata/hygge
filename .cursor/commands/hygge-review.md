# hygge Review

Review code and APIs from hygge's perspective: comfort, simplicity, and natural flow. Be direct and candid - does this feel right?

## Review Mindset

Think like you're reviewing code for a cozy framework. Ask: **Does this feel good to use?** Be honest - "This feels awkward" is more helpful than "You might consider..."

## API Review

### Does It Feel Natural?
- [ ] Method names read like English (`flow.run()` not `FlowExecutor.execute()`)
- [ ] Minimal configs "just work" with smart defaults
- [ ] Error messages are helpful and actionable ("key_columns is required" not "ValidationError")
- [ ] Uses `home`/`store` terminology (not `from`/`to`)

### Naming & Terminology
- [ ] Simple, direct names: "deletions" not "deletion_detection"
- [ ] Natural verbs: "find" not "detect", "run" not "execute"
- [ ] Matches existing hygge patterns in the codebase

### Configuration
- [ ] Convention over configuration: smart defaults, minimal setup
- [ ] Clear errors: tells you exactly what to fix
- [ ] No choice paralysis: obvious choices, not endless options

## Code Style

### Pythonic Essentials
- [ ] f-strings (not `.format()` or `%`)
- [ ] `pathlib.Path` (not string paths)
- [ ] Type hints on public APIs
- [ ] `async with` for resources
- [ ] Comprehensions where natural

### Anti-Patterns to Avoid
- [ ] No `isinstance()` overuse (use duck typing/protocols)
- [ ] No bare `Exception` catches
- [ ] No `== None` (use `is None`)
- [ ] No mutable default arguments
- [ ] No string concatenation with `+` (use f-strings)

### Code Quality
- [ ] Reads like well-written English
- [ ] Explicit intent (clear what it does)
- [ ] Simple solutions (not over-engineered)
- [ ] Files not too big (split if >1500 lines)

## hygge Principles

- [ ] **Comfort over complexity**: Feels natural, not over-engineered
- [ ] **Flow over force**: Data moves smoothly, no friction
- [ ] **Clarity over cleverness**: Simple and obvious, not clever
- [ ] **Convention over configuration**: Smart defaults, minimal setup
- [ ] **Reliability over speed**: Robust and predictable

## Architecture Consistency

- [ ] Polars everywhere (not pandas)
- [ ] Pydantic for configs
- [ ] Custom exceptions (`HomeError`, `StoreError`, `FlowError`)
- [ ] Async/await for I/O
- [ ] Matches existing patterns in codebase

## Quick Checks

- [ ] **DRY**: No duplicated logic that could be extracted?
- [ ] **KISS**: Is this the simplest solution?
- [ ] **YAGNI**: Solving current problem, not hypothetical future?
- [ ] **Backward compatibility**: Maintained unless explicitly discussed?

## Review Process

1. **Read the code**: Does it feel comfortable?
2. **Try the API mentally**: Would you want to use this?
3. **Check patterns**: Does it match hygge's style?
4. **Question complexity**: Is there a simpler way?

## Review Output

### What Feels Good
- What's comfortable and natural
- What makes this feel hygge-esque

### What Feels Off
- APIs that feel awkward
- Code that's too complex or clever
- Patterns that don't match hygge

### Specific Issues
- API issues: method names, config patterns, error messages
- Code style: Pythonic violations, anti-patterns
- Architecture: inconsistencies with hygge patterns

### Suggestions
- Concrete improvements with rationale
- Simpler alternatives if applicable

### Overall
**FEELS GOOD** | **NEEDS POLISH** | **FEELS OFF** | **NOT HYGGE**

Be direct: "This feels awkward" not "You might consider..."
