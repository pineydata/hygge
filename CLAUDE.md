# hygge Project Guide

A cozy, comfortable data movement framework that makes data feel at home.

## Critical Constraints

**Always ask permission before running terminal commands** - Request explicit approval before executing any commands that could affect the system. Provide guidance and code suggestions, but ask before running commands.

## Project Overview

hygge (pronounced "hoo-ga") is a data movement framework for solo developers/small teams. **Not a commercial product** - focus on comfort over enterprise rigor.

### Core Architecture
- **Home**: Data source (parquet files, SQL databases)
- **Store**: Data destination (parquet files, cloud storage)
- **Flow**: Single data movement from Home to Store
- **Coordinator**: Manages multiple flows from YAML config

## Philosophy & Principles

### hygge Core Values
- **Comfort**: Data should feel at home wherever it lives
- **Simplicity**: Clean, intuitive APIs that feel natural
- **Reliability**: Robust, predictable behavior without surprises
- **Flow**: Smooth, efficient movement without friction

### Rails-Inspired Development Principles

#### Core Principles
1. **Convention over Configuration**: Smart defaults, minimal setup
2. **Programmer Happiness**: APIs should feel natural and comfortable
3. **Separation of Concerns**: Clean architecture with configs/ subdirectories
4. **Comfort Over Complexity**: APIs should feel natural
5. **Flow Over Force**: Data should move smoothly between systems
6. **Reliability Over Speed**: Prefer robust, predictable behavior
7. **Clarity Over Cleverness**: Simple, clear code over complex optimizations

#### Detailed Rails Philosophy for hygge
1. **Optimize for Programmer Happiness**
   - APIs should feel natural and comfortable
   - Code should make developers smile
   - **hygge application**: Make data movement feel natural, not forced

2. **Convention over Configuration**
   - Smart defaults eliminate repetitive decisions
   - "You're not a beautiful and unique snowflake" - embrace conventions
   - **hygge application**: `home: path` instead of complex configs

3. **The Menu is Omakase**
   - Curated stack decisions, not endless choice paralysis
   - **hygge application**: Polars + PyArrow + Pydantic by default

4. **No One Paradigm**
   - Use the right tool for each job, not one-size-fits-all
   - **hygge application**: Async patterns for I/O, classes for models, functions for utilities

5. **Exalt Beautiful Code**
   - Aesthetically pleasing code is valuable
   - **hygge application**: `flow.run()` feels natural, not `FlowExecutor.execute(flow_instance)`

6. **Provide Sharp Knives**
   - Trust programmers with powerful tools
   - **hygge application**: Allow custom Home/Store implementations

7. **Value Integrated Systems**
   - Majestic monoliths over premature microservices
   - **hygge application**: Single framework for data movement, not scattered tools

8. **Progress over Stability**
   - Evolution keeps frameworks relevant
   - **hygge application**: Keep improving the config system and error handling

9. **Push up a Big Tent**
   - Welcome disagreement and diversity of thought
   - **hygge application**: Support different data sources and use cases

## Development Guidelines

### Code Standards
- **Rails-inspired**: Convention over configuration
- **Pythonic**: Follow Zen of Python principles and Pythonic patterns
- **Clean separation**: Configs in `configs/` subdirectories
- **Type safety**: Pydantic validation throughout
- **Async patterns**: Use async/await for I/O operations

### Code Quality & Review Principles

**DO NOT RUSH CHANGES** - Always consider the greater codebase and adhere to strong development standards:

1. **DRY (Don't Repeat Yourself)**
   - Extract duplicated logic into helper methods or base classes
   - If you're copying code, stop and refactor
   - Before adding code, check if similar logic already exists elsewhere
   - When overriding methods, prefer calling `super()` over duplicating logic

2. **KISS (Keep It Simple, Stupid)**
   - Prefer simple, clear solutions over clever ones
   - Complex logic should be broken into smaller, understandable pieces
   - If a solution feels convoluted, there's probably a simpler way
   - Code should be readable by someone who didn't write it

3. **YAGNI (You Aren't Gonna Need It)**
   - Don't add functionality "just in case"
   - Solve the current problem, not hypothetical future problems
   - Avoid over-engineering or premature optimization
   - Add complexity only when there's a concrete need

4. **Backward Compatibility**
   - **Maintain backward compatibility unless there is a clear discussion** about breaking changes
   - Preserve existing APIs and behavior
   - Breaking changes require explicit discussion and justification
   - Document and communicate breaking changes clearly

**Code Review Checklist:**
- [ ] Have I checked if similar code exists elsewhere in the codebase?
- [ ] Am I duplicating logic that could be extracted or reused?
- [ ] Is this the simplest solution that solves the problem?
- [ ] Am I adding unnecessary complexity or abstractions?
- [ ] Does this change maintain consistency with existing patterns?
- [ ] Have I considered how this fits into the broader architecture?
- [ ] Does this maintain backward compatibility, or is there a clear discussion about breaking changes?

**When making changes:**
- Search the codebase for similar patterns before implementing
- Look for opportunities to reuse existing methods or extract helpers
- Question every line of code - does it need to be there?
- Refactor during review, not after - fix issues as you find them

### Pythonic Development Principles

hygge code should be **Pythonic** - following Python's idioms, conventions, and philosophy. Pythonic code feels natural to Python developers and reads like well-written English.

#### Core Zen of Python Principles

1. **Beautiful is Better Than Ugly**: Code should be aesthetically pleasing (`flow.run()` not `FlowExecutor.execute()`)
2. **Explicit is Better Than Implicit**: Make intentions clear (`home: path` explicitly declares source)
3. **Simple is Better Than Complex**: Prefer straightforward solutions with smart defaults
4. **Complex is Better Than Complicated**: Some problems require complexity, but keep it organized (Polars + PyArrow integration)
5. **Flat is Better Than Nested**: Keep structures flat (`configs/` subdirectories, not deep nesting)
6. **Sparse is Better Than Dense**: Use whitespace and clear formatting
7. **Readability Counts**: Code is read more often than written (`Home`, `Store`, `Flow` are self-documenting)
8. **Special Cases Aren't Special Enough**: Unified interfaces work for all types, not special cases
9. **Practicality Beats Purity**: Async where needed, sync where simpler, not dogmatic
10. **Errors Should Never Pass Silently**: Fail fast with clear messages (`HomeError`, `StoreError`, `FlowError`)
11. **In the Face of Ambiguity, Refuse to Guess**: Pydantic validation rejects ambiguous configs
12. **One Obvious Way**: `flow.run()` is the obvious way, not multiple execution methods
13. **If Hard to Explain, it's a Bad Idea**: Core logic should be explainable
14. **Namespaces are One Honking Great Idea**: `hygge.core`, `hygge.homes`, `hygge.stores` provide clear namespaces

#### Pythonic Patterns for hygge

- **Type Hints**: Full type hints on all public APIs and configs
- **f-strings**: Use f-strings for all string formatting (not `.format()` or `%`)
- **pathlib.Path**: Use `Path` objects for file paths, not string paths
- **Context Managers**: Use `async with` for resource management (`async with home:`, `async with store:`)
- **EAFP (Easier to Ask for Forgiveness)**: Try operations, handle specific exceptions, provide helpful messages
- **Duck Typing**: Use protocols for interfaces (`Home`, `Store`), not `isinstance()` checks
- **Comprehensions & Generators**: Use for transformations and streaming data batches
- **Pydantic Models**: Use for all configuration with automatic validation
- **Enums**: Use for constants (store types, compression options, etc.)
- **Property Decorators**: Use `@property` for computed attributes (`flow.status`, `store.size`)
- **Dataclasses**: Use for simple data containers, Pydantic for configs

#### Pythonic Anti-Patterns to Avoid

- Overusing `isinstance()` checks - use duck typing and protocols instead
- Manual iteration with indices - use `enumerate()` or direct iteration
- String concatenation with `+` - use f-strings or `.join()`
- Catching bare `Exception` - catch specific exceptions
- Using `== None` or `!= None` - use `is None` or `is not None`
- Mutable default arguments - use `None` and assign in function body
- Importing with `*` - use explicit imports
- Not using context managers - always use `with` for resources
- Premature optimization - write clear code first, optimize if needed
- Ignoring type hints - use type hints for clarity and tooling

#### Pythonic + Rails Philosophy Alignment

Many Pythonic principles align beautifully with Rails philosophy:

- **Beautiful code** (Python) ↔ **Exalt Beautiful Code** (Rails)
- **Simple is better** (Python) ↔ **Convention over Configuration** (Rails)
- **One obvious way** (Python) ↔ **The Menu is Omakase** (Rails)
- **Readability counts** (Python) ↔ **Optimize for Programmer Happiness** (Rails)
- **Practicality beats purity** (Python) ↔ **No One Paradigm** (Rails)
- **Namespaces** (Python) ↔ **Value Integrated Systems** (Rails)

hygge benefits from both philosophies: Pythonic clarity and Rails-inspired comfort.

### Configuration System
- Centralized defaults in individual config classes
- Smart defaults with validation
- Support both minimal (`home: path`) and advanced configs
- Remove redundant settings files
- Always use `home`/`store` terminology (not `from`/`to`)

### Error Handling
- Custom exception hierarchy: `HomeError`, `StoreError`, `FlowError`
- Retry decorator with exponential backoff
- Graceful failure handling

### Testing Approach
- **Test immediately after functionality**: Write tests as soon as you implement features
- **Focus on behavior that matters**: Test user experience and data integrity
- **Verify defaults "just work"**: Ensure smart defaults function correctly
- **Don't compromise codebase for tests**: Keep tests simple and maintainable
- **Test the happy path first**: Ensure basic functionality works before edge cases
- **Test error scenarios**: Verify graceful failure handling
- **Integration over unit tests**: Focus on end-to-end behavior that users care about


## Technical Stack

### Core Data Movement Stack
hygge is **built on Polars + PyArrow** for optimal data movement performance:
- **Polars**: Primary API for all data operations - clean, fast, pythonic
- **PyArrow**: Columnar memory backend - efficient, zero-copy where possible
- **Why this choice**: Best balance of performance, developer experience, and database compatibility for E-L workflows
- **All data movement** happens through Polars DataFrames - this is a firm commitment, not a suggestion

### Supporting Technologies
- **Python 3.11+**: Modern async/await patterns
- **SQLAlchemy**: Database connectivity for SQL homes/stores (coming soon)
- **Pydantic**: Type safety and validation throughout
- **YAML**: Configuration format with smart defaults
- **Async/await patterns** for efficient I/O operations

## File Organization

```
src/hygge/core/
├── configs/           # Centralized configuration
├── homes/            # Data sources
│   └── configs/      # Home-specific configs
└── stores/           # Data destinations
    └── configs/      # Store-specific configs
```

## Configuration Examples

### Minimal Flow (Rails spirit)
```yaml
flows:
  users_to_parquet:
    home:
      type: parquet
      path: data/users.parquet
    store:
      type: parquet
      path: data/lake/users
    # That's it! Everything else uses smart defaults
```

### Advanced Configuration
```yaml
flows:
  users_to_parquet:
    home:
      type: sql
      table: users
      connection: ${DATABASE_URL}
      options:
        batch_size: 10000
    store:
      type: parquet
      path: data/lake/users
      options:
        compression: snappy
```

## Development Style

- **Be direct and candid** (not deferential)
- **Focus on hygge's ethos**: comfort, reliability, natural data movement
- **Prioritize user experience** over technical perfection
- **Follow Rails conventions**: convention over configuration
- **Think like a data engineer**: outcomes and impact matter more than perfection
- **Make progress, not perfection**: ship working solutions that solve real problems
- **Update progress tracking**: Always update HYGGE_DONE.md (append-only) and HYGGE_PROGRESS.md (evolving) when completing work

## Development Philosophy

- Keep it simple and focused
- Make common tasks easy
- Prioritize user experience
- Write clear, maintainable code
- Test immediately but sensibly - focus on behavior that matters
- Test the user experience - does it work as expected?
- **Focus on outcomes** - does this solve the user's problem?
- **Iterate and improve** - get something working, then make it better

**hygge isn't just about moving data - it's about making data movement feel natural, comfortable, and reliable.**

## Testing Checklist

Before considering any feature complete:
- [ ] Does the basic functionality work as expected?
- [ ] Are there tests that verify the core behavior?
- [ ] Do the tests focus on user experience and data integrity?
- [ ] Can a user successfully use this feature with minimal configuration?

**If any answer is "no", the feature is not complete.**

## Context Verification

Before starting work, ensure:
- [ ] You understand hygge's philosophy and constraints
- [ ] You're familiar with the Rails-inspired principles
- [ ] You will ask permission before running terminal commands
- [ ] You prioritize comfort over complexity
- [ ] You will write tests immediately after implementing functionality
- [ ] You understand that testing is part of the development process, not optional
- [ ] You think like a data engineer: outcomes and impact over perfection
- [ ] You focus on making progress and shipping working solutions

**If any answer is "no", ask the user to re-provide context before proceeding.**

## Current Status

See [HYGGE_PROGRESS.md](.progress/HYGGE_PROGRESS.md) for detailed assessment.

## Reference Materials

- **[README.md](README.md)** - Project documentation
- **[HYGGE_PROGRESS.md](.progress/HYGGE_PROGRESS.md)** - Evolving TODO tracking and progress assessment
- **[HYGGE_DONE.md](.progress/HYGGE_DONE.md)** - Append-only celebration of completed work and achievements
- **[.llm/](.llm/)** - Original context files (kept for reference)
  - `context.md` - Project overview, ethos, and current status
  - `development.md` - Code standards and implementation guidelines
  - `rails_philosophy.md` - Condensed Rails principles for hygge
  - `python_philosophy.md` - Pythonic principles and patterns for hygge

See the `samples/` directory for working examples and the `examples/` directory for programmatic usage.
