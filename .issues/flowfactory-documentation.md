---
title: FlowFactory Documentation
priority: low
status: deferred
---

### Problem

The Flow constructor requires `entity_name` and fails if not provided, enforcing FlowFactory as the canonical creation path.

### Reality Check: CLI is Primary Interface

**hygge users interact via CLI (`hygge go`) with YAML configs.** They never:
- Create Flow instances directly
- See FlowFactory
- Need to know about `entity_name`

The Coordinator handles all of this internally. This is an **internal implementation detail**, not a user-facing concern.

### When This Matters

Only if hygge expands to support:
- Library/SDK usage (programmatic flow creation)
- Plugin development
- Advanced scripting beyond CLI

### Current Impact

- **Not a breaking change for CLI users** - they're unaffected
- **Internal enforcement** - ensures flows have proper context for journaling/tracking
- **Self-documenting** - error message explains what to do if someone does hit it

### If Programmatic Usage Becomes a Thing

If hygge later supports library usage, consider:
1. `FlowFactory.simple()` convenience method for scripts
2. Documentation for programmatic patterns
3. Examples in `examples/` directory

### Decision

**Defer.** CLI-only users don't need this. Revisit if/when programmatic usage becomes a supported use case.

### Related

- `src/hygge/core/flow/flow.py` - Flow class
- `src/hygge/core/flow/factory.py` - FlowFactory class
