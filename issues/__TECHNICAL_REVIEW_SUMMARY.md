# hygge Roadmap: Current Status & Next Steps

**Last Updated:** Post-Entity Lifecycle Refactoring
**Status:** Production-ready for midmarket scale, actively improving

---

## Overview

This roadmap tracks hygge's development progress, current priorities, and planned improvements. hygge is a cozy, comfortable data movement framework designed for solo developers and small teams working at midmarket regional org scale.

**Current State:**
- Core architecture is clean and production-ready
- 610+ tests passing with comprehensive coverage
- Clear separation of concerns across all components
- Exception handling provides clear, actionable error messages
- Polisher feature enables lightweight data finishing

---

## Completed Work ✅

### Major Architectural Improvements

1. **Flow Creation Extraction**
   - FlowFactory successfully extracts all flow creation logic from Coordinator
   - Coordinator's `_create_flows()` reduced from ~300 lines to ~50 lines
   - Clean separation of concerns: FlowFactory handles creation, Coordinator handles orchestration
   - Production-ready with excellent testability and maintainability

2. **Entity Configuration Lifecycle**
   - Clear separation between flow templates (FlowConfig) and configured instances (Entity)
   - Single point of entity expansion in Workspace eliminates scattered logic
   - Lenient validation allows incomplete flow configs that are completed by entity overrides
   - Resolved validation timing issues that blocked Open Mirroring stores with entity-level `key_columns`
   - Architecture is simpler and more maintainable

3. **Error Handling Standardization**
   - Exception chaining preserves full stack traces for production debugging
   - Specific exception types (FlowError, HomeError, StoreError, JournalError) enable precise error handling
   - SQLSTATE-based connection error detection replaces brittle string matching
   - Critical for troubleshooting production issues at 2am in midmarket orgs

4. **Coordinator Simplification**
   - Workspace handles all configuration discovery and loading
   - Progress tracking and summary generation extracted to dedicated messaging utilities
   - Coordinator is now a pure orchestrator focused on flow execution
   - Significantly reduced complexity while maintaining all functionality

5. **Polisher Transform**
   - Lightweight last-mile data finishing before writes to stores
   - Column normalization (remove special chars, PascalCase, space removal)
   - Deterministic row-level hash ID generation from multiple columns
   - Generic constant columns and load timestamps
   - Integrated seamlessly into all store types with Open Mirroring compatibility

---

## Active Issues 🔄

### High Priority

1. **[Test Coverage Improvements](test-coverage-improvements.md)**
   - **Goal:** Establish visibility into test coverage and identify gaps
   - **Needs:**
     - Coverage reporting infrastructure (pytest-cov)
     - Large data volume tests approaching midmarket limits (100M+ rows)
     - Concurrent flow stress tests (10+ flows running simultaneously)
     - Connection pool exhaustion scenarios
   - **Why:** Foundational work for maintaining code quality as the framework grows
   - **Estimated Effort:** 1 day initial setup + incremental test additions

### Medium Priority

3. **[Store Interface Standardization](store-interface-standardization.md)**
   - **Goal:** Make optional store methods explicit with default implementations
   - **Current:** Stores use `hasattr()` checks for optional methods (configure_for_run, cleanup_staging, etc.)
   - **Proposed:** Base Store class provides default no-op implementations
   - **Why:** Improves type safety, developer experience, and API clarity
   - **Estimated Effort:** 1-2 days
   - **Impact:** Should be done before adding more store implementations

4. **[Watermark Tracker Extraction](watermark-tracker-extraction.md)**
   - **Goal:** Separate watermark tracking logic from flow execution
   - **Current:** Watermark logic is mixed with Flow's batch processing
   - **Proposed:** Dedicated `Watermark` class with upfront schema validation
   - **Why:** Improves testability, maintainability, and error experience
   - **Estimated Effort:** 2-3 days
   - **Impact:** Better separation of concerns, easier to test edge cases

### Low Priority

5. **[Mirror Journal Batching](mirror-journal-batching.md)**
   - **Goal:** Batch journal mirror writes to reduce Fabric churn
   - **Current:** Mirrored journal reloads after every entity completion
   - **Proposed:** Accumulate entity run notifications, publish once per flow run
   - **Why:** Performance optimization for flows with multiple entities
   - **Estimated Effort:** 1 day

6. **[Schema Manifest Improvements](schema-manifest-improvements.md)**
   - **Goal:** Extract reusable schema generation logic from OpenMirroringStore
   - **Current:** Schema generation tightly coupled to Open Mirroring store
   - **Proposed:** Shared helper for Polars dtype → Fabric schema mapping
   - **Why:** Enables other Fabric destinations to reuse schema generation
   - **Estimated Effort:** 1 day

### Deferred/Evaluation

7. **[MSSQL Python Migration](mssql-python-migration.md)**
   - **Status:** Decision to stay with `pyodbc`
   - **Rationale:** `mssql-python` lacks bulk copy operations critical for Store writes, provides no async advantage
   - **Action:** Monitor `mssql-python` development for bulk copy support
   - **Re-evaluate:** 12-24 months if bulk operations are added

---

## Roadmap Priorities

### Immediate Next Steps (Next 1-2 Weeks)

1. **[Add Test Coverage Reporting](test-coverage-improvements.md)**
   - Establish baseline coverage metrics to understand current state
   - Identify gaps in test coverage, especially edge cases and error paths
   - Add CI coverage reporting to prevent regressions
   - **Why:** Foundational for maintaining quality as the codebase grows

2. **Verify Resolved Issues**
   - Confirm Store Config Entity Inference is fully resolved
   - Confirm Flow Entity Separation is fully resolved
   - Close issues if verification passes
   - **Why:** Keep issue tracker clean and accurate

### Short-Term (Next 1-2 Months)

2. **[Store Interface Standardization](store-interface-standardization.md)**
   - Improve type safety and developer experience for store implementers
   - Should be completed before adding more store implementations
   - **Why:** Code quality improvement that makes the framework easier to extend

4. **[Watermark Tracker Extraction](watermark-tracker-extraction.md)**
   - Improve testability and maintainability of watermark logic
   - Better error experience with upfront validation instead of reactive warnings
   - **Why:** Code quality improvement that makes the framework easier to maintain

### Long-Term (Next 3-6 Months)

4. **Small Improvements** (As Needed)
   - [Mirror Journal Batching](mirror-journal-batching.md) for performance
   - [Schema Manifest Improvements](schema-manifest-improvements.md) for code reuse
   - **Why:** Incremental improvements that enhance the framework

---

## Architecture Status

### Current Architecture ✅

**Clean Separation of Concerns:**
- **Workspace:** Discovers configuration, expands entities from templates
- **Coordinator:** Pure orchestrator that runs flows in parallel
- **FlowFactory:** Creates flows from entities with proper wiring
- **Flow:** Orchestrates data movement with producer-consumer pattern
- **Store/Home:** Handle data I/O with Polars DataFrames

**Key Strengths:**
- Clear responsibilities make the codebase easy to understand
- Testable components enable confident refactoring
- Production-ready for midmarket scale data volumes
- Appropriate complexity - not over-engineered for the use case

### Areas for Improvement

**Code Quality:**
- All identified code quality improvements have been completed

**Test Coverage:**
- No visibility into coverage gaps or untested code paths
- Missing stress tests for midmarket scale scenarios
- Need baseline metrics to track coverage over time

**Remaining Complexity:**
- Watermark logic mixed with Flow execution ([extraction planned](watermark-tracker-extraction.md))
- Store interface uses `hasattr()` checks ([standardization planned](store-interface-standardization.md))

---

## Success Metrics

### Code Quality
- 610+ tests passing with good coverage of core functionality
- Clear architecture with well-defined separation of concerns
- Exception handling provides actionable error messages
- Test coverage baseline (in progress)

### Production Readiness
- Handles midmarket org data volumes reliably (millions to low billions of rows)
- Appropriate error handling and retry mechanisms for transient failures
- Data integrity preserved through failures with atomic operations
- Proper logging and observability for production troubleshooting

### Developer Experience
- Simple, intuitive APIs that feel natural to use
- Smart defaults enable minimal configuration
- Clear error messages guide users to solutions
- Type safety improvements (planned)

---

## Next Review

**Recommended Review Cycle:** Quarterly

**Next Review Focus:**
1. Test coverage baseline established and gaps identified
2. Store interface standardization progress
3. Watermark tracker extraction status
4. Any new issues or priorities that emerge

---

## Notes

- **Scale Context:** Midmarket regional org (millions to low billions of rows, not trillions)
- **Philosophy:** Comfort over complexity, reliability over speed, clarity over cleverness
- **Testing:** Test immediately after functionality, focus on behavior that matters
- **Breaking Changes:** Maintain backward compatibility unless explicitly discussed

---

**Last Updated:** Post-Entity Lifecycle Refactoring
**Status:** Production-ready, actively improving
