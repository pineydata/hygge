# hygge Roadmap: Current Status & Next Steps

**Last Updated:** Post-Store Interface Standardization
**Status:** Production-ready for midmarket scale, actively improving

---

## Overview

This roadmap tracks hygge's development progress, current priorities, and planned improvements. hygge is a cozy, comfortable data movement framework designed for solo developers and small teams working at midmarket regional org scale.

**Current State:**
- Core architecture is clean and production-ready
- 3,000+ new test lines added, comprehensive coverage reporting in place
- Clear separation of concerns across all components
- Exception handling provides clear, actionable error messages
- Polisher feature enables lightweight data finishing
- Test coverage visibility established with CI integration

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

6. **Test Coverage Infrastructure**
   - Comprehensive test coverage reporting with pytest-cov configuration
   - CI integration generates HTML, XML, and term-missing coverage reports
   - Added 3,000+ lines of new tests for Journal, MSSQL Home, ADLS Store, OpenMirroring Store, and Azure OneLake utilities
   - Coverage reporting focuses on source code while excluding test files and common patterns
   - Visibility into coverage gaps enables targeted test improvements
   - Coverage reports available as CI artifacts for ongoing review

7. **Watermark Filter Consistency Fix**
   - Fixed integer watermark filtering to always use `watermark_column` for WHERE clauses
   - Removed incorrect `primary_key` preference logic that caused issues with composite keys, non-integer primary keys, and non-sequential primary keys
   - Made behavior consistent across all watermark types (datetime, string, int)
   - Updated tests to validate correct filtering behavior
   - Discovered during test coverage improvements - validates the value of comprehensive testing

8. **Store Interface Standardization**
   - Made store interface explicit by adding default implementations for all optional methods (`configure_for_run`, `cleanup_staging`, `reset_retry_sensitive_state`, `set_pool`)
   - Removed fragile `hasattr()` checks in favor of direct method calls with safe defaults
   - Improved type safety and developer experience for store implementers with clear required vs optional method contracts
   - All existing stores remain fully compatible with no breaking changes
   - Added comprehensive tests for optional method default implementations

---

## Active Issues 🔄

### High Priority

1. **Large Data Volume & Stress Testing** (Remaining from test coverage work)
   - **Goal:** Add stress tests for midmarket scale scenarios
   - **Needs:**
     - Large data volume tests approaching midmarket limits (100M+ rows)
     - Concurrent flow stress tests (10+ flows running simultaneously)
     - Connection pool exhaustion scenarios
   - **Why:** Verify framework reliability at midmarket scale
   - **Estimated Effort:** 2-3 days for comprehensive stress test suite

### Medium Priority

1. **[Watermark Tracker Extraction](watermark-tracker-extraction.md)**
   - **Goal:** Separate watermark tracking logic from flow execution
   - **Current:** Watermark logic is mixed with Flow's batch processing
   - **Proposed:** Dedicated `Watermark` class with upfront schema validation
   - **Why:** Improves testability, maintainability, and error experience
   - **Estimated Effort:** 2-3 days
   - **Impact:** Better separation of concerns, easier to test edge cases

### Low Priority

2. **[Mirror Journal Batching](mirror-journal-batching.md)**
   - **Goal:** Batch journal mirror writes to reduce Fabric churn
   - **Current:** Mirrored journal reloads after every entity completion
   - **Proposed:** Accumulate entity run notifications, publish once per flow run
   - **Why:** Performance optimization for flows with multiple entities
   - **Estimated Effort:** 1 day

3. **[Schema Manifest Improvements](schema-manifest-improvements.md)**
   - **Goal:** Extract reusable schema generation logic from OpenMirroringStore
   - **Current:** Schema generation tightly coupled to Open Mirroring store
   - **Proposed:** Shared helper for Polars dtype → Fabric schema mapping
   - **Why:** Enables other Fabric destinations to reuse schema generation
   - **Estimated Effort:** 1 day

### Deferred/Evaluation

4. **[MSSQL Python Migration](mssql-python-migration.md)**
   - **Status:** Decision to stay with `pyodbc`
   - **Rationale:** `mssql-python` lacks bulk copy operations critical for Store writes, provides no async advantage
   - **Action:** Monitor `mssql-python` development for bulk copy support
   - **Re-evaluate:** 12-24 months if bulk operations are added

---

## Roadmap Priorities

### Immediate Next Steps (Next 1-2 Weeks)

1. **[Watermark Tracker Extraction](watermark-tracker-extraction.md)**
   - Extract watermark tracking logic from Flow into dedicated Watermark class
   - Add upfront schema validation for better error experience
   - Improve testability and maintainability of watermark logic
   - **Why:** Better separation of concerns, easier to test edge cases, fail fast with clear errors

### Short-Term (Next 1-2 Months)

1. **[Watermark Tracker Extraction](watermark-tracker-extraction.md)**
   - Improve testability and maintainability of watermark logic
   - Better error experience with upfront validation instead of reactive warnings
   - **Why:** Code quality improvement that makes the framework easier to maintain

2. **Large Data Volume & Stress Testing**
   - Add stress tests for midmarket scale scenarios (100M+ rows, concurrent flows, connection pool exhaustion)
   - **Why:** Verify framework reliability at production scale

### Long-Term (Next 3-6 Months)

1. **Small Improvements** (As Needed)
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
- ✅ Coverage reporting infrastructure in place with CI integration
- ✅ 3,000+ new test lines added for previously under-tested components
- ⚠️ Missing stress tests for midmarket scale scenarios (100M+ rows, concurrent flows, connection pool exhaustion)
- ✅ Coverage reports available as CI artifacts for ongoing review

**Remaining Complexity:**
- Watermark logic mixed with Flow execution ([extraction planned](watermark-tracker-extraction.md))

---

## Success Metrics

### Code Quality
- 3,000+ new test lines added, comprehensive coverage reporting in place
- Clear architecture with well-defined separation of concerns
- Exception handling provides actionable error messages
- ✅ Test coverage visibility established with CI integration
- Coverage reports identify gaps and guide targeted improvements

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
1. ✅ Test coverage baseline established and gaps identified
2. ✅ Watermark filter consistency fix completed
3. ✅ Store interface standardization completed
4. Watermark tracker extraction status
5. Large data volume stress testing progress
6. Any new issues or priorities that emerge

---

## Notes

- **Scale Context:** Midmarket regional org (millions to low billions of rows, not trillions)
- **Philosophy:** Comfort over complexity, reliability over speed, clarity over cleverness
- **Testing:** Test immediately after functionality, focus on behavior that matters
- **Breaking Changes:** Maintain backward compatibility unless explicitly discussed

---

**Last Updated:** Post-Store Interface Standardization
**Status:** Production-ready, actively improving
