# Friendly Error Messages - Turn Frustration into Guidance

**Status:** Horizon 2 – Future Priority
**Scope:** Error handling, user experience, messaging
**Depends on:** Narrative progress messages (completed)

## The Problem

When things go wrong in hygge, users get technical stack traces and error messages that feel cold and unhelpful. Instead of feeling guided toward a solution, they feel abandoned with cryptic technical details.

**Current experience:**
```
Traceback (most recent call last):
  File "/hygge/stores/mssql/store.py", line 123, in write
    connection.execute(query)
  ...
pymssql.OperationalError: (20009, b'DB-Lib error message 20009, severity 9:\n
Unable to connect: Adaptive Server is unavailable or does not exist\n')
```

**Desired experience:**
```
❌ Can't connect to SQL Server

Your database isn't responding. This usually means:
• The server address is wrong (check "server" in hygge.yml)
• The database is offline or unreachable
• There's a network/firewall issue blocking the connection

💡 What to try:
1. Run: hygge debug
   (This will test your connection and show more details)
2. Verify your server address in hygge.yml
3. Check that the database is running and accessible

Need help? Check docs/connections.md
```

## The Vision

**Errors should feel like gentle guidance, not cold rejection.**

Every error message should:
1. **Explain what went wrong** - In plain language, not technical jargon
2. **Explain why it happened** - Help users understand the context
3. **Suggest specific fixes** - Actionable steps they can take right now
4. **Point to help** - Where to find more information

## Scope

### Three Categories of Errors

**1. Connection Errors** (Most common)
- Database connection failures
- Cloud storage authentication issues
- Network/firewall problems
- Credential problems

**2. Configuration Errors** (Most preventable)
- Invalid YAML syntax
- Missing required fields
- Type mismatches
- Path problems

**3. Runtime Errors** (Most complex)
- Schema mismatches during write
- Disk space issues
- Memory problems
- Timeout failures

## Implementation Approach

### Phase 1: Define Error Patterns (1 session)
- Audit existing error types across all components
- Group into categories (connection, config, runtime, etc.)
- Define standard message format
- Create examples for each category

### Phase 2: Connection Errors (1 session)
- Database connection failures (MSSQL, SQLite)
- Cloud storage auth failures (ADLS, OneLake)
- Add context-aware suggestions
- Test with common failure scenarios

### Phase 3: Configuration Errors (1 session)
- YAML validation errors
- Missing required fields
- Type mismatches
- Path validation failures

### Phase 4: Runtime Errors (1-2 sessions)
- Schema mismatches
- Write failures
- Resource exhaustion
- Timeout handling

## Design Principles

### 1. Context is King
Use information from the error context to provide specific guidance:
- Show the actual config values that are wrong
- Reference specific files and line numbers
- Suggest fixes based on the actual error, not generic advice

### 2. Progressive Disclosure
- **First line**: What went wrong (emoji + plain language)
- **Middle section**: Why it might have happened (bullet list)
- **Action items**: Specific numbered steps to try
- **Help pointer**: Where to learn more

### 3. No Stack Traces for Users
- Internal errors get full traces in logs (DEBUG level)
- Users see friendly messages by default
- `--verbose` can show technical details if needed

### 4. Fail Fast with Clarity
Following hygge's philosophy:
- No silent fallbacks
- No ambiguous "might work" states
- Clear, actionable messages that respect user's time

## Message Format Template

```
[emoji] [One-line summary of what went wrong]

[Plain language explanation]

[Context bullets]
• [Specific detail about this error]
• [Another relevant detail]
• [What this usually means]

💡 What to try:
1. [First specific action to take]
2. [Second specific action to take]
3. [Third action if needed]

[Optional: Link to docs or help]
```

## Example Transformations

### Before: Database Connection Error
```
pymssql.OperationalError: (20009, b'Unable to connect: Adaptive Server is unavailable')
```

### After: Database Connection Error
```
❌ Can't connect to SQL Server at 'prod-db.company.com'

The database server isn't responding. This usually means:
• The server address is incorrect
• The server is offline or unreachable
• A firewall is blocking port 1433

💡 What to try:
1. Run: hygge debug
2. Check server address in hygge.yml (currently: prod-db.company.com)
3. Verify the database is running and accessible from your network

Need help? Check docs/connections/mssql.md
```

---

### Before: Schema Mismatch
```
polars.exceptions.SchemaError: dtypes don't match
expected: Int64
got: Utf8
```

### After: Schema Mismatch
```
❌ Column type mismatch in 'user_id'

Your source data has a different type than expected:
• Source has: text (Utf8)
• Destination expects: number (Int64)

This usually happens when:
• The source data format changed
• You're writing to an existing table with a fixed schema
• There's a data quality issue in the source

💡 What to try:
1. Check the source data for the 'user_id' column
2. If the type changed, update your flow configuration
3. If writing to existing table, consider: --force-schema or recreate table

Related: docs/schema-handling.md
```

---

### Before: File Not Found
```
FileNotFoundError: [Errno 2] No such file or directory: 'data/users.parquet'
```

### After: File Not Found
```
❌ Can't find source file: data/users.parquet

hygge looked for this file but it doesn't exist.

Common reasons:
• Path is relative and you're running from wrong directory
• File was moved or renamed
• Path has a typo in flow configuration

💡 What to try:
1. Check the path in your flow YAML: flows/my_flow/flow.yml
2. Run from project root (where hygge.yml lives)
3. Verify the file exists: ls -la data/users.parquet

Current working directory: /Users/you/projects/my-hygge-project
```

## Success Criteria

**How do we know this is working?**

1. **Users can self-serve** - Common errors have clear solutions without needing to ask for help
2. **No more "what do I do now?"** - Every error message has actionable next steps
3. **Faster resolution** - Context-specific suggestions reduce debugging time
4. **Maintains hygge feel** - Errors feel like guidance from a friend, not technical rejection

## Why This is Deferred (Not Now)

1. **Less frequent** - Progress messages are seen every run; errors are exceptional
2. **Requires foundation** - Better to establish patterns with happy path first
3. **Larger scope** - Touches every component, needs careful design
4. **Current state works** - Errors are clear enough for initial users

## Why This Matters (When We Do It)

1. **Completes the comfort arc** - Before (debug/dry-run) + During (progress) + When things break (errors)
2. **Reduces support burden** - Self-service error resolution
3. **Builds confidence** - Users trust hygge to guide them through problems
4. **Differentiates hygge** - Few data tools treat error UX as a first-class concern

---

**Related Issues:**
- ✅ #hygge-feels-hyggesque (Narrative progress - completed)
- 🔗 This builds on established messaging patterns

**Next Steps:**
When we're ready to tackle this:
1. Run Phase 1 (audit and pattern definition)
2. Pick one error category to start with
3. Implement, test with real failure scenarios
4. Iterate based on feedback
5. Expand to other categories

**Note:** This is intentionally deferred to Horizon 2. Focus remains on core data movement reliability and performance. Error UX improvements will build on the narrative foundation we've established.
