# Does hygge *feel* hyggesque?

**Status:** Horizon 1 – First Priority
**Scope:** CLI experience, messaging, user interaction

## Context

We've been so focused on making hygge work that we haven't stopped to ask: **does it feel cozy to use?**

When you run `hygge go`, do you feel wrapped in comfort or left out in the cold? When something goes wrong, do you feel guided home or abandoned in the dark? When it finishes, does your data feel settled and at home?

**Our first priority is making every interaction feel warm.**

## The Problem

Today's hygge works reliably, but the experience can feel mechanical. Generic progress messages, technical error output, and minimal feedback leave users feeling disconnected from what's happening with their data.

### Current Experience Gaps

1. **Progress messages are generic** – "Processing 300,000 rows..." doesn't tell you what's actually happening
2. **Errors are technical** – Stack traces and technical jargon instead of friendly guidance
3. **First-time experience is cold** – No warm welcome, no guidance on getting started
4. **Completion feels abrupt** – Data finishes moving but you don't feel the satisfaction of it settling home

## The Vision

Every interaction should feel like a friend is keeping you company – not like you're struggling with a tool.

### Progress Messages That Tell a Story

Replace generic, mechanical feedback with concrete, narrative updates:

**Instead of:**
```
Processing 300,000 rows...
```

**Aim for:**
```
Settling 300,000 rows into their new home...
- Connecting to source
- Reading data batch 1/10 (30,000 rows)
- Writing batch to destination: data/users.parquet
- Schema mapped: 10 columns, 0 warnings
- Verifying integrity and finishing up
```

Progress messages should clearly tell the user what's happening—data counts, files, batches, finishing steps—in plain, approachable language.

### Errors That Guide You Home

Errors should feel like gentle guidance, not cold rejection. Instead of stack traces, provide:
- Clear explanation of what went wrong
- Why it happened
- What you can do to fix it
- Where to find more help

### A Warm First Experience

The first time you try hygge should feel like being invited in from the cold:
- Friendly welcome message
- Clear next steps
- Helpful examples
- Guidance on getting started

## Implementation Plan

### 1. Audit Every Moment

We'll review every interaction point:
- The messages you see while your data journeys home
- The errors you encounter when things go sideways
- The first time you welcome hygge into a new project
- The moment your data finally settles into its new home

### 2. Enhanced `hygge debug` – Your Trusted Companion

Today `hygge debug` shows discovered flows and validates config structure. We'll extend it to also:
- Test connections to databases and cloud storage
- Verify credentials and authentication are working
- Check that source paths exist and destinations are writable
- Surface any issues with clear guidance on how to fix them

One command, complete peace of mind before you run.

### 3. `--dry-run` Flag for `hygge go` – "Show Me What Would Happen First"

Preview mode that shows exactly what `hygge go` would do, without moving any data:
- Which flows and entities would run
- Source → destination mappings for each
- Row counts and schema information from sources
- What files/tables would be created or overwritten
- Any warnings or potential issues

Perfect for validating a new flow before committing, or checking what an incremental run would pick up.

### 4. Narrative Progress Messages

Transform progress output to tell a story:
- Use concrete details (file names, row counts, batch numbers)
- Show clear steps in the journey
- Use warm, approachable language
- Make it clear what's happening and what's next

### 5. Friendly Error Messages

Replace technical errors with helpful guidance:
- Explain what went wrong in plain language
- Suggest specific fixes
- Point to relevant documentation
- Make users feel supported, not abandoned

## Recent CLI Improvements

### Enhanced `hygge debug` Command ✅
Your trusted companion that makes sure everything feels right before you run.

**Before:** Basic configuration validation with technical output
**After:** Warm, helpful guidance with actionable insights

**What it does now:**
- 🏡 **Warm welcome** - Feels like a friend checking in
- ✓ **Configuration validation** - Clear, friendly status messages
- 📋 **Flow discovery** - Shows all your flows in a readable format
- 🔌 **Connection testing** - Tests database connections with helpful feedback
- 📁 **Path validation** - Checks that source/destination paths exist
- ✨ **Success summary** - Clear next steps when everything looks good
- ❌ **Helpful error messages** - Actionable guidance when things need attention

**Example output:**
```
🏡 hygge debug - Let's make sure everything feels right

✓ Project configuration is valid
  Project: my_project
  Flows directory: flows
  Total flows: 3

📋 Discovered Flows:
  • salesforce
    3 entities ready to move
      - Account
      - Contact
      - Opportunity

🔌 Testing 1 database connection(s)...
  my_database
    Type: mssql
    Server: localhost
    Database: prod
    ✓ Connection successful!

📁 Validating paths...
  ⚠️  Home path doesn't exist: data/source
     Flow: users_to_lake
     💡 Create it: mkdir -p data/source

✨ Everything looks good!
   Your hygge project is ready to go.

💡 Next steps:
   • Run: hygge go
   • Or run specific flows: hygge go --flow flow_name
```

### Comma-Separated Flow Arguments ✅
Clean, simple syntax for running multiple flows without quoting headaches.

**Single flow:**
```bash
hygge go --flow salesforce
```

**Multiple flows (comma-separated, no spaces needed):**
```bash
hygge go --flow flow1,flow2,flow3
```

**Multiple entities:**
```bash
hygge go --entity salesforce.Involvement,salesforce.Account,salesforce.Contact
```

**Real-world examples:**
```bash
# Run multiple flows at once
hygge go --flow salesforce,users_to_lake,orders_to_warehouse

# Run specific entities from a flow
hygge go --entity salesforce.Account,salesforce.Contact,salesforce.Opportunity

# Combine with other flags
hygge go --flow salesforce,users --incremental --concurrency 4
```

**Help text is now clearer:**
- Shows both single and multiple examples
- Explicitly mentions comma-separation for multiple values
- Format: `--flow flow1,flow2,flow3` (no spaces around commas needed, but tolerated)

## Success Criteria

After this work:
- [ ] The CLI feels warm and welcoming
- [ ] Errors feel like gentle guidance, not cold rejection
- [ ] New users feel invited in from the cold
- [ ] "Cozy" and "comfortable" are words people use to describe hygge
- [ ] Users reach for hygge because it feels comfortable, not just because it works

## Related Issues

- Part of [ROADMAP.md](ROADMAP.md) Horizon 1: Warmth
- Foundation for all future user-facing features
