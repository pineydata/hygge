# Feature: Lightweight Polisher for last‑mile column and ID finishing

Status: Proposal
Owner: @louise
Labels: enhancement, ergonomics, core

## Problem
We need a lightweight, optional “last‑mile” step to make data feel at home before it reaches the Store. Today, Open Mirroring injects and enforces `__rowMarker__`, and the Journal generates hashed run identifiers. We lack a small, composable, flow‑scoped finishing step for:
- Column name normalization (remove special chars, PascalCase, optional space removal)
- Deterministic row‑level hash ID generation from one or more columns
- Optional `__rowMarker__` injection for non‑Open‑Mirroring flows

This should be opt‑in, simple, and aligned with hygge’s comfort-first ethos.

## Goals
- Provide a gentle “polish” step that can be applied per‑flow, per‑batch, before writing to the Store.
- Keep config minimal and conventional; avoid heavy transform engines.
- Centralize hashing approach so it aligns with `utility/run_id` semantics (stable, predictable, UTF‑8).
- Maintain Open Mirroring as the source of truth for `__rowMarker__` ordering and validation.

## Non‑Goals
- Full ETL/ELT framework
- Arbitrary expression engine or per‑column DSL
- Replacing Home/Store responsibilities

## Naming
- Class: `Polisher`
- Config block: `polish`
- File location: `src/hygge/core/polish.py` (or `core/transforms/polish.py` if we prefer a folder)

## Proposed Config (YAML)
```yaml
flows:
  users_to_lake:
    home:
      type: parquet
      path: data/source/users.parquet
    store:
      type: parquet
      path: data/lake/users
      polish:
        columns:
          remove_special: true     # remove punctuation, parentheses contents, etc.
          pascal_case: true        # “Employee Number” -> “EmployeeNumber”
          remove_spaces: false     # optional; if true: “Employee Number” -> “EmployeeNumber”
        hash_ids:
          - name: UserIdHash
            from_columns: ["Employee Number", "Effective Date"]
            algo: sha256            # default sha256
            hex: true               # default true (hex digest); false => binary (rarely used)
        # Generic constant columns (e.g., row markers)
        constants:
          - name: "__rowMarker__"
            value: 4                # 0=Insert, 1=Update, 2=Delete, 4=Upsert
        # Load timestamps (e.g., __LastLoadedAt__)
        timestamps:
          - name: "__LastLoadedAt__"
            source: now_utc         # now_utc | now_local (default: now_utc)
            type: datetime          # datetime | string (default: datetime)
            format: null            # optional strftime when type=string
        # Back-compat alias (optional): add_row_marker → maps internally to constants
        add_row_marker:
          enabled: false            # kept for compatibility; prefer constants
          value: 4
```

## Behavior
Order of operations for each batch (`pl.DataFrame`) when `polish` is configured:
1) Column normalization (if any flag in `columns` is true)
   - `remove_special`: remove parentheses and contents, and non‑alnum chars except spaces, `_`, `-`
   - `remove_spaces`: remove spaces
   - `pascal_case`: split on spaces/underscores/hyphens and camelCase boundaries; join as PascalCase
2) Hash IDs (for each rule in `hash_ids`)
   - Concatenate specified columns as strings with a safe separator (e.g., `|`), then hash
   - Use shared hashing semantics consistent with `utility/run_id` (see “Hashing Alignment”)
   - Insert new hash column (default: place first for visibility)
3) Constant columns
   - For each entry in `constants`, add the named column with the provided value if missing.
   - This generalizes row marker addition; `__rowMarker__` is just a specific constant.
4) Timestamps
   - For each entry in `timestamps`, add the named column with the current time.
   - `source`: `now_utc` (default) or `now_local`; `type`: `datetime` (default) or `string` with optional `format`.
5) Ordering and store-specific invariants
   - Stores may enforce final ordering; Open Mirroring always ensures `__LastLoadedAt__` second-to-last and `__rowMarker__` last.

All steps are intentionally minimal and deterministic. The feature is resilient: if a `hash_ids` rule references missing columns, the rule is skipped to avoid breaking the flow (log at debug/info).

## Integration Point
- Instantiate `Polisher` per Store if `store.polish` config is present.
- Base `Store` implements a `_pre_write(df)` hook that applies `Polisher` before buffering/writing.
- Specialized stores (e.g., Open Mirroring) may override/augment `_pre_write` but should still call `super()._pre_write(df)` to reuse common logic.

## Hashing Alignment
- Reuse/align with `hygge.utility.run_id` hashing behavior (UTF‑8, stable, SHA‑256 default).
- Implementation options:
  - Directly call a helper exported by `utility/run_id.py` for digest (preferred for single source of truth).
  - Or mirror its parameters (algo, encoding) to prevent drift.

Journal run IDs remain separate (metadata scope). `Polisher` creates row‑level IDs derived from data columns.

## Open Mirroring Compatibility
- Open Mirroring stores SHOULD still use `polish` for column normalization and hash IDs via the base `Store._pre_write` path.
- Row marker handling remains the OM store’s responsibility and authority:
  - The OM store will reuse shared helpers (extracted from Polisher) for both constant column addition (`__rowMarker__`) and timestamps (`__LastLoadedAt__`) to stay DRY.
  - The OM store continues to enforce/validate: `__rowMarker__` values, `__rowMarker__` last, `__LastLoadedAt__` second-to-last, and update-row requirements. It performs a final reorder/validation pass regardless of upstream configuration.
  - If `store.polish.add_row_marker` is set (back-compat), OM ignores the flag and relies on its own logic; both code paths use the same shared helpers for consistency.
  - Polisher MUST NOT reorder columns in a way that breaks OM’s ordering; OM’s final pass is authoritative.
- The Open Mirroring store continues to inject/validate/position `__rowMarker__` and `__LastLoadedAt__`.
- If a user enables row marker via `polish` in a non‑OM flow, `Polisher` adds it and keeps it last.

## API Surface
- `class Polisher:`
  - `__init__(config: Optional[PolishConfig])`
  - `apply(df: pl.DataFrame) -> pl.DataFrame`
- `class PolishConfig(pydantic.BaseModel)`
  - `columns: ColumnRules`
  - `hash_ids: List[HashIdRule]`
  - `constants: List[ConstantRule]`
  - `timestamps: List[TimestampRule]`

## Acceptance Criteria
- Configuration
  - Flow accepts a `polish` block; absence means no behavior change.
- Column normalization
  - Given columns with spaces/special chars/camelCase, when `remove_special` and `pascal_case` are true, names are normalized as expected.
  - When `remove_spaces` is true, spaces are removed.
- Hash IDs
  - Given two columns, `hash_ids` adds a deterministic SHA‑256 hex column with stable output across runs.
  - Missing input columns cause the rule to be skipped with a non‑fatal log.
- Constants
  - When configured and missing, the specified constant columns are added; `__rowMarker__` scenario covered by constants.
- Timestamps
  - When configured, `__LastLoadedAt__` (or any named column) is added using UTC by default; respects `type` and `format` settings.
- Open Mirroring
  - For OM flows, column order is still enforced by the store, and `polish.add_row_marker` defaults to disabled.
- Performance
  - Overhead is minimal (regex + hashing), appropriate for “lightweight” finishing.

## Tests
- Unit tests (core)
  - Column normalization: `remove_special`, `pascal_case`, `remove_spaces` combinations
  - Hash IDs: deterministic output, hex vs binary types, missing column tolerance
  - Constants: `__rowMarker__` added when missing (non‑OM)
  - Timestamps: `__LastLoadedAt__` added with UTC datetime; optional string formatting
- Integration
  - Flow with `polish` writes expected schema and values to a parquet store
  - Open Mirroring flow unaffected: store remains authority for `__rowMarker__`

## Risks / Considerations
- Naming collisions after renaming (two different cols normalizing to the same name). For now: last‑win rename; document this. Follow‑up could add collision disambiguation.
- Performance for extremely wide frames: mapping is O(cols); acceptable for a lightweight step.
- Separator for hashing: use a constant unlikely in data (e.g., `|`) and document; future option to configure.

## Migration / Docs
- No breaking changes.
- Add a short section in README and an example under `examples/` showing `polish` usage.

## Implementation Sketch
- Add `src/hygge/core/polish.py` with `Polisher`, `PolishConfig` and helpers.
- Update Flow to instantiate and call `Polisher` when configured.
- Reuse/align `utility/run_id` hashing semantics.

---

If the above looks good, I’ll proceed to:
1) Implement `Polisher` and config (core).
2) Wire optional `polish` into `Flow` batches.
3) Add unit + minimal integration tests.
4) Update README with a short example.
