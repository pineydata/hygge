# Google Drive Integration (Home & Store)

**Status:** Backlog
**Scope:** GDrive as ingestion source and landing destination; optional Google Sheets store. No transform layer (DuckDB/dbt) in scope.

---

## Context

Lightweight, cloud-light pipelines often use Google Drive as the persistent boundary: raw CRM exports land in Drive, and curated outputs are written back to Drive (or Sheets) for reporting—without a warehouse or persistent compute. hygge can meet data where it lives and land it where users already work.

**Use case:** Variable CRM sources → standardized ingestion → (external transform layer) → landing in Drive/Sheets. Small datasets (&lt;10MB), infrequent refresh (e.g. twice per month), consulting/small-team use. All variability is upstream of the in-memory DataFrame; once in Polars, processing is deterministic.

---

## Scope (In Scope)

- **GDrive Home** – Read raw files from Google Drive (e.g. CSV/JSON in a `/raw/`-style folder, optional file naming convention like `crm_export_YYYY_MM_DD.csv`).
- **GDrive Store** – Write flow outputs to Google Drive:
  - **Versioned** – timestamped files for auditability.
  - **Append** – e.g. KPI snapshots.
  - **Overwrite** – latest state only.
- **Google Sheets Store (optional)** – Land DataFrames to Sheets for live reporting/visibility.
- **Contract:** Standardized in-memory boundary (Polars DataFrame). UTF-8, consistent columns/IDs; file naming and folder layout are configuration.

**Out of scope for this issue:** Transformation layer (DuckDB, dbt, business logic). That lives outside hygge; hygge handles ingestion from Drive and landing to Drive/Sheets.

---

## Integration Concept

- **Ingestion:** Source data (e.g. CRM export) → lands in GDrive or is pulled via API → **GDrive Home** reads from Drive into a Polars DataFrame → `hygge go` runs the flow.
- **Landing:** Flow output (Polars DataFrame) → **GDrive Store** (or **Sheets Store**) writes versioned/append/overwrite to Drive or a Sheet.
- Historical accumulation is handled by versioned files and folder structure; no warehouse required.

All flows stay declarative and reproducible. Drive/Sheets are just another Home/Store abstraction.

---

## Design Principles (from pipeline design)

- **Decouple variability from stability** – CRM sources are variable; standardization happens at the Python/Polars boundary.
- **Traceable history** – Drive (and optionally Sheets) holds raw snapshots and curated outputs.
- **Lightweight & modular** – Fits small datasets, infrequent runs, minimal infra.
- **Extensible** – Same store model as other hygge destinations; other destinations can be added later.

---

## Implementation Directions

### GDrive Home

- Use Google Drive API (e.g. `google-api-python-client` / `google-auth`) or `gdrive`/PyDrive-style libraries per “Stand on Cozy Shoulders.”
- Support reading from a folder (e.g. by folder ID or path); optional file pattern or naming convention.
- Output: Polars DataFrame (CSV/JSON → `pl.read_csv` / `pl.read_ndjson` or equivalent).
- Config: credentials (OAuth2, service account), folder identifier, optional file filter/naming.

### GDrive Store

- Write flow output (Polars DataFrame) to a Drive folder.
- Modes: versioned (timestamped filenames), append, overwrite.
- File format: CSV (or Parquet if desired for larger outputs).
- Config: credentials, target folder, write mode, optional naming convention.

### Google Sheets Store (optional)

- Write DataFrame to a Sheet (existing or new).
- Consider sheet size limits; optional chunking or “latest only” overwrite.
- Config: credentials, spreadsheet ID, sheet name/range.

### Credentials & auth

- Support OAuth2 and service account (for automation, e.g. GitHub Actions).
- No secrets in YAML; use env vars or existing credential patterns (e.g. `GOOGLE_APPLICATION_CREDENTIALS`).

---

## Success Criteria

- [ ] GDrive Home can read CSV/JSON from a configured Drive folder into a Polars DataFrame.
- [ ] GDrive Store can write flow output to Drive (versioned, append, or overwrite).
- [ ] (Optional) Google Sheets Store can land a DataFrame to a Sheet.
- [ ] Auth uses standard Google mechanisms (service account / OAuth), no secrets in repo.
- [ ] Behavior and config align with existing hygge Home/Store patterns and CLAUDE.md principles.

---

## Related

- [welcome-data-from-everywhere.md](welcome-data-from-everywhere.md) – Multi-cloud and SaaS vision; GDrive fits as “data’s home.”
- [staged-writer-abstraction.md](staged-writer-abstraction.md) – If GDrive Store needs atomic replace semantics, consider staged write pattern.
- CLAUDE.md – “Stand on Cozy Shoulders”: use official or battle-tested Google libraries; keep integration in Home/Store; Polars at the boundary.
