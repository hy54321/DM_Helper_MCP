# DM Helper v1 — Final Implementation Plan

> Consolidated from two prior designs (Plan A: stdio/CLI-first, Plan B: FastAPI/React + HTTP MCP).
> Where the two differed, each decision is justified below.

---

## 1. Summary

Build a local single-user application for comparing source-system data
extracts against target-ERP extracts (CSV / Excel files) using DuckDB:

- **MCP server** (FastMCP, stdio transport) exposes all data-migration
  tools to any MCP-compatible client.
- **FastAPI + React admin UI** served from the same process for
  browser-based catalog management, profiling, job monitoring and
  report browsing.
- **DuckDB** for ephemeral, read-only query execution.
- **SQLite** for persisted metadata (catalog, pairs, key presets, jobs,
  report manifests).
- **Claude CLI client** (prompt-toolkit) as a secondary consumer of the
  same MCP tools (like the D365_Automation_mcp pattern we already use).

### Design decision: transport

| Option | Verdict |
|---|---|
| stdio only (Plan A) | Keep — cheapest client for quick ad-hoc use; already proven in our other MCP projects. |
| HTTP only (Plan B) | Keep — needed for the admin UI and for external MCP clients (e.g. Claude Desktop, Copilot Studio). |
| **Both** | **Selected** — `mcp_server.py` exposes both transports. CLI uses stdio; web UI and external clients use HTTP. Negligible extra cost. |

### Design decision: admin UI

Plan A had no UI; Plan B specified FastAPI + React.
**Verdict:** Include the admin UI. The MCP tools are the source of truth;
the UI is a thin consumer.  Implementation is Phase 2 (after the core
MCP server is solid).

---

## 2. Architecture

```
DM_Helper_MCP/
├── main.py                    # CLI entry (stdio MCP client → Claude loop)
├── mcp_server.py              # FastMCP server (stdio + HTTP transport)
├── mcp_client.py              # MCP stdio client wrapper
├── pyproject.toml
├── .env
├── core/
│   ├── chat.py                # Agentic tool-use loop
│   ├── claude.py              # Anthropic API wrapper
│   ├── cli_chat.py            # CLI-specific chat + system prompt
│   ├── cli.py                 # prompt-toolkit terminal UI
│   └── tools.py               # Tool dispatch across MCP clients
├── server/
│   ├── catalog.py             # CatalogService
│   ├── query_engine.py        # DuckDB connection factory + view registration
│   ├── sql_guard.py           # Read-only SQL validator
│   ├── profile.py             # ProfileService (top-N, blanks, combos)
│   ├── comparison.py          # ComparisonService (diff engine)
│   ├── jobs.py                # JobService (async job lifecycle)
│   ├── reports.py             # ReportService (XLSX writer + metadata)
│   └── db.py                  # SQLite schema + access helpers
├── ui/                        # (Phase 2) FastAPI + React admin
│   ├── api.py                 # REST routes
│   └── static/                # Built React bundle
└── tests/
    ├── test_sql_guard.py
    ├── test_catalog.py
    ├── test_comparison.py
    └── ...
```

### Core services

| Service | Source | Responsibility |
|---|---|---|
| `CatalogService` | Plan B | Scans source + target folders, registers one logical dataset per CSV / per Excel sheet, stores schema snapshots in SQLite. |
| `QueryEngine` | Both | Opens fresh in-memory DuckDB per operation, registers views, returns results. Same patterns as `workspace/ui/sql_query.py`. |
| `SqlGuard` | Both (Plan A had inline regex, Plan B proposed a service) | Dedicated module. Validates SQL is read-only; blocks dangerous keywords, multi-statement, DuckDB engine commands. |
| `ProfileService` | Plan B (enriched from `excel_summary.py` patterns) | Per-column top-N values, blank counts, multi-column combo summaries, filtered record previews. |
| `ComparisonService` | Both | Source-vs-target diffs by key columns. Emits ADDED / REMOVED / CHANGED with field-level before/after. Detects schema drift and duplicate keys. |
| `JobService` | Plan B | Tracks async comparison jobs (queued → running → succeeded / failed / canceled). Stores progress counters. |
| `ReportService` | Both | Writes XLSX reports to disk, tracks report metadata in SQLite, links reports to jobs. |

### Data stores

| Store | Usage |
|---|---|
| DuckDB (in-memory) | Ephemeral query execution. No persisted business state. |
| SQLite | Catalog snapshots, pair overrides, key presets, job history, report manifests. |
| File system | Source/target data files, generated XLSX reports. |

---

## 3. Security and Safety

### SQL policy (agreed in both plans)

- **Allow:** `SELECT`, `WITH`, `FROM`, `SUMMARIZE`.
- **Block:** `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CREATE`, `ALTER`,
  `DROP`, `TRUNCATE`, `REPLACE`, `EXEC/EXECUTE`, `GRANT`, `REVOKE`.
- **Also block DuckDB-specific escapes:** `PRAGMA`, `INSTALL`, `LOAD`,
  `ATTACH`, `DETACH`, `COPY`, `EXPORT`.
- **Multi-statement:** reject any unquoted semicolons.

### Data exposure policy

| Aspect | Rule |
|---|---|
| Default preview row cap | **10 rows** (Plan B's tighter default — safer for large migration files). Plan A had 20; 10 is fine since the LLM can always request more via `limit` param up to the hard cap. |
| Hard cap for MCP tool responses | **100 rows** — never return more in a single tool call. |
| Full data | Written to disk reports only; client receives summary + file path. |

### Scope policy

- Single local user in v1.
- No multi-tenant auth / RBAC.

---

## 4. Catalog, Pairing, and Keys

### Catalog intake (from Plan B — superior to Plan A's flat single-folder approach)

- **Separate source and target directories** configured at startup or via
  tool call.  This is better than Plan A's single `set_data_folder`
  because:
  - It makes source/target pairing deterministic.
  - It prevents users from accidentally comparing two source files.
  - It mirrors real migration workflows where extracts come from different systems.
- **Refresh command** rescans both directories.
- **Excel handling:** each sheet → separate dataset.
- **CSV:** delimiter sniffing (same as `sql_query.py`), `utf-8-sig` header fallback.
- **Schema snapshots** stored in SQLite for change detection across refreshes.

### Pairing strategy (Plan B — new, important addition)

- **Auto-match** source ↔ target by normalised file + sheet name
  (strip prefixes like `SRC_`, `TGT_`, case-fold, strip underscores).
- **Manual overrides** persisted in SQLite (create, update, disable).
- **Expose** pair inventory in MCP tools and admin UI.

### Key strategy (Plan B — new, important addition)

- **Suggest candidate keys** from profiling heuristics:
  - High uniqueness ratio.
  - Low null ratio.
  - High overlap between source and target.
- **User confirmation** required before comparison runs.
- **Persist confirmed presets** per pair in SQLite for reuse.

---

## 5. MCP Tools

### Design decision: tool naming

Plan A used short action verbs (`preview_table`, `run_query`).
Plan B used noun-centric identifiers (`preview_dataset`, `run_sql_preview`).
**Verdict:** Use verb-first naming for clarity in LLM tool selection,
with `dataset` replacing `table` as the domain term (since these are
data files, not database tables).

### 5.1 Catalog tools

| Tool | Args | Returns |
|---|---|---|
| `refresh_catalog` | `source_folder?, target_folder?` | Discovery summary (counts, new/removed datasets). |
| `list_datasets` | `filter?, side?` ("source"/"target"/"all") | Dataset list with file, sheet, column count, row count. |
| `list_fields` | `dataset_id` | Column names and DuckDB-inferred types. |
| `preview_dataset` | `dataset_id, limit=10, offset=0, fields?` | Formatted top-N rows. |
| `run_sql_preview` | `sql, limit=10` | Execute read-only SQL, return capped result. |
| `export_query` | `sql, filename?, format?` | Run SQL, save ALL rows to `reports/`, return file path. |
| `row_count_summary` | — | Row counts for all loaded datasets. |

### 5.2 Profiling tools

| Tool | Args | Returns |
|---|---|---|
| `data_profile` | `dataset_id` | Per-column: non-null%, distinct, min, max, samples. |
| `column_value_summary` | `dataset_id, column?, top_n=10` | Per-column top-N value frequencies + blank counts. |
| `combo_value_summary` | `dataset_id, columns, top_n=10` | Frequency of combined-field value tuples. |
| `preview_filtered_records` | `dataset_id, filter_spec, limit=10` | Capped records matching value/blanks filters. |
| `find_duplicates` | `dataset_id, key_columns, limit=10` | Duplicate groups. |
| `value_distribution` | `dataset_id, column, limit=20` | Frequency counts sorted by count desc. |

### 5.3 Pairing and key tools

| Tool | Args | Returns |
|---|---|---|
| `list_pairs` | — | All auto-matched + override pairs. |
| `upsert_pair_override` | `source_dataset_id, target_dataset_id, enabled` | Confirm or override a pairing. |
| `suggest_keys` | `pair_id` | Candidate key columns with rationale. |
| `save_key_preset` | `pair_id, key_fields, name` | Persist reusable key config. |
| `list_key_presets` | `pair_id` | Saved key presets for a pair. |
| `schema_diff` | `source_dataset_id, target_dataset_id` | Missing/extra columns between two datasets. |

### 5.4 Comparison and report tools

| Tool | Args | Returns |
|---|---|---|
| `start_comparison_job` | `pair_id \| (source, target), key_fields, options?` | Job ID + initial status. |
| `get_job_status` | `job_id` | State, progress counters, elapsed time. |
| `get_job_summary` | `job_id` | Counts (added/removed/changed), sample diffs. |
| `cancel_job` | `job_id` | Confirmation. |
| `compare_field` | `source, target, key_columns, field` | Per-row diffs for a single field (drill-down). |
| `list_reports` | — | All generated reports with metadata. |
| `get_report_metadata` | `report_id` | Path, timestamp, pair, summary counts. |
| `delete_report` | `report_id` | Confirmation. |

### 5.5 Quick comparison (Plan A pattern, kept for convenience)

For simple ad-hoc use (e.g. via CLI), the server also exposes a
synchronous `compare_tables` tool that runs the comparison inline
(no job) and returns a summary.  This calls the same `ComparisonService`
under the hood but skips the job/async layer.

### 5.6 MCP resources

| URI | Returns |
|---|---|
| `data://datasets` | JSON list of dataset IDs + side (source/target). |
| `data://datasets/{id}/schema` | JSON column list for a dataset. |
| `jobs://{id}/summary` | JSON comparison summary. |
| `reports://{id}/manifest` | JSON report metadata. |

### 5.7 MCP prompts

| Prompt | Purpose |
|---|---|
| `compare_data` | Guided end-to-end comparison workflow. |
| `profile_data` | Guided data-quality profiling. |
| `reconcile_data` | Full multi-table reconciliation. |

---

## 6. Report Format (XLSX)

### Workbook tabs

| Tab | Content |
|---|---|
| `Summary` | Source/target metadata, key config, execution timestamp, row counts, ADDED/REMOVED/CHANGED totals. |
| `Schema_Drift` | Missing/extra column details, type mismatches. |
| `Added` | Target-only records (full rows). |
| `Removed` | Source-only records (full rows). |
| `Changed` | Key columns, field name, source value, target value. |

### Writer requirements

- Safe worksheet naming (31-char Excel limit, invalid char replacement).
- Unique sheet name generation on collision.
- Machine-readable summary metadata also persisted in SQLite.

---

## 7. Admin UI (Phase 2: FastAPI + React)

### Pages

| Page | Features |
|---|---|
| **Catalog** | Scan folders, inspect datasets/sheets, review schema + field list. |
| **Profiling** | Top-N per column, blank counts, combo summaries, drilldowns. |
| **Pairs & Keys** | Auto-pair review, manual overrides, key suggestions, preset management. |
| **Jobs** | Start comparisons, monitor progress, inspect errors, cancel. |
| **Reports** | List XLSX reports, inspect metadata, open file location, delete. |

### UX patterns (from `workspace/ui/excel_summary.py`)

- Configurable top-N ranking for distributions.
- Explicit blank/empty tracking per field.
- Combined-field summary rows for correlation checks.
- Drilldown from summary cells → filtered record preview.
- Progress indicators for long-running operations.

---

## 8. Implementation Phases

### Phase 1 — Core MCP server + CLI client

1. `server/db.py` — SQLite schema and access helpers.
2. `server/sql_guard.py` — Read-only validator with tests.
3. `server/query_engine.py` — DuckDB connection factory, view registration (ported from `sql_query.py`).
4. `server/catalog.py` — File scanner, dataset registration, SQLite persistence.
5. `server/profile.py` — Column profiling, top-N, blanks, combos.
6. `server/comparison.py` — Diff engine (ADDED/REMOVED/CHANGED).
7. `server/reports.py` — XLSX writer.
8. `server/jobs.py` — Async job runner (can be synchronous-first, async later).
9. `mcp_server.py` — Wire all services into FastMCP tools/resources/prompts.
10. `mcp_client.py` — Complete client methods.
11. `core/cli_chat.py` — System prompt, table-aware query processing.
12. `core/cli.py` — Auto-complete for datasets, commands.
13. `main.py` — Bootstrap.

### Phase 2 — Admin UI

14. `ui/api.py` — FastAPI routes wrapping the same services.
15. `ui/static/` — React app (catalog, profiling, pairs, jobs, reports).
16. Dual-transport support in `mcp_server.py` (stdio + HTTP).

### Phase 3 — Polish

17. Key suggestion heuristics.
18. Progress streaming for long comparisons.
19. Report retention/cleanup policy.

---

## 9. Testing and Validation

### Unit tests

- SQL guard allow/block matrix.
- Catalog parsing: CSV (various delimiters), multi-sheet Excel, edge cases.
- Identifier sanitization and path escaping.
- Top-N summary and blank counting logic.
- Combo summary logic.
- Filtered preview logic.
- Duplicate detection.
- Schema diff.
- Comparison engine: ADDED/REMOVED/CHANGED categorisation.
- Report writer: sheet naming, data integrity.

### Integration tests

- Catalog refresh → query execution end-to-end.
- Suggest keys → save preset → run comparison.
- Async job lifecycle: success, failure, cancellation.
- Report generation + metadata registration.
- Enforced row cap on all preview outputs.
- SQL guard rejects destructive queries in tool context.

### UI tests (Phase 2)

- End-to-end: scan → inspect → profile → pair → key preset → compare → monitor → view report.

---

## 10. Operational Defaults

| Setting | Value |
|---|---|
| MCP transport | stdio (CLI) + HTTP (UI, external clients) |
| Deployment | Single local process |
| User model | Single local trusted user |
| Metadata DB | SQLite |
| Data source model | Separate source and target folders |
| Excel handling | Each sheet → separate dataset |
| Default preview row cap | 10 |
| Hard row cap (MCP responses) | 100 |
| Report format | XLSX |
| Retention | Manual cleanup (no auto-delete in v1) |

---

## 11. Open Questions / Points of Disagreement

### Resolved in this plan

| Topic | Plan A | Plan B | Resolution |
|---|---|---|---|
| Transport | stdio only | HTTP only | Both — stdio for CLI, HTTP for UI/external |
| Folder model | Single folder (`set_data_folder`) | Separate source + target | Separate — mirrors real migration workflow |
| Admin UI | None | FastAPI + React | Include, but Phase 2 |
| Pairing | None (manual via tool args) | Auto-pair + overrides | Include auto-pairing — saves significant user effort |
| Key presets | None | Persisted in SQLite | Include — users run comparisons repeatedly |
| Job system | Synchronous only | Full async (queued/running/done/failed/canceled) | Include — large files can take minutes |
| Default row cap | 20 | 10 | 10 (safer default; users can request up to 100) |

### To revisit during implementation

- **PRAGMA block:** Plan B blocks `PRAGMA`. We use `SET default_collation='nocase'` in QueryEngine
  internally. The block only applies to user-submitted SQL — internal engine queries bypass SqlGuard.
  This is correct and intentional.
- **Combo summaries:** Plan B includes `combo_value_summary`. Useful but complex. Implement after
  single-column profiling is solid. Mark as Phase 1 stretch goal.
- **Key suggestion heuristics:** The algorithm for scoring candidate keys needs tuning with
  real migration data. Start simple (uniqueness % + null %) and iterate.
- **Delete report:** Plan B includes `delete_report`. Fine to add, but ensure it only deletes
  the generated file and metadata — never source/target data.
