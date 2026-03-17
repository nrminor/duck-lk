# Bulk Export Roadmap

## Where we are now

The `duck-lk` extension has a working spike that uses LabKey's `query-exportRowsTsv.view` endpoint for bulk table syncs. On a ~1.2M row table with 24 columns, this completes in about 22 minutes — compared to 3.5+ hours (and eventual failure) via the paginated JSON `selectRows` path.

The spike works by making a raw `reqwest` HTTP call directly from `duck-lk`, bypassing `labkey-rs` entirely for the bulk export. This is architecturally messy: it duplicates auth logic, introduces a direct HTTP dependency in the extension, and builds on an endpoint that isn't part of LabKey's official API surface. It's the right thing to ship now for usability, but it's not where we want to stay.

There are also two remaining performance issues on the extension side. First, the TSV response is currently buffered entirely in memory before parsing begins, which means a long silent download phase with no progress feedback. Second, cache-hit reads go through our VTab `func()` implementation rather than DuckDB's native Parquet reader, which means we lose DuckDB's parallelism, row group skipping, and predicate pushdown on cached data.

## Architectural principle

The LabKey interaction layer belongs in `labkey-rs` and in the official reference implementations it mirrors. The DuckDB extension should delegate all server communication to the client library and focus on caching, DuckDB integration, and user experience. Raw HTTP calls in `duck-lk` are a temporary measure, not a long-term design.

Similarly, Parquet reading on cache hits should be delegated to DuckDB's native reader rather than going through our VTab row-by-row. DuckDB is better at reading Parquet than we are.

## Forward trajectory

### Phase 1: Confirm endpoint stability with LabKey (current)

Reach out to LabKey support with the questions and measurements we've compiled. The key question is whether `query-exportRowsTsv.view` (or an equivalent bulk export endpoint) is considered stable enough for programmatic use.

**If LabKey confirms the TSV export endpoint is stable**, proceed to Phase 2.

**If LabKey recommends a different endpoint** for bulk export (e.g. a formal API endpoint we haven't discovered, a different export format, or a server-side materialization/job API), adjust the plan accordingly. The rest of the roadmap still applies — only the specific endpoint changes.

**If LabKey says no supported bulk path exists**, we may need to keep the TSV spike as a best-effort path with appropriate caveats in the documentation, or explore other approaches (direct database access, server-side export jobs, etc.).

### Phase 2: Contribute bulk export to the official client libraries

Before adding bulk export to `labkey-rs`, contribute the feature to the official reference implementations so it becomes part of the recognized LabKey client API surface.

The Java client (`labkey-api-java`) is probably the more authoritative of the two and should go first. The JavaScript client (`labkey-api-js`) should follow. Both PRs should:

- add a method for bulk TSV/CSV export
- use the endpoint LabKey confirmed as stable
- handle auth consistently with the existing client patterns
- support streamed responses where the language/framework allows
- include tests and documentation

Once these PRs are accepted (or at least reviewed and not rejected), the feature is de facto part of the LabKey client API.

### Phase 3: Add bulk export to `labkey-rs`

With the official clients as reference, add a bulk export method to `labkey-rs`. This should:

- follow the same API shape as the Java/JS implementations
- return a streaming byte reader rather than a fully buffered response
- handle auth through the existing `Credential` / `LabkeyClient` infrastructure
- be published as a new minor version of the crate

### Phase 4: Replace the `duck-lk` spike with the `labkey-rs` API

Remove the raw `reqwest` call from `duck-lk` and replace it with a call to the new `labkey-rs` bulk export method. This eliminates the duplicated auth logic and the direct HTTP dependency. The extension's `fetch_and_cache` path would then use `labkey-rs` for both the metadata/staleness API calls and the bulk data export.

At this point, the TSV response should also be streamed rather than buffered in memory, which would:

- allow progress reporting during the download phase
- reduce peak memory usage
- potentially allow overlapping download and Parquet conversion

### Phase 5: Delegate Parquet reading to DuckDB's native reader

The final architectural improvement: stop reading cached Parquet files through the VTab `func()` path and instead let DuckDB read them directly with its native Parquet reader.

The extension's role on cache hits would become:

1. resolve credentials and check staleness (existing logic)
2. determine the cached Parquet file path (existing logic)
3. hand the path to DuckDB's native Parquet reader

This would give cache-hit queries the full benefit of DuckDB's optimized Parquet engine: parallel reads, row group skipping, predicate pushdown, column pruning, and vectorized execution. Queries like `SELECT count(*)`, `LIMIT 5`, or filtered scans on large cached tables would go from painfully slow to near-instant.

The exact mechanism for this delegation needs investigation — it may involve DuckDB's `replacement_scan` API, or it may require restructuring how the VTab reports results. This is the least well-understood phase and may require experimentation.

## What stays true regardless

No matter what LabKey says about the specific endpoint:

- bulk LabKey communication belongs in `labkey-rs`, not in `duck-lk`
- `labkey-rs` should mirror the official client libraries
- `duck-lk` should focus on caching, DuckDB integration, and UX
- Parquet reading should be delegated to DuckDB's native reader
- the paginated JSON path should remain as a fallback for small tables and servers that don't support bulk export

## Timeline

No time estimates. Each phase depends on external responses (LabKey support, PR reviews) and on what we learn along the way. The phases are ordered by dependency, not by calendar.
