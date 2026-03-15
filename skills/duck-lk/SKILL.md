---
name: duck-lk
description: DuckDB extension for querying LabKey Server tables with local Parquet caching. Use when the user wants to query, explore, or analyze data from a LabKey LIMS using DuckDB SQL. Also use when the user mentions LabKey tables, LIMS data, or wants to do exploratory data analysis on laboratory data that lives in LabKey.
license: MIT OR Apache-2.0
metadata:
  repository: https://github.com/nrminor/duck-lk
---

# duck-lk

DuckDB extension that synchronizes LabKey Server tables into local Parquet files for fast analytical access. LabKey's web UI is too slow for real data exploration on large tables; this extension gives you DuckDB's vectorized execution engine, columnar Parquet storage, and full SQL dialect (joins, window functions, CTEs, aggregations) on your LIMS data. The first query fetches and caches the table; subsequent queries run entirely locally with no network overhead.

## Loading the extension

For local builds (requires `duckdb -unsigned`):

```sql
LOAD 'build/debug/extension/duck_lk/duck_lk.duckdb_extension';
```

## Configuration

The extension needs a LabKey base URL, container path, and API key. These can come from environment variables (checked automatically) or named SQL parameters (per-query override).

Environment variables:

- `LABKEY_BASE_URL` — the server root, e.g. `https://labkey.example.com`
- `LABKEY_CONTAINER_PATH` — the project/folder path, e.g. `/MyProject`
- `LABKEY_API_KEY` — a LabKey API key for authentication

When environment variables are set, queries need only the schema and query name:

```sql
SELECT * FROM labkey_query('lists', 'Samples');
```

Named parameters override environment variables for a single query:

```sql
SELECT * FROM labkey_query('lists', 'Samples',
    base_url = 'https://labkey.example.com',
    container_path = '/MyProject',
    api_key = 'your-key'
);
```

The credential resolution order is: named SQL parameters → environment variables → `.netrc` file → guest access.

## Querying data

The primary function is `labkey_query(schema_name, query_name)`. The two positional arguments are the LabKey schema (e.g. `'lists'`, `'core'`, `'study'`, `'assay'`) and the table/query name within that schema.

```sql
-- Basic query
SELECT * FROM labkey_query('lists', 'Samples');

-- DuckDB SQL works normally on the results
SELECT sample_type, count(*) as n
FROM labkey_query('lists', 'Samples')
WHERE sample_date >= '2024-01-01'
GROUP BY sample_type
ORDER BY n DESC;

-- Join LabKey tables together
SELECT s.*, m.result
FROM labkey_query('lists', 'Samples') s
JOIN labkey_query('lists', 'Measurements') m
  ON s.sample_id = m.sample_id;

-- Create a persistent DuckDB table from LabKey data
CREATE TABLE samples AS
SELECT * FROM labkey_query('lists', 'Samples');
```

## How synchronization works

`labkey_query` operates in two phases:

1. **Fetch (first call only):** The entire table is fetched from LabKey via the REST API and written to a local Parquet file. This initial sync takes time proportional to the table size — there is no server-side filtering, since the goal is a complete local copy. Do not assume `LIMIT 10` makes the first call fast; the full table is always fetched.

2. **Query (all subsequent calls):** DuckDB reads the local Parquet file with its full query optimizer — columnar reads, row group skipping, predicate pushdown, and vectorized execution. This is where the performance payoff lives. Queries that are impossible in LabKey's web UI (cross-table joins, window functions, complex aggregations on millions of rows) run in seconds.

The extension checks staleness automatically by comparing `MAX(Modified)` timestamps with the server. If the data has changed, it re-fetches transparently.

## Disk footprint

Each cached table is stored as a Parquet file on disk. Large tables produce large cache files. Monitor and manage the cache with:

```sql
-- See what's cached, how big each table is, and when it was fetched
SELECT * FROM labkey_cache_info();

-- Clear a specific table's cache
CALL labkey_cache_clear(schema = 'lists', query = 'Samples');

-- Clear all cached data
CALL labkey_cache_clear();
```

Cache files are stored in the platform-specific cache directory:

- macOS: `~/Library/Caches/duck-lk/`
- Linux: `~/.cache/duck-lk/`
- Windows: `%LOCALAPPDATA%/duck-lk/`

When working with very large tables, check `labkey_cache_info()` periodically to understand the disk footprint. Clear tables you no longer need with `labkey_cache_clear()`.

## Offline mode

Tables without a `Modified` column skip staleness checks and serve from cache until manually cleared. To force a cache-only query (no network access, no staleness check):

```sql
SELECT * FROM labkey_query('lists', 'Samples', offline = true);
```

This fails if no cache exists for the requested table.

## This is DuckDB SQL, not LabKey SQL or PostgreSQL

Once data is cached locally, you query it with DuckDB's SQL dialect. This is **not** the same as:

- **LabKey SQL** (used in LabKey's web-based query editor) — LabKey SQL has its own syntax for lookups, schema-qualified names, and special columns.
- **PostgreSQL** (which LabKey uses internally) — while DuckDB supports much of PostgreSQL's syntax, there are differences in functions, type casting, and extensions.

DuckDB has its own strengths for analytical work: `COLUMNS(*)` expressions, `LIST`/`MAP` aggregates, `QUALIFY` clauses, `EXCLUDE`/`REPLACE` in `SELECT`, native Parquet/CSV/JSON reading, and more. Consult the [DuckDB SQL documentation](https://duckdb.org/docs/sql/introduction) for the full syntax reference.

## Finding schema and query names from LabKey URLs

If you can see a table in the LabKey web UI, the URL contains the schema and query name. For a URL like:

```
https://labkey.example.com/MyProject/list-grid.view?name=People
```

- **Base URL**: `https://labkey.example.com`
- **Container path**: `/MyProject`
- **Schema**: `lists` (derived from `list-grid.view`)
- **Query name**: `People` (the `name` parameter)

Common schema names: `lists`, `core`, `study`, `assay`.

## Common mistakes

**Assuming server-side filtering.** `WHERE` clauses do not reduce what is fetched from LabKey on the initial fetch. The full table is always materialized first. Once cached, DuckDB's optimizer handles filtering efficiently on the local Parquet file.

**Ignoring disk usage on large tables.** An 18-million-row table can produce a cache file of several hundred megabytes. Use `labkey_cache_info()` to monitor.

**Confusing SQL dialects.** LabKey SQL, PostgreSQL, and DuckDB SQL are all different. Once data is in DuckDB, use DuckDB syntax.

**Forgetting to clear stale caches.** If a table lacks a `Modified` column, the cache never auto-refreshes. Use `labkey_cache_clear()` to force a re-fetch.

**Not setting environment variables before launching DuckDB.** Environment variables are read at process start. Set `LABKEY_BASE_URL`, `LABKEY_CONTAINER_PATH`, and `LABKEY_API_KEY` before running `duckdb`.
