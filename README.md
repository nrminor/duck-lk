# duck-lk

A DuckDB extension that brings the full power of a modern analytical SQL engine to your [LabKey Server](https://www.labkey.org/) data.

LabKey's web interface struggles with large tables — slow page loads, limited filtering, no joins across tables, no aggregations beyond what the UI offers. duck-lk solves this by synchronizing LabKey tables into local [Parquet](https://parquet.apache.org/) files and exposing them through DuckDB, where you get columnar compression, vectorized execution, window functions, CTEs, cross-table joins, and the full DuckDB SQL dialect. A table that takes minutes to page through in the LabKey UI can be scanned, filtered, and aggregated in seconds.

The synchronization is automatic and transparent: the first query fetches the full table from LabKey and caches it as Parquet. Subsequent queries read directly from the local cache with no network overhead. Staleness detection ensures the cache stays fresh — if the server data has changed, the extension re-fetches automatically (though with escape hatches for the user).

The extension is strictly read-only — it never writes back to LabKey.

## Installation

### From the DuckDB Community Extensions (recommended)

```sql
INSTALL duck_lk FROM community;
LOAD duck_lk;
```

### From a local build

Building from source requires [Nix](https://nixos.org/) with flakes enabled (recommended) or a manual toolchain with Rust, Python 3, Make, and Git.

```bash
git clone --recurse-submodules https://github.com/nrminor/duck-lk.git
cd duck-lk

# With Nix (sets up the full toolchain automatically):
nix develop

# Then:
make configure
make debug    # or: make release
```

Load the extension in DuckDB with the `-unsigned` flag (required for local builds):

```bash
duckdb -unsigned
```

```sql
LOAD 'build/debug/extension/duck_lk/duck_lk.duckdb_extension';
```

## Quick start

Set your LabKey connection details as environment variables:

```bash
export LABKEY_BASE_URL="https://labkey.example.com"
export LABKEY_CONTAINER_PATH="/MyProject"
export LABKEY_API_KEY="your-api-key"
```

Then query any LabKey table by schema and query name:

```sql
SELECT * FROM labkey_query('lists', 'People');
```

The first call fetches data from the server and caches it locally as Parquet. Subsequent calls serve from cache unless the server data has changed.

### Finding your schema and query names

If you can see a table in the LabKey web UI, the URL contains the information you need. For a URL like:

```text
https://labkey.example.com/MyProject/list-grid.view?name=People
```

- **Base URL**: `https://labkey.example.com` (everything before the container path)
- **Container path**: `/MyProject` (the project/folder path)
- **Schema**: `lists` (derived from `list-grid.view`)
- **Query name**: `People` (the `name` parameter)

Common schema names include `lists`, `core`, `study`, and `assay`.

## Table functions

### `labkey_query` — query LabKey data

```sql
-- Minimal (connection details from environment variables):
SELECT * FROM labkey_query('lists', 'People');

-- With explicit connection parameters:
SELECT * FROM labkey_query('lists', 'People',
    base_url = 'https://labkey.example.com',
    container_path = '/MyProject',
    api_key = 'your-api-key'
);

-- Offline mode (cache only, no network):
SELECT * FROM labkey_query('lists', 'People', offline = true);
```

Positional parameters:

| Parameter     | Type    | Description                                         |
| ------------- | ------- | --------------------------------------------------- |
| `schema_name` | VARCHAR | LabKey schema (e.g. `'lists'`, `'core'`, `'study'`) |
| `query_name`  | VARCHAR | Table or query name within the schema               |

Named parameters:

| Parameter        | Type    | Default | Description                                 |
| ---------------- | ------- | ------- | ------------------------------------------- |
| `base_url`       | VARCHAR |         | LabKey server URL                           |
| `container_path` | VARCHAR | `"/"`   | LabKey project/folder path                  |
| `api_key`        | VARCHAR |         | API key for authentication                  |
| `offline`        | BOOLEAN | `false` | Skip staleness check, serve only from cache |

### `labkey_cache_info` — inspect the cache

```sql
SELECT * FROM labkey_cache_info();
```

Returns one row per cached table with columns: `base_url`, `container_path`, `schema_name`, `query_name`, `row_count`, `size_bytes`, `fetched_at`, `server_modified`, `staleness_check`, and `parquet_path`.

### `labkey_cache_clear` — manage the cache

```sql
-- Clear a specific table's cache:
CALL labkey_cache_clear(schema = 'lists', query = 'People');

-- Clear all cached data:
CALL labkey_cache_clear();
```

Returns a status message with details about what was cleared.

## Credential resolution

Credentials are resolved using a precedence chain — the first source that provides a value wins:

1. **Named SQL parameters** (`api_key`, `base_url`, `container_path`)
2. **Environment variables** (`LABKEY_API_KEY`, `LABKEY_BASE_URL`, `LABKEY_CONTAINER_PATH`)
3. **`.netrc` file** (matched by hostname from the resolved base URL)
4. **Guest access** (no credentials — only works if the server permits anonymous reads)

The base URL is required. If it isn't provided by any source, the query returns an error explaining the available configuration methods.

## Cache behavior

Fetched data is cached as Parquet files in the platform-specific cache directory:

- **macOS**: `~/Library/Caches/duck-lk/`
- **Linux**: `~/.cache/duck-lk/`
- **Windows**: `%LOCALAPPDATA%/duck-lk/`

The directory structure mirrors the LabKey hierarchy (`{hostname}/{container}/{schema}/{query}.parquet`), making it easy to inspect cached files directly.

On each query, the extension checks whether the LabKey table has been modified since the last fetch by comparing `MAX(Modified)` timestamps. If the data is stale, it transparently re-fetches. Tables without a `Modified` column skip this check and serve from cache until manually cleared.

## How it works

The extension operates in two phases: **fetch** and **query**.

On the first call to `labkey_query` for a given table, the extension fetches the entire table from LabKey via the REST API and writes it to a local Parquet file. This initial fetch takes time proportional to the table size — there is no server-side filtering, since the goal is to create a complete local copy for fast analytical access.

Once cached, all subsequent queries hit the local Parquet file with DuckDB's full query optimizer: columnar reads, row group skipping, predicate pushdown, and vectorized execution. This is where the performance payoff lives — queries that would be impossible in LabKey's web UI (joins across tables, window functions, complex aggregations) run in seconds against the local cache.

**Disk usage.** Parquet is a compressed columnar format, so cache files are typically much smaller than the raw data. Still, large tables produce proportionally large files. Use `labkey_cache_info()` to monitor cache size and `labkey_cache_clear()` to reclaim space when you're done.

**SQL dialect.** Once data is in DuckDB, you query it with [DuckDB SQL](https://duckdb.org/docs/sql/introduction) — not LabKey SQL (used in LabKey's web query editor) or PostgreSQL (which LabKey uses internally). DuckDB's dialect is rich: `COLUMNS(*)` expressions, `LIST` and `MAP` aggregates, `QUALIFY`, `EXCLUDE`/`REPLACE`, native Parquet/CSV/JSON I/O, and much more.

## Building from source

The development environment is managed with a Nix flake. Running `nix develop` (or allowing direnv to activate the `.envrc`) provides Rust 1.90.0, DuckDB, Python 3, Make, and all build dependencies.

Available make targets:

| Target         | Description                        |
| -------------- | ---------------------------------- |
| `make debug`   | Build debug extension binary       |
| `make release` | Build optimized release binary     |
| `make check`   | Run `cargo fmt` and `cargo clippy` |
| `make test`    | Run SQL logic tests (debug build)  |
| `make clean`   | Remove build artifacts             |

The extension uses pedantic Clippy lints with `unwrap_used = "deny"`.
