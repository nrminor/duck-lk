# duck-lk

A DuckDB extension that brings the full power of a modern analytical SQL engine to your [LabKey Server](https://www.labkey.org/) data.

## Overview

LabKey's web interface does a lot of work under to the hood to give users a good experience when interacting with their tables. This work correctly optimizes for the vast majority of LabKey users, who don't work with particularly large tables. For the few who do, though, large tables can become very slow to work with, to the point where interacting with them on LabKey is no longer appealing.

`duck-lk` offers one possible solution for users comfortable with SQL and the command line: synchronize your big LabKey table into a local, analytics-optimized data representation ([Parquet](https://parquet.apache.org/)) and then explore and transform it with DuckDB, the leading in-process analytics query engine. With this setup, you get columnar compression, vectorized execution, window functions, CTEs, cross-table joins, and the DuckDB "Friendly SQL" dialect. A table that takes minutes to page through in the LabKey UI can usually be scanned, filtered, and aggregated in seconds.

The synchronization is automatic and transparent: the first query fetches the full table from LabKey and caches it as Parquet. Subsequent queries read directly from the local cache with no network overhead. Staleness detection ensures the cache stays fresh — if the server data has changed, the extension re-fetches automatically (though with escape hatches for the user).

> [!NOTE]
> The extension is strictly for read-only data exploration — it never writes back to LabKey.

## Installation

### Easy Mode: From the DuckDB Community Extensions (recommended)

```sql
INSTALL duck_lk FROM community;
LOAD duck_lk;
```

### Hard Mode: From a local build

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
export LABKEY_CONTAINER="/MyProject"
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

## `duck-lk` functions

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

Returns the table rows themselves. On a cache miss or stale cache, `labkey_query` fetches the full table from LabKey, stores it locally as Parquet, and then serves the query from that local copy.

> [!TIP]
> `labkey_query()` provides an all-in-one function to call to get your labkey data. But for the best data access and query performance, we recommend a two-step approach where users call `labkey_sync()` and then directly access the data as demonstrated below.

### `labkey_sync` — populate the local cache without returning all rows

```sql
-- Sync a table using connection details from the environment:
CALL labkey_sync('lists', 'People');

-- Sync with explicit connection parameters:
CALL labkey_sync('lists', 'People',
    base_url = 'https://labkey.example.com',
    container_path = '/MyProject',
    api_key = 'your-api-key'
);
```

Positional parameters:

| Parameter     | Type    | Description                                         |
| ------------- | ------- | --------------------------------------------------- |
| `schema_name` | VARCHAR | LabKey schema (e.g. `'lists'`, `'core'`, `'study'`) |
| `query_name`  | VARCHAR | Table or query name within the schema               |

Named parameters:

| Parameter        | Type    | Default | Description                |
| ---------------- | ------- | ------- | -------------------------- |
| `base_url`       | VARCHAR |         | LabKey server URL          |
| `container_path` | VARCHAR | `"/"`   | LabKey project/folder path |
| `api_key`        | VARCHAR |         | API key for authentication |

Recommended usage is via `CALL`, since `labkey_sync` performs a side effect and returns a single-row status message like `Synced lists.People (12345 rows) to /path/to/file.parquet`. This is useful when you want to warm the cache first and query the cached table separately afterward.

For example:

```sql
CALL labkey_sync('lists', 'People');

-- Once synced, the cached table is available directly in the console instance's namespace
SELECT * FROM People;

-- Normal DuckDB SQL works against the cached table:
SELECT count(*) FROM People;
SELECT Name, count(*) FROM People GROUP BY Name ORDER BY count(*) DESC;
```

Here, the sync step warms the local Parquet cache, and subsequent reads can go through DuckDB's native Parquet reader instead of routing back through the row-producing `labkey_query(...)` table function.

### `labkey_cache_info` — inspect the cache

```sql
SELECT * FROM labkey_cache_info();
```

Returns one row per cached table. This is useful for seeing what is cached, how large it is, when it was fetched, and what Parquet path backs it.

Columns returned:

| Column            | Type    | Description                                             |
| ----------------- | ------- | ------------------------------------------------------- |
| `base_url`        | VARCHAR | LabKey server URL used for the cache entry              |
| `container_path`  | VARCHAR | LabKey project/folder path                              |
| `schema_name`     | VARCHAR | LabKey schema name                                      |
| `query_name`      | VARCHAR | LabKey query/table name                                 |
| `row_count`       | BIGINT  | Cached row count                                        |
| `size_bytes`      | BIGINT  | Size of the cached Parquet file                         |
| `fetched_at`      | VARCHAR | Timestamp when the cache entry was written              |
| `server_modified` | VARCHAR | Last `MAX(Modified)` value seen on the server, if known |
| `staleness_check` | VARCHAR | Status of staleness tracking for the entry              |
| `parquet_path`    | VARCHAR | Absolute path to the cached Parquet file                |

### `labkey_cache_clear` — manage the cache

```sql
-- Clear a specific table's cache:
CALL labkey_cache_clear(schema = 'lists', query = 'People');

-- Clear all cached data:
CALL labkey_cache_clear();
```

Named parameters:

| Parameter | Type    | Default | Description                                             |
| --------- | ------- | ------- | ------------------------------------------------------- |
| `schema`  | VARCHAR |         | Schema name to clear; must be paired with `query`       |
| `query`   | VARCHAR |         | Query/table name to clear; must be paired with `schema` |

If both `schema` and `query` are provided, only that cached table is removed. If neither is provided, the entire local cache is cleared. Returns a single-row status message describing what was removed and how much disk space was reclaimed.

### `labkey_parquet_path` — get the local Parquet path for a cached table

```sql
SELECT labkey_parquet_path('lists', 'People');
```

Arguments:

| Parameter     | Type    | Description             |
| ------------- | ------- | ----------------------- |
| `schema_name` | VARCHAR | LabKey schema name      |
| `query_name`  | VARCHAR | LabKey query/table name |

Returns the absolute path to the cached Parquet file for that table, or `NULL` if the table is not currently cached.

### `labkey_is_stale` — check whether a cached table is stale

```sql
SELECT labkey_is_stale('lists', 'People');
```

Arguments:

| Parameter     | Type    | Description             |
| ------------- | ------- | ----------------------- |
| `schema_name` | VARCHAR | LabKey schema name      |
| `query_name`  | VARCHAR | LabKey query/table name |

Returns:

- `true` if the cached table appears stale relative to the server
- `false` if the cached table appears fresh
- `NULL` if staleness cannot be determined, for example because the table is not cached, the server has no `Modified` column to compare, or credentials are unavailable

## Credential resolution

Credentials are resolved using a precedence chain — the first source that provides a value wins:

1. **Named SQL parameters** (`api_key`, `base_url`, `container_path`)
2. **Environment variables** (`LABKEY_API_KEY`, `LABKEY_BASE_URL`, `LABKEY_CONTAINER`)
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

On the first call to `labkey_query` for a given table, the extension fetches the entire table from LabKey via the LabKey executeSQL API (docs for this from the unofficial LabKey client `duck-lk` users are [here](https://nrminor.github.io/labkey-rs/recipes/labkey-sql.html)) and writes it to a local Parquet file. This initial fetch takes time proportional to the table size — there is no server-side filtering, since the goal is to create a complete local copy for fast analytical access.

Once cached, all subsequent queries hit the local Parquet file with DuckDB's full query optimizer: columnar reads, row group skipping, predicate pushdown, and vectorized execution. This is where the performance payoff lives — queries that could become glacial in LabKey's web UI usually become delightfully quick.

**Disk usage.** Parquet is a compressed columnar format, so cache files are typically much smaller than the raw data. Still, large tables produce proportionally large files. Use `labkey_cache_info()` to monitor cache size and `labkey_cache_clear()` to reclaim space when you're done.

**SQL dialect.** Once data is in DuckDB, you query it with [DuckDB SQL](https://duckdb.org/docs/sql/introduction) — _not_ LabKey SQL (used in LabKey's web query editor) or PostgreSQL (which LabKey uses internally). DuckDB's dialect is rich: `COLUMNS(*)` expressions, `LIST` and `MAP` aggregates, `QUALIFY`, `EXCLUDE`/`REPLACE`, native Parquet/CSV/JSON I/O, and much more.

## Teach your agent to use `duck-lk`

First thing's first: have you agent load the [`duck-lk` skill](skills/duck-lk/SKILL.md)! It is intentionally opinionated about the best workflow: community install, user-managed environment variables for credentials, non-interactive DuckDB usage with markdown outputs, and sync-when-needed followed by direct reads from the cached table.

To recap from above, the components of that workflow are first to install and load the `duck-lk` extension in DuckDB SQL:

```sql
INSTALL duck_lk FROM community;
LOAD duck_lk;
```

And then to:

1. `CALL labkey_sync(schema, query)` to warm the local Parquet cache **when needed**
2. query the synced table directly by name so DuckDB can read the cached Parquet natively

To put this all together in DuckDB as an agent should run it:

```bash
duckdb -markdown <<'SQL'
INSTALL duck_lk FROM community;
LOAD duck_lk;

CALL labkey_sync('lists', 'People');

SELECT
    Name,
    count(*) AS n
FROM People
GROUP BY ALL
ORDER BY n DESC
LIMIT 20;
SQL
```

That two-step sync-then-query flow is preferable to making `labkey_query(...)` your default analysis path, especially for large tables. `labkey_query(...)` is still useful for one-off access and explicit `offline = true` reads, but it is a row-producing table function. Once a table is synced, direct reads like `FROM People` can go through DuckDB's native Parquet reader, which is the fastest path.

> [!IMPORTANT]
> The cache is stored on disk, so later `duckdb -c` invocations do not need to resync manually. A typical exploratory data analysis session with an agent may start with one `labkey_query()` or `labkey_sync()` call, but the remainder of the session should operate on the synchronized table directly. This will amortize the cost of big synchronizations.

A system that hasn't installed and run the extension will still need `INSTALL`/`LOAD`, but if the table is already cached on your system, you can usually just query it directly:

```bash
duckdb -markdown <<'SQL'
LOAD duck_lk;

FROM People LIMIT 10;
SQL
```

So the practical rule for agents (and people!) is:

> _sync as little as possible without letting the data go stale_

For multi-table analysis on a cold cache, sync each table first and then join the synced tables directly:

```bash
duckdb -markdown <<'SQL'
LOAD duck_lk;

CALL labkey_sync('lists', 'Samples');
CALL labkey_sync('lists', 'Measurements');

SELECT
    s.sample_id,
    s.sample_type,
    m.result
FROM Samples s
JOIN Measurements m USING (sample_id)
LIMIT 50;
SQL
```

If the agent only needs a cold-cache one-liner, this is still valid:

```bash
duckdb -markdown <<'SQL'
LOAD duck_lk;

FROM labkey_query('lists', 'People')
LIMIT 10;
SQL
```

But for repeated work, the shortest useful rule is:

```text
CALL labkey_sync(...), then query the synced table directly.
```

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
