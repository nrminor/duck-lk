---
name: duck-lk
description: DuckDB extension for fast local-first analysis of LabKey Server data. Use when the user wants to analyze LabKey tables with DuckDB, especially non-interactively from an agent or script. Prefer the two-step workflow: sync with labkey_sync(), then query the synced table directly through DuckDB's native Parquet reader.
license: MIT OR Apache-2.0
metadata:
  repository: https://github.com/nrminor/duck-lk
---

# duck-lk

`duck-lk` is a DuckDB community extension for synchronizing LabKey Server tables into local Parquet and then querying them with DuckDB. The key idea is local-first analysis: fetch once, cache as Parquet, then let DuckDB do what it is good at.

The extension exposes two different styles of access:

1. `labkey_query(schema, query)` — convenient all-in-one access that can fetch and return rows directly.
2. `labkey_sync(schema, query)` followed by direct table reads like `FROM Samples` — the preferred high-performance path once a table is cached.

For agents and scripts, the second style is usually the right one once a table has been cached.

## Use DuckDB non-interactively

Prefer the DuckDB CLI from bash over ad hoc Python wrappers. Use one-shot commands with `-c` for simple cases and heredocs for anything multi-step.

```bash
duckdb -c "SELECT 42;"
```

```bash
duckdb <<'SQL'
SELECT 42;
SQL
```

This works especially well with `duck-lk`, because the natural flow is a small sequence of SQL statements rather than interactive clicking around.

## Load the extension

Prefer the community extension once available in the target environment:

```sql
INSTALL duck_lk FROM community;
LOAD duck_lk;
```

For local development builds only:

```sql
LOAD 'build/debug/extension/duck_lk/duck_lk.duckdb_extension';
```

## Configuration

The extension needs three LabKey connection values:

- `LABKEY_BASE_URL`
- `LABKEY_CONTAINER`
- `LABKEY_API_KEY`

Example:

```bash
export LABKEY_BASE_URL='https://labkey.example.com'
export LABKEY_CONTAINER='/MyProject'
export LABKEY_API_KEY='your-api-key'
```

These can also be supplied as named SQL parameters (`base_url`, `container_path`, `api_key`), but for agents the environment-variable route is usually cleaner.

For agents, this should be treated as the **default and strongly preferred** approach. Ask the user to set:

- `LABKEY_BASE_URL`
- `LABKEY_CONTAINER`
- `LABKEY_API_KEY`

in their own shell environment before running DuckDB.

Do **not** casually encourage users to paste API keys into chat messages, SQL text, notebooks, screenshots, or shell history if it can be avoided. In particular, agents should be vigilant when a user is being careless with tokens. Prefer guidance like:

```bash
export LABKEY_BASE_URL='https://labkey.example.com'
export LABKEY_CONTAINER='/MyProject'
export LABKEY_API_KEY='...'
```

and then write SQL that relies on those environment variables implicitly.

Named SQL parameters still exist and may be useful in tightly controlled situations, but agents should generally avoid generating examples like this when a real token is involved:

```sql
SELECT * FROM labkey_query(
    'lists',
    'Samples',
    base_url = 'https://labkey.example.com',
    container_path = '/MyProject',
    api_key = 'real-secret-token'
);
```

That pattern is too easy to leak into transcripts, notebooks, query logs, and copy-pasted snippets.

Credential resolution order is:

1. named SQL parameters
2. environment variables
3. `.netrc`
4. guest access

For agent-driven workflows, the practical rule is:

```text
Have the user set LABKEY_BASE_URL, LABKEY_CONTAINER, and LABKEY_API_KEY first.
Then run DuckDB SQL without embedding secrets in the SQL itself.
```

## Recommended workflow for agents

For large tables, do **not** make `labkey_query(...)` your default read path.

Preferred workflow:

1. sync the table into the local cache **when needed**
2. query the synced table directly by name

Example:

```bash
duckdb <<'SQL'
INSTALL duck_lk FROM community;
LOAD duck_lk;

CALL labkey_sync('lists', 'Samples');

FROM Samples LIMIT 10;
SQL
```

Why this is preferred:

- `labkey_sync()` warms the Parquet cache
- afterward, direct table reads can go through DuckDB's native `read_parquet` path via the replacement scan
- this avoids routing the final query through the row-producing `labkey_query(...)` VTab machinery

That is the highest-performance path in the extension.

Important: the cache lives on disk, not just in a single DuckDB process. A new `duckdb -c` invocation still needs `INSTALL`/`LOAD`, but it does **not** necessarily need `CALL labkey_sync(...)` again if the table is already cached.

So the real rule is:

> _sync as little as possible without letting the data go stale_

For an already-cached table, a later invocation can simply do:

```bash
duckdb -markdown <<'SQL'
LOAD duck_lk;

FROM Samples LIMIT 10;
SQL
```

## When to use `labkey_query(...)`

Use `labkey_query(...)` when you want a single SQL statement that can fetch-on-demand and return rows immediately:

```sql
SELECT * FROM labkey_query('lists', 'Samples');
```

Or when you want explicit cache-only behavior:

```sql
SELECT * FROM labkey_query('lists', 'Samples', offline = true);
```

But for repeated analysis or large-table work, prefer:

```sql
CALL labkey_sync('lists', 'Samples');
FROM Samples;
```

## Recommended non-interactive patterns

### Cold cache: sync, then analyze

```bash
duckdb -markdown <<'SQL'
INSTALL duck_lk FROM community;
LOAD duck_lk;

CALL labkey_sync('lists', 'Samples');

SELECT
    sample_type,
    count(*) AS n
FROM Samples
GROUP BY ALL
ORDER BY n DESC;
SQL
```

### Join multiple synced tables

```bash
duckdb -markdown <<'SQL'
INSTALL duck_lk FROM community;
LOAD duck_lk;

CALL labkey_sync('lists', 'Samples');
CALL labkey_sync('lists', 'Measurements');

SELECT
    s.sample_id,
    s.sample_type,
    m.result
FROM Samples s
JOIN Measurements m USING (sample_id)
LIMIT 20;
SQL
```

### Materialize a local DuckDB table from synced data

```bash
duckdb <<'SQL'
INSTALL duck_lk FROM community;
LOAD duck_lk;

CALL labkey_sync('lists', 'Samples');

CREATE OR REPLACE TABLE samples_local AS
FROM Samples;
SQL
```

### Warm cache: reuse the synced table directly

```bash
duckdb -markdown <<'SQL'
LOAD duck_lk;

SELECT sample_type, count(*) AS n
FROM Samples
GROUP BY ALL
ORDER BY n DESC;
SQL
```

## Cache inspection and management

Useful helper functions:

```sql
SELECT * FROM labkey_cache_info();
CALL labkey_cache_clear(schema = 'lists', query = 'Samples');
CALL labkey_cache_clear();
SELECT labkey_parquet_path('lists', 'Samples');
SELECT labkey_is_stale('lists', 'Samples');
```

Interpretation:

- `labkey_cache_info()` shows what is cached and how large it is
- `labkey_cache_clear()` removes cache entries
- `labkey_parquet_path()` tells you where the cached file lives
- `labkey_is_stale()` is a helper, but not the primary recommended workflow for agents

## How synchronization actually works

Important mental model:

- initial sync downloads the **entire** LabKey table
- `WHERE` clauses do **not** reduce the first fetch size
- once cached, local DuckDB queries are fast

So this:

```sql
SELECT * FROM labkey_query('lists', 'Samples') LIMIT 10;
```

may still require a full initial table sync if the cache is cold.

For large tables, agents should assume:

```text
if not cached (or if an explicit refresh is desired): CALL labkey_sync(...)
then query the synced table directly
```

## Finding schema and query names

Given a LabKey URL like:

```text
https://labkey.example.com/MyProject/list-grid.view?name=People
```

The relevant pieces are:

- base URL: `https://labkey.example.com`
- container: `/MyProject`
- schema: `lists`
- query: `People`

Common schemas include `lists`, `core`, `study`, and `assay`.

## Common mistakes

### Treating `labkey_query(...)` as the preferred analysis path

It is convenient, but it is not the best-performance workflow for repeated or large-table analysis.

Prefer:

```sql
-- If needed:
CALL labkey_sync('lists', 'Samples');

-- Then reuse the cached table directly:
FROM Samples;
```

### Assuming server-side filtering on initial fetch

The initial sync is whole-table.

### Forgetting that synced tables become directly queryable

After sync, this is valid and recommended:

```sql
FROM Samples;
```

That “query by bare table name after sync” behavior is intentional and performance-relevant, not a curiosity.

### Using LabKey SQL or PostgreSQL syntax mentally

Once data is in DuckDB, use DuckDB SQL.
