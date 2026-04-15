//! `labkey_query` table function — the primary `VTab` for querying `LabKey` tables.
//!
//! Implements bind/init/func for the `labkey_query(schema_name, query_name)`
//! table function. Provides automatic local Parquet caching with staleness
//! detection.
//!
//! Control flow:
//! - `bind()`: resolves credentials, checks cache, performs staleness check,
//!   fetches column metadata from `LabKey` if needed.
//! - `init()`: reads cached Parquet or fetches all rows and writes to cache.
//! - `func()`: writes `RecordBatch` data to `DuckDB` vectors in chunks.

use std::{env, error::Error, path::PathBuf, sync::Mutex, time::Duration};

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use indicatif::{ProgressBar, ProgressStyle};
use labkey_rs::{
    query::{QueryColumn, SelectRowsOptions, ShowRows},
    ClientConfig, LabkeyClient,
};
use libduckdb_sys::{duckdb_date, duckdb_timestamp};
use tokio::runtime::Runtime;

use super::{cache, credential, types};

const DEBUG_ENV_VAR: &str = "DUCK_LK_DEBUG";
const PHASE_PROGRESS_TICK_MS: u64 = 100;

/// Bind data for the `labkey_query` table function.
///
/// Stores the resolved connection configuration, column schema, and cache
/// state determined during `bind()`. The `parquet_path` field signals to
/// `init()` whether to read from cache (`Some`) or fetch fresh data (`None`).
pub(crate) struct LabkeyBindData {
    /// Column names in schema order.
    column_names: Vec<String>,
    /// Column `DuckDB` types in schema order.
    column_types: Vec<LogicalTypeId>,
    /// Original `LabKey` JSON type strings in schema order.
    column_json_types: Vec<String>,
    /// Resolved `LabKey` connection configuration.
    config: credential::ResolvedConfig,
    /// `LabKey` schema name (positional param 0).
    schema_name: String,
    /// `LabKey` query name (positional param 1).
    query_name: String,
    /// Whether to skip staleness check and serve only from cache. Currently
    /// resolved entirely in `bind()`, but retained for future use in `init()`
    /// (e.g. to suppress network retries on cache-miss recovery).
    #[cfg_attr(not(test), allow(dead_code))]
    offline: bool,
    /// Path to the Parquet file to read (set after cache resolution).
    /// `Some` = cache hit, `None` = cache miss / stale.
    parquet_path: Option<PathBuf>,
}

/// Init data for the `labkey_query` table function.
///
/// Holds a streaming Parquet reader plus the current in-memory Arrow batch.
pub(crate) struct LabkeyInitData {
    stream: Mutex<ParquetStreamState>,
}

struct ParquetStreamState {
    reader: cache::ParquetBatchIterator,
    current_batch: Option<arrow_array::RecordBatch>,
    row_offset: usize,
}

pub(crate) struct LabkeyQueryVTab;

impl VTab for LabkeyQueryVTab {
    type InitData = LabkeyInitData;
    type BindData = LabkeyBindData;

    #[allow(clippy::too_many_lines)] // VTab bind has inherently sequential steps
    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        // 1. Read positional parameters
        let schema_name = bind.get_parameter(0).to_string();
        let query_name = bind.get_parameter(1).to_string();

        // 2. Read named parameters
        let param_base_url = bind.get_named_parameter("base_url").map(|v| v.to_string());
        let param_container_path = bind
            .get_named_parameter("container_path")
            .map(|v| v.to_string());
        let param_api_key = bind.get_named_parameter("api_key").map(|v| v.to_string());
        let offline = bind
            .get_named_parameter("offline")
            .is_some_and(|v| v.to_string() == "true");

        // 3. Resolve credentials
        let config =
            credential::resolve_config(param_base_url, param_container_path, param_api_key)?;

        // 4. Compute cache key
        let key = cache::cache_key(
            &config.base_url,
            &config.container_path,
            &schema_name,
            &query_name,
        )?;

        // 5. Check cache
        let mgr = cache::CacheManager::new()?;
        let cached_entry = mgr.get_entry(&key);

        // 6. Offline mode
        if offline {
            return match cached_entry {
                Some(entry) => {
                    register_columns_from_cache(bind, &entry);
                    Ok(bind_data_from_cache(
                        config,
                        &entry,
                        &mgr,
                        schema_name,
                        query_name,
                        offline,
                    ))
                }
                None => Err(format!(
                    "No cached data for {schema_name}.{query_name}. \
                     Run without offline = true to fetch from LabKey."
                )
                .into()),
            };
        }

        // 7. Cache hit — use cached schema and defer staleness validation to init.
        if let Some(entry) = &cached_entry {
            register_columns_from_cache(bind, entry);
            return Ok(bind_data_from_cache(
                config,
                entry,
                &mgr,
                schema_name,
                query_name,
                offline,
            ));
        }

        // 8. Fresh fetch — get metadata from LabKey
        let rt = Runtime::new()?;
        let client_config = ClientConfig::new(
            config.base_url.clone(),
            config.credential.clone(),
            config.container_path.clone(),
        );
        let client = LabkeyClient::new(client_config)?;

        let meta_opts = SelectRowsOptions::builder()
            .schema_name(schema_name.clone())
            .query_name(query_name.clone())
            .include_metadata(true)
            .show_rows(ShowRows::None)
            .build();
        let meta_response = rt.block_on(client.select_rows(meta_opts))?;

        let meta_data = meta_response.meta_data.ok_or(
            "LabKey did not return column metadata. \
             The schema or query may not exist.",
        )?;

        let filtered_columns = filter_columns(meta_data.fields);

        for col in &filtered_columns {
            let duckdb_type = types::map_json_type_to_duckdb(col.json_type.as_deref());
            bind.add_result_column(&col.name, LogicalTypeHandle::from(duckdb_type));
        }

        let column_names: Vec<String> = filtered_columns.iter().map(|c| c.name.clone()).collect();
        let column_types: Vec<LogicalTypeId> = filtered_columns
            .iter()
            .map(|c| types::map_json_type_to_duckdb(c.json_type.as_deref()))
            .collect();
        let column_json_types: Vec<String> = filtered_columns
            .iter()
            .map(|c| c.json_type.clone().unwrap_or_default())
            .collect();

        Ok(LabkeyBindData {
            column_names,
            column_types,
            column_json_types,
            config,
            schema_name,
            query_name,
            offline,
            parquet_path: None,
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<LabkeyBindData>() };

        let parquet_path = if let Some(ref path) = bind_data.parquet_path {
            if bind_data.offline {
                path.clone()
            } else {
                let mgr = cache::CacheManager::new()?;
                let key = cache::cache_key(
                    &bind_data.config.base_url,
                    &bind_data.config.container_path,
                    &bind_data.schema_name,
                    &bind_data.query_name,
                )?;
                let cached_entry = mgr.get_entry(&key).ok_or_else(|| {
                    format!(
                        "No cached data for {}.{}",
                        bind_data.schema_name, bind_data.query_name
                    )
                })?;

                let is_fresh = match &cached_entry.server_modified {
                    None => true,
                    Some(cached_modified) => {
                        let spinner = build_phase_spinner(format!(
                            "Checking whether {}.{} is stale...",
                            bind_data.schema_name, bind_data.query_name
                        ));
                        let freshness = check_freshness(
                            &bind_data.config,
                            &bind_data.schema_name,
                            &bind_data.query_name,
                            cached_modified,
                        );
                        match freshness {
                            Some(true) => {
                                spinner.finish_with_message(format!(
                                    "Cached data for {}.{} is fresh",
                                    bind_data.schema_name, bind_data.query_name
                                ));
                                true
                            }
                            Some(false) => {
                                spinner.finish_with_message(format!(
                                    "Cached data for {}.{} is stale; refreshing...",
                                    bind_data.schema_name, bind_data.query_name
                                ));
                                false
                            }
                            None => {
                                spinner.finish_with_message(format!(
                                    "Could not determine staleness for {}.{}; using cached data",
                                    bind_data.schema_name, bind_data.query_name
                                ));
                                true
                            }
                        }
                    }
                };

                if is_fresh {
                    path.clone()
                } else {
                    let refreshed_path = sync_to_cache(
                        &bind_data.config,
                        &bind_data.schema_name,
                        &bind_data.query_name,
                        &[],
                        &[],
                    )?;

                    let refreshed_entry = mgr.get_entry(&key).ok_or_else(|| {
                        format!(
                            "Cache refresh for {}.{} did not produce a cache entry",
                            bind_data.schema_name, bind_data.query_name
                        )
                    })?;

                    if !cache_columns_match_bind(bind_data, &refreshed_entry) {
                        return Err(format!(
                            "Schema for {}.{} changed after cache refresh; rerun the query so DuckDB can rebind the new columns",
                            bind_data.schema_name, bind_data.query_name
                        )
                        .into());
                    }

                    refreshed_path
                }
            }
        } else {
            fetch_and_cache(bind_data)?
        };

        let reader = cache::CacheManager::open_parquet_batch_reader(&parquet_path, 2048)?;

        init.set_max_threads(1);

        Ok(LabkeyInitData {
            stream: Mutex::new(ParquetStreamState {
                reader,
                current_batch: None,
                row_offset: 0,
            }),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();

        let mut stream = init_data
            .stream
            .lock()
            .map_err(|_| "Parquet stream mutex poisoned")?;

        loop {
            if stream.current_batch.is_none() && !advance_to_next_batch(&mut stream)? {
                output.set_len(0);
                return Ok(());
            }

            let Some(current_batch) = stream.current_batch.as_ref() else {
                output.set_len(0);
                return Ok(());
            };

            let remaining = current_batch.num_rows().saturating_sub(stream.row_offset);
            if remaining == 0 {
                if !advance_to_next_batch(&mut stream)? {
                    output.set_len(0);
                    return Ok(());
                }
                continue;
            }

            let offset = stream.row_offset;
            let chunk_size = std::cmp::min(2048, remaining);

            debug_assert_eq!(
                bind_data.column_names.len(),
                bind_data.column_types.len(),
                "column_names and column_types must have the same length"
            );
            debug_assert_eq!(
                bind_data.column_names.len(),
                current_batch.num_columns(),
                "bind column count must match RecordBatch column count"
            );

            for col_idx in 0..bind_data.column_names.len() {
                let array = current_batch.column(col_idx);
                let mut vector = output.flat_vector(col_idx);

                match bind_data.column_types[col_idx] {
                    LogicalTypeId::Bigint => {
                        write_i64_column(&mut vector, array, offset, chunk_size)?;
                    }
                    LogicalTypeId::Double => {
                        write_f64_column(&mut vector, array, offset, chunk_size)?;
                    }
                    LogicalTypeId::Boolean => {
                        write_bool_column(&mut vector, array, offset, chunk_size)?;
                    }
                    LogicalTypeId::Timestamp => {
                        write_timestamp_column(&mut vector, array, offset, chunk_size)?;
                    }
                    LogicalTypeId::Date => {
                        write_date_column(&mut vector, array, offset, chunk_size)?;
                    }
                    _ => {
                        write_varchar_column(&mut vector, array, offset, chunk_size)?;
                    }
                }
            }

            stream.row_offset += chunk_size;
            output.set_len(chunk_size);
            return Ok(());
        }
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // schema_name
            LogicalTypeHandle::from(LogicalTypeId::Varchar), // query_name
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            (
                "base_url".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "container_path".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "api_key".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "offline".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Boolean),
            ),
        ])
    }
}

/// Filters out hidden columns and internal `_labkeyurl_` columns.
pub(crate) fn filter_columns(fields: Vec<QueryColumn>) -> Vec<QueryColumn> {
    fields
        .into_iter()
        .filter(|col| !col.hidden && !col.name.starts_with(labkey_rs::query::URL_COLUMN_PREFIX))
        .collect()
}

/// Constructs `LabkeyBindData` from a cached entry (used for cache hit and
/// offline paths).
fn bind_data_from_cache(
    config: credential::ResolvedConfig,
    entry: &cache::CacheEntry,
    mgr: &cache::CacheManager,
    schema_name: String,
    query_name: String,
    offline: bool,
) -> LabkeyBindData {
    LabkeyBindData {
        column_names: entry.columns.iter().map(|c| c.name.clone()).collect(),
        column_types: entry
            .columns
            .iter()
            .map(|c| types::map_json_type_to_duckdb(c.json_type.as_deref()))
            .collect(),
        column_json_types: entry
            .columns
            .iter()
            .map(|c| c.json_type.clone().unwrap_or_default())
            .collect(),
        config,
        schema_name,
        query_name,
        offline,
        parquet_path: Some(mgr.parquet_path(entry)),
    }
}

/// Registers result columns from a cached entry's column metadata.
fn register_columns_from_cache(bind: &BindInfo, entry: &cache::CacheEntry) {
    for col in &entry.columns {
        let duckdb_type = types::map_json_type_to_duckdb(col.json_type.as_deref());
        bind.add_result_column(&col.name, LogicalTypeHandle::from(duckdb_type));
    }
}

/// Performs a staleness check against the `LabKey` server.
///
/// Returns `Some(true)` if the cache is fresh (server timestamp matches
/// cached), `Some(false)` if stale (server has newer data). Returns `None`
/// if the check failed (network error, runtime creation failure, etc.) —
/// callers should treat `None` as "assume fresh" via `.unwrap_or(true)`.
fn check_freshness(
    config: &credential::ResolvedConfig,
    schema_name: &str,
    query_name: &str,
    cached_modified: &str,
) -> Option<bool> {
    let rt = Runtime::new().ok()?;
    let client_config = ClientConfig::new(
        config.base_url.clone(),
        config.credential.clone(),
        config.container_path.clone(),
    );
    let client = LabkeyClient::new(client_config).ok()?;

    let server_modified =
        cache::CacheManager::check_staleness(&client, &rt, schema_name, query_name)?;

    Some(server_modified == cached_modified)
}

fn cache_columns_match_bind(bind_data: &LabkeyBindData, entry: &cache::CacheEntry) -> bool {
    let entry_names: Vec<&str> = entry.columns.iter().map(|c| c.name.as_str()).collect();
    let bind_names: Vec<&str> = bind_data.column_names.iter().map(String::as_str).collect();
    let entry_types: Vec<&str> = entry
        .columns
        .iter()
        .map(|c| c.json_type.as_deref().unwrap_or(""))
        .collect();
    let bind_types: Vec<&str> = bind_data
        .column_json_types
        .iter()
        .map(String::as_str)
        .collect();

    entry_names == bind_names && entry_types == bind_types
}

fn build_phase_spinner(message: String) -> ProgressBar {
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    spinner.set_message(message);
    spinner.enable_steady_tick(Duration::from_millis(PHASE_PROGRESS_TICK_MS));
    spinner
}

struct FetchResult {
    columns: Vec<cache::CacheColumn>,
    row_count: usize,
    size_bytes: u64,
}

struct CacheWriteTarget {
    key: String,
    relative_path: String,
    absolute_path: PathBuf,
}

struct ColumnMetadata {
    column_names: Vec<String>,
    column_json_types: Vec<String>,
}

fn debug_logging_enabled_value(value: Option<&str>) -> bool {
    value.is_some_and(|v| {
        matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn debug_logging_enabled() -> bool {
    debug_logging_enabled_value(env::var(DEBUG_ENV_VAR).ok().as_deref())
}

pub(crate) fn debug_log(message: impl AsRef<str>) {
    if debug_logging_enabled() {
        eprintln!("[duck-lk] {}", message.as_ref());
    }
}

pub(crate) fn format_elapsed(elapsed: Duration) -> String {
    let secs = elapsed.as_secs();
    let millis = elapsed.subsec_millis();
    if secs >= 60 {
        let minutes = secs / 60;
        let rem = secs % 60;
        format!("{minutes}m{rem:02}s")
    } else if secs > 0 {
        format!("{secs}.{millis:03}s")
    } else {
        format!("0.{millis:03}s")
    }
}

pub(crate) fn format_rows_per_second(rows: usize, elapsed: Duration) -> String {
    let millis = elapsed.as_millis();
    if millis == 0 {
        return "- rows/s".to_string();
    }
    let rows_per_second = ((rows as u128) * 1_000) / millis;
    format!("{rows_per_second} rows/s")
}

fn resolve_cache_write_target(
    mgr: &cache::CacheManager,
    config: &credential::ResolvedConfig,
    schema_name: &str,
    query_name: &str,
) -> Result<CacheWriteTarget, Box<dyn Error>> {
    let key = cache::cache_key(
        &config.base_url,
        &config.container_path,
        schema_name,
        query_name,
    )?;
    let relative_path = cache::parquet_relative_path(
        &config.base_url,
        &config.container_path,
        schema_name,
        query_name,
    )?;
    let absolute_path = mgr.parquet_path_from_relative(&relative_path);
    Ok(CacheWriteTarget {
        key,
        relative_path,
        absolute_path,
    })
}

fn persist_cache_entry(
    mgr: &cache::CacheManager,
    target: CacheWriteTarget,
    config: &credential::ResolvedConfig,
    schema_name: &str,
    query_name: &str,
    fetch_result: FetchResult,
    server_modified: Option<String>,
) -> Result<PathBuf, Box<dyn Error>> {
    let row_count = i64::try_from(fetch_result.row_count).unwrap_or(i64::MAX);
    let entry = cache::CacheEntry {
        parquet_path: target.relative_path,
        fetched_at: chrono::Utc::now().to_rfc3339(),
        server_modified,
        row_count,
        size_bytes: fetch_result.size_bytes,
        base_url: config.base_url.clone(),
        container_path: config.container_path.clone(),
        schema_name: schema_name.to_owned(),
        query_name: query_name.to_owned(),
        columns: fetch_result.columns,
    };
    mgr.put_entry(&target.key, &entry)?;
    Ok(target.absolute_path)
}

fn fetch_column_metadata(
    config: &credential::ResolvedConfig,
    schema_name: &str,
    query_name: &str,
) -> Result<ColumnMetadata, Box<dyn Error>> {
    let rt = Runtime::new()?;
    let client_config = ClientConfig::new(
        config.base_url.clone(),
        config.credential.clone(),
        config.container_path.clone(),
    );
    let client = LabkeyClient::new(client_config)?;

    let meta_opts = SelectRowsOptions::builder()
        .schema_name(schema_name.to_owned())
        .query_name(query_name.to_owned())
        .include_metadata(true)
        .show_rows(ShowRows::None)
        .build();
    let meta_response = rt.block_on(client.select_rows(meta_opts))?;

    let meta_data = meta_response
        .meta_data
        .ok_or("LabKey did not return column metadata. The schema or query may not exist.")?;
    let filtered_columns = filter_columns(meta_data.fields);

    Ok(ColumnMetadata {
        column_names: filtered_columns.iter().map(|c| c.name.clone()).collect(),
        column_json_types: filtered_columns
            .iter()
            .map(|c| c.json_type.clone().unwrap_or_default())
            .collect(),
    })
}

fn resolve_sync_columns(
    config: &credential::ResolvedConfig,
    schema_name: &str,
    query_name: &str,
    column_names: &[String],
    column_json_types: &[String],
) -> Result<(Vec<String>, Vec<String>), Box<dyn Error>> {
    if !column_names.is_empty() && column_names.len() == column_json_types.len() {
        return Ok((column_names.to_vec(), column_json_types.to_vec()));
    }

    let metadata = fetch_column_metadata(config, schema_name, query_name)?;
    Ok((metadata.column_names, metadata.column_json_types))
}

fn advance_to_next_batch(state: &mut ParquetStreamState) -> Result<bool, Box<dyn Error>> {
    match state.reader.next() {
        Some(Ok(batch)) => {
            state.current_batch = Some(batch);
            state.row_offset = 0;
            Ok(true)
        }
        Some(Err(err)) => Err(Box::new(err)),
        None => {
            state.current_batch = None;
            state.row_offset = 0;
            Ok(false)
        }
    }
}

#[allow(clippy::doc_markdown)]
/// Syncs a LabKey table to the local Parquet cache and returns the path to
/// the cached file. This is the shared implementation used by both
/// `labkey_query` (VTab) and `labkey_sync` (CALL-only).
pub(crate) fn sync_to_cache(
    config: &credential::ResolvedConfig,
    schema_name: &str,
    query_name: &str,
    column_names: &[String],
    column_json_types: &[String],
) -> Result<std::path::PathBuf, Box<dyn Error>> {
    let mgr = cache::CacheManager::new()?;
    let target = resolve_cache_write_target(&mgr, config, schema_name, query_name)?;
    let (column_names, column_json_types) = resolve_sync_columns(
        config,
        schema_name,
        query_name,
        column_names,
        column_json_types,
    )?;

    let sql_result = super::experimental_sql::fetch_experimental_sql_to_parquet(
        config,
        schema_name,
        query_name,
        &column_names,
        &column_json_types,
        &target.absolute_path,
    )?;

    let fetch_result = FetchResult {
        columns: column_names
            .iter()
            .zip(column_json_types.iter())
            .map(|(name, jt)| cache::CacheColumn {
                name: name.clone(),
                json_type: if jt.is_empty() {
                    None
                } else {
                    Some(jt.clone())
                },
            })
            .collect(),
        row_count: sql_result.row_count,
        size_bytes: sql_result.size_bytes,
    };

    let rt = Runtime::new()?;
    let client_config = ClientConfig::new(
        config.base_url.clone(),
        config.credential.clone(),
        config.container_path.clone(),
    );
    let client = LabkeyClient::new(client_config)?;

    let server_modified =
        cache::CacheManager::check_staleness(&client, &rt, schema_name, query_name);

    persist_cache_entry(
        &mgr,
        target,
        config,
        schema_name,
        query_name,
        fetch_result,
        server_modified,
    )
}

fn fetch_and_cache(bind_data: &LabkeyBindData) -> Result<std::path::PathBuf, Box<dyn Error>> {
    sync_to_cache(
        &bind_data.config,
        &bind_data.schema_name,
        &bind_data.query_name,
        &bind_data.column_names,
        &bind_data.column_json_types,
    )
}

/// Writes an `Int64` Arrow column to a `DuckDB` `FlatVector`.
///
/// When the column has no nulls, uses `copy_from_slice` to memcpy the entire
/// chunk in one operation. Otherwise falls back to a two-pass approach: first
/// writes all values (with 0 for null slots), then marks null positions via
/// `set_null`.
fn write_i64_column(
    vector: &mut duckdb::core::FlatVector,
    array: &dyn Array,
    offset: usize,
    chunk_size: usize,
) -> Result<(), Box<dyn Error>> {
    let arr = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or("Column type mismatch: expected Int64Array in RecordBatch")?;
    let slice = vector.as_mut_slice::<i64>();
    if arr.null_count() == 0 {
        let src = &arr.values().as_ref()[offset..offset + chunk_size];
        slice[..chunk_size].copy_from_slice(src);
    } else {
        for (i, slot) in slice.iter_mut().enumerate().take(chunk_size) {
            let src_idx = offset + i;
            *slot = if arr.is_null(src_idx) {
                0
            } else {
                arr.value(src_idx)
            };
        }
        for i in 0..chunk_size {
            if arr.is_null(offset + i) {
                vector.set_null(i);
            }
        }
    }
    Ok(())
}

/// Writes a `Float64` Arrow column to a `DuckDB` `FlatVector`.
///
/// When the column has no nulls, uses `copy_from_slice` to memcpy the entire
/// chunk in one operation. Otherwise falls back to per-element copy with null
/// marking.
fn write_f64_column(
    vector: &mut duckdb::core::FlatVector,
    array: &dyn Array,
    offset: usize,
    chunk_size: usize,
) -> Result<(), Box<dyn Error>> {
    let arr = array
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or("Column type mismatch: expected Float64Array in RecordBatch")?;
    let slice = vector.as_mut_slice::<f64>();
    if arr.null_count() == 0 {
        let src = &arr.values().as_ref()[offset..offset + chunk_size];
        slice[..chunk_size].copy_from_slice(src);
    } else {
        for (i, slot) in slice.iter_mut().enumerate().take(chunk_size) {
            let src_idx = offset + i;
            *slot = if arr.is_null(src_idx) {
                0.0
            } else {
                arr.value(src_idx)
            };
        }
        for i in 0..chunk_size {
            if arr.is_null(offset + i) {
                vector.set_null(i);
            }
        }
    }
    Ok(())
}

/// Writes a `Boolean` Arrow column to a `DuckDB` `FlatVector`.
fn write_bool_column(
    vector: &mut duckdb::core::FlatVector,
    array: &dyn Array,
    offset: usize,
    chunk_size: usize,
) -> Result<(), Box<dyn Error>> {
    let arr = array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or("Column type mismatch: expected BooleanArray in RecordBatch")?;
    let slice = vector.as_mut_slice::<bool>();
    for (i, slot) in slice.iter_mut().enumerate().take(chunk_size) {
        let src = offset + i;
        *slot = if arr.is_null(src) {
            false
        } else {
            arr.value(src)
        };
    }
    for i in 0..chunk_size {
        if arr.is_null(offset + i) {
            vector.set_null(i);
        }
    }
    Ok(())
}

/// Writes a `TimestampMicrosecond` Arrow column to a `DuckDB` `FlatVector`.
///
/// When the column has no nulls, reads directly from the Arrow values buffer
/// without per-element null checks. The type mismatch between `i64` and
/// `duckdb_timestamp { micros: i64 }` prevents a raw `copy_from_slice`, but
/// eliminating the branch per element is still a significant win at scale.
fn write_timestamp_column(
    vector: &mut duckdb::core::FlatVector,
    array: &dyn Array,
    offset: usize,
    chunk_size: usize,
) -> Result<(), Box<dyn Error>> {
    let arr = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or("Column type mismatch: expected TimestampMicrosecondArray in RecordBatch")?;
    let slice = vector.as_mut_slice::<duckdb_timestamp>();
    if arr.null_count() == 0 {
        let values = arr.values();
        for (i, slot) in slice.iter_mut().enumerate().take(chunk_size) {
            slot.micros = values[offset + i];
        }
    } else {
        for (i, slot) in slice.iter_mut().enumerate().take(chunk_size) {
            let src_idx = offset + i;
            *slot = if arr.is_null(src_idx) {
                duckdb_timestamp { micros: 0 }
            } else {
                duckdb_timestamp {
                    micros: arr.value(src_idx),
                }
            };
        }
        for i in 0..chunk_size {
            if arr.is_null(offset + i) {
                vector.set_null(i);
            }
        }
    }
    Ok(())
}

/// Writes a `Date32` Arrow column to a `DuckDB` `FlatVector`.
///
/// When the column has no nulls, reads directly from the Arrow values buffer
/// without per-element null checks. The type mismatch between `i32` and
/// `duckdb_date { days: i32 }` prevents a raw `copy_from_slice`, but
/// eliminating the branch per element is still a significant win at scale.
fn write_date_column(
    vector: &mut duckdb::core::FlatVector,
    array: &dyn Array,
    offset: usize,
    chunk_size: usize,
) -> Result<(), Box<dyn Error>> {
    let arr = array
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or("Column type mismatch: expected Date32Array in RecordBatch")?;
    let slice = vector.as_mut_slice::<duckdb_date>();
    if arr.null_count() == 0 {
        let values = arr.values();
        for (i, slot) in slice.iter_mut().enumerate().take(chunk_size) {
            slot.days = values[offset + i];
        }
    } else {
        for (i, slot) in slice.iter_mut().enumerate().take(chunk_size) {
            let src_idx = offset + i;
            *slot = if arr.is_null(src_idx) {
                duckdb_date { days: 0 }
            } else {
                duckdb_date {
                    days: arr.value(src_idx),
                }
            };
        }
        for i in 0..chunk_size {
            if arr.is_null(offset + i) {
                vector.set_null(i);
            }
        }
    }
    Ok(())
}

/// Writes a `Utf8` Arrow column to a `DuckDB` `FlatVector` via `Inserter`.
fn write_varchar_column(
    vector: &mut duckdb::core::FlatVector,
    array: &dyn Array,
    offset: usize,
    chunk_size: usize,
) -> Result<(), Box<dyn Error>> {
    let arr = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("Column type mismatch: expected StringArray in RecordBatch")?;
    for i in 0..chunk_size {
        let src = offset + i;
        if arr.is_null(src) {
            vector.set_null(i);
        } else {
            vector.insert(i, arr.value(src));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use serde_json::json;

    fn make_column(name: &str, json_type: Option<&str>, hidden: bool) -> QueryColumn {
        let mut col_json = json!({
            "name": name,
            "fieldKey": name,
            "hidden": hidden,
            "nullable": true,
            "keyField": false,
            "versionField": false,
            "readOnly": false,
            "userEditable": true,
            "autoIncrement": false,
            "mvEnabled": false,
            "selectable": true,
        });
        if let Some(jt) = json_type {
            col_json["jsonType"] = json!(jt);
        }
        serde_json::from_value(col_json).expect("QueryColumn deserialization")
    }

    fn sample_record_batch(names: &[&str], ages: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));
        let names = Arc::new(StringArray::from(
            names.iter().map(|name| Some(*name)).collect::<Vec<_>>(),
        ));
        let ages = Arc::new(Int64Array::from(
            ages.iter().map(|age| Some(*age)).collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, vec![names, ages]).expect("RecordBatch")
    }

    #[test]
    fn filter_columns_removes_hidden() {
        let fields = vec![
            make_column("visible", Some("string"), false),
            make_column("secret", Some("string"), true),
            make_column("also_visible", Some("int"), false),
        ];
        let result = filter_columns(fields);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "visible");
        assert_eq!(result[1].name, "also_visible");
    }

    #[test]
    fn filter_columns_removes_url_prefix() {
        let fields = vec![
            make_column("Name", Some("string"), false),
            make_column("_labkeyurl_Name", Some("string"), false),
            make_column("Id", Some("int"), false),
        ];
        let result = filter_columns(fields);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "Name");
        assert_eq!(result[1].name, "Id");
    }

    #[test]
    fn filter_columns_removes_both_hidden_and_url() {
        let fields = vec![
            make_column("Keep", Some("string"), false),
            make_column("HiddenCol", Some("string"), true),
            make_column("_labkeyurl_Keep", Some("string"), false),
            make_column("_labkeyurl_Hidden", Some("string"), true),
        ];
        let result = filter_columns(fields);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "Keep");
    }

    #[test]
    fn filter_columns_preserves_order() {
        let fields = vec![
            make_column("C", Some("string"), false),
            make_column("A", Some("string"), false),
            make_column("B", Some("string"), false),
        ];
        let result = filter_columns(fields);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].name, "C");
        assert_eq!(result[1].name, "A");
        assert_eq!(result[2].name, "B");
    }

    #[test]
    fn filter_columns_empty_input() {
        let result = filter_columns(vec![]);
        assert!(result.is_empty());
    }

    #[test]
    fn filter_columns_all_filtered() {
        let fields = vec![
            make_column("hidden", Some("string"), true),
            make_column("_labkeyurl_x", Some("string"), false),
        ];
        let result = filter_columns(fields);
        assert!(result.is_empty());
    }

    // -- bind_data_from_cache tests --

    fn sample_cache_entry() -> cache::CacheEntry {
        cache::CacheEntry {
            parquet_path: "labkey.example.com/labkey/Project/lists/People.parquet".into(),
            fetched_at: "2026-03-10T12:00:00Z".into(),
            server_modified: Some("2026-03-10T11:00:00Z".into()),
            row_count: 2,
            size_bytes: 123,
            base_url: "https://labkey.example.com/labkey".into(),
            container_path: "/Project".into(),
            schema_name: "lists".into(),
            query_name: "People".into(),
            columns: vec![
                cache::CacheColumn {
                    name: "Id".into(),
                    json_type: Some("int".into()),
                },
                cache::CacheColumn {
                    name: "Created".into(),
                    json_type: Some("dateTime".into()),
                },
                cache::CacheColumn {
                    name: "Label".into(),
                    json_type: None,
                },
            ],
        }
    }

    fn guest_config() -> credential::ResolvedConfig {
        credential::ResolvedConfig {
            base_url: "https://labkey.example.com/labkey".into(),
            container_path: "/Project".into(),
            credential: labkey_rs::Credential::Guest,
        }
    }

    #[test]
    fn bind_data_from_cache_populates_column_names_and_types() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = cache::CacheManager::with_dir(dir.path().to_path_buf()).expect("CacheManager");
        let entry = sample_cache_entry();

        let bind = bind_data_from_cache(
            guest_config(),
            &entry,
            &mgr,
            "lists".into(),
            "People".into(),
            false,
        );

        assert_eq!(bind.column_names, vec!["Id", "Created", "Label"]);
        assert_eq!(
            bind.column_types,
            vec![
                LogicalTypeId::Bigint,
                LogicalTypeId::Timestamp,
                LogicalTypeId::Varchar,
            ]
        );
        assert_eq!(bind.schema_name, "lists");
        assert_eq!(bind.query_name, "People");
        assert!(!bind.offline);
        assert!(bind.parquet_path.is_some());
    }

    #[test]
    fn bind_data_from_cache_sets_offline_flag() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = cache::CacheManager::with_dir(dir.path().to_path_buf()).expect("CacheManager");
        let entry = sample_cache_entry();

        let bind = bind_data_from_cache(
            guest_config(),
            &entry,
            &mgr,
            "lists".into(),
            "People".into(),
            true,
        );

        assert!(bind.offline);
    }

    #[test]
    fn bind_data_from_cache_maps_unknown_types_to_varchar() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = cache::CacheManager::with_dir(dir.path().to_path_buf()).expect("CacheManager");

        let entry = cache::CacheEntry {
            parquet_path: "host/s/q.parquet".into(),
            fetched_at: "2026-03-10T12:00:00Z".into(),
            server_modified: None,
            row_count: 0,
            size_bytes: 0,
            base_url: "https://host".into(),
            container_path: "/".into(),
            schema_name: "s".into(),
            query_name: "q".into(),
            columns: vec![
                cache::CacheColumn {
                    name: "T".into(),
                    json_type: Some("time".into()),
                },
                cache::CacheColumn {
                    name: "X".into(),
                    json_type: Some("mystery".into()),
                },
            ],
        };

        let config = credential::ResolvedConfig {
            base_url: "https://host".into(),
            container_path: "/".into(),
            credential: labkey_rs::Credential::Guest,
        };

        let bind = bind_data_from_cache(config, &entry, &mgr, "s".into(), "q".into(), false);

        assert_eq!(
            bind.column_types,
            vec![LogicalTypeId::Varchar, LogicalTypeId::Varchar]
        );
    }

    #[test]
    fn debug_logging_enabled_value_truthy_values() {
        for value in ["1", "true", "TRUE", " yes ", "on"] {
            assert!(
                debug_logging_enabled_value(Some(value)),
                "{value} should enable debug logging"
            );
        }
    }

    #[test]
    fn debug_logging_enabled_value_falsey_values() {
        for value in [
            None,
            Some(""),
            Some("0"),
            Some("false"),
            Some("no"),
            Some("off"),
            Some("anything-else"),
        ] {
            assert!(
                !debug_logging_enabled_value(value),
                "{value:?} should disable debug logging"
            );
        }
    }

    #[test]
    fn advance_to_next_batch_resets_offset_and_exhausts_reader() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("streamed.parquet");
        let batch1 = sample_record_batch(&["Alice", "Bob"], &[30, 31]);
        let batch2 = sample_record_batch(&["Carol", "Dave"], &[40, 41]);

        let mut writer = cache::IncrementalParquetWriter::try_new(&path, batch1.schema())
            .expect("IncrementalParquetWriter::try_new");
        writer.write(&batch1).expect("write batch1");
        writer.write(&batch2).expect("write batch2");
        writer.finish().expect("finish writer");

        let mut reader = cache::CacheManager::open_parquet_batch_reader(&path, 2)
            .expect("open_parquet_batch_reader");
        let first = reader
            .next()
            .expect("first batch exists")
            .expect("first batch ok");
        let mut state = ParquetStreamState {
            reader,
            current_batch: Some(first),
            row_offset: 2,
        };

        assert!(advance_to_next_batch(&mut state).expect("advance to second batch"));
        assert_eq!(state.row_offset, 0);
        assert_eq!(
            state
                .current_batch
                .as_ref()
                .expect("current batch")
                .num_rows(),
            2
        );

        assert!(!advance_to_next_batch(&mut state).expect("reader exhausted"));
        assert_eq!(state.row_offset, 0);
        assert!(state.current_batch.is_none());
    }
}
