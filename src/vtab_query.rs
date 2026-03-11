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

use std::{
    error::Error,
    sync::atomic::{AtomicUsize, Ordering},
};

use arrow_array::{
    Array, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};
use labkey_rs::{
    query::{QueryColumn, SelectRowsOptions, ShowRows},
    ClientConfig, LabkeyClient,
};
use libduckdb_sys::{duckdb_date, duckdb_timestamp};

use super::{cache, credential, types};

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
    parquet_path: Option<std::path::PathBuf>,
}

/// Init data for the `labkey_query` table function.
///
/// Holds the fully-materialized `RecordBatch` (from cache or fresh fetch) and
/// an atomic row offset for chunked output to `DuckDB`.
pub(crate) struct LabkeyInitData {
    /// All rows as a column-major Arrow `RecordBatch`.
    record_batch: arrow_array::RecordBatch,
    /// Current row offset (how many rows have been emitted so far).
    /// Uses `AtomicUsize` because the `VTab` trait requires `Send + Sync` on
    /// init data. Safe with `Relaxed` ordering because `set_max_threads(1)`
    /// ensures single-threaded scanning.
    row_offset: AtomicUsize,
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

        // 7. Cache hit — check staleness
        if let Some(entry) = &cached_entry {
            let is_fresh = match &entry.server_modified {
                None => true,
                Some(cached_modified) => {
                    check_freshness(&config, &schema_name, &query_name, cached_modified)
                        .unwrap_or(true)
                }
            };

            if is_fresh {
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
        }

        // 8. Fresh fetch — get metadata from LabKey
        let rt = tokio::runtime::Runtime::new()?;
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

        Ok(LabkeyBindData {
            column_names,
            column_types,
            config,
            schema_name,
            query_name,
            offline,
            parquet_path: None,
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<LabkeyBindData>() };

        let record_batch = if let Some(ref path) = bind_data.parquet_path {
            cache::CacheManager::read_parquet(path)?
        } else {
            fetch_and_cache(bind_data)?
        };

        init.set_max_threads(1);

        Ok(LabkeyInitData {
            record_batch,
            row_offset: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();

        let offset = init_data.row_offset.load(Ordering::Relaxed);
        let total_rows = init_data.record_batch.num_rows();
        let remaining = total_rows.saturating_sub(offset);

        if remaining == 0 {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = std::cmp::min(2048, remaining);

        debug_assert_eq!(
            bind_data.column_names.len(),
            bind_data.column_types.len(),
            "column_names and column_types must have the same length"
        );
        debug_assert_eq!(
            bind_data.column_names.len(),
            init_data.record_batch.num_columns(),
            "bind column count must match RecordBatch column count"
        );

        for col_idx in 0..bind_data.column_names.len() {
            let array = init_data.record_batch.column(col_idx);
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

        init_data
            .row_offset
            .store(offset + chunk_size, Ordering::Relaxed);
        output.set_len(chunk_size);

        Ok(())
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
    let rt = tokio::runtime::Runtime::new().ok()?;
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

/// Fetches all rows from `LabKey`, writes them to the Parquet cache, and
/// returns the resulting `RecordBatch`.
fn fetch_and_cache(bind_data: &LabkeyBindData) -> Result<arrow_array::RecordBatch, Box<dyn Error>> {
    let rt = tokio::runtime::Runtime::new()?;
    let client_config = ClientConfig::new(
        bind_data.config.base_url.clone(),
        bind_data.config.credential.clone(),
        bind_data.config.container_path.clone(),
    );
    let client = LabkeyClient::new(client_config)?;

    let data_opts = SelectRowsOptions::builder()
        .schema_name(bind_data.schema_name.clone())
        .query_name(bind_data.query_name.clone())
        .include_metadata(true)
        .build();
    let data_response = rt.block_on(client.select_rows(data_opts))?;

    let meta_data = data_response
        .meta_data
        .ok_or("LabKey did not return column metadata on data fetch.")?;
    let filtered_columns = filter_columns(meta_data.fields);

    let batch = types::rows_to_record_batch(&data_response.rows, &filtered_columns)?;

    let mgr = cache::CacheManager::new()?;
    let key = cache::cache_key(
        &bind_data.config.base_url,
        &bind_data.config.container_path,
        &bind_data.schema_name,
        &bind_data.query_name,
    )?;

    let relative_path = cache::parquet_relative_path(
        &bind_data.config.base_url,
        &bind_data.config.container_path,
        &bind_data.schema_name,
        &bind_data.query_name,
    )?;

    let server_modified = cache::CacheManager::check_staleness(
        &client,
        &rt,
        &bind_data.schema_name,
        &bind_data.query_name,
    );
    let row_count = i64::try_from(batch.num_rows()).unwrap_or(i64::MAX);
    let columns: Vec<cache::CacheColumn> = filtered_columns
        .iter()
        .map(|c| cache::CacheColumn {
            name: c.name.clone(),
            json_type: c.json_type.clone(),
        })
        .collect();

    let pq_path_str = relative_path;
    let entry_without_size = cache::CacheEntry {
        parquet_path: pq_path_str,
        fetched_at: chrono::Utc::now().to_rfc3339(),
        server_modified,
        row_count,
        size_bytes: 0,
        base_url: bind_data.config.base_url.clone(),
        container_path: bind_data.config.container_path.clone(),
        schema_name: bind_data.schema_name.clone(),
        query_name: bind_data.query_name.clone(),
        columns,
    };

    let pq_path = mgr.parquet_path(&entry_without_size);
    let size = cache::CacheManager::write_parquet(&pq_path, &batch)?;

    let entry = cache::CacheEntry {
        size_bytes: size,
        ..entry_without_size
    };
    mgr.put_entry(&key, &entry)?;

    Ok(batch)
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
}
