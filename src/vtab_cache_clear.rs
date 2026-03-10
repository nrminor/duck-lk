//! `labkey_cache_clear` table function — cache management.
//!
//! Clears cached data for a specific table (by schema + query name) or all
//! tables at once. Returns a single-row result with a `status` column
//! describing what was cleared.
//!
//! This `VTab` performs no network I/O and requires no credentials — it
//! operates purely on local cache files via [`CacheManager`].

use std::{error::Error, sync::atomic::AtomicBool};

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};

use super::cache;

/// What the user wants to clear. Constructed by [`parse_clear_target`] from
/// the raw `Option<String>` named parameters — invalid combinations (one
/// provided, one missing) are rejected at parse time so they cannot propagate.
enum ClearTarget {
    Specific { schema: String, query: String },
    All,
}

/// Validates and converts the raw named parameters into a [`ClearTarget`].
///
/// Returns an error if exactly one of `schema` / `query` is provided (the
/// user must supply both or neither).
fn parse_clear_target(
    schema: Option<String>,
    query: Option<String>,
) -> Result<ClearTarget, Box<dyn Error>> {
    match (schema, query) {
        (Some(s), Some(q)) => Ok(ClearTarget::Specific {
            schema: s,
            query: q,
        }),
        (None, None) => Ok(ClearTarget::All),
        _ => Err("Both schema and query must be provided, or neither (to clear all).".into()),
    }
}

/// Bind data for `labkey_cache_clear`. Stores the validated clear target.
pub(crate) struct CacheClearBindData {
    target: ClearTarget,
}

/// Init data for `labkey_cache_clear`. Holds the status message produced by
/// the cache clear operation and a flag to ensure it is emitted only once.
pub(crate) struct CacheClearInitData {
    status: String,
    done: AtomicBool,
}

/// Formats a byte count as a human-readable size string (e.g. "2.3 MB").
///
/// The exact output format is for human display only and is not a stable API.
/// Do not rely on parsing it programmatically.
///
/// Precision loss from `u64 → f64` is acceptable here — the result is a
/// display-only string rounded to one decimal place.
#[allow(clippy::cast_precision_loss)]
fn format_size(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    let b = bytes as f64;
    if b >= GB {
        format!("{:.1} GB", b / GB)
    } else if b >= MB {
        format!("{:.1} MB", b / MB)
    } else if b >= KB {
        format!("{:.1} KB", b / KB)
    } else {
        format!("{bytes} B")
    }
}

pub(crate) struct LabkeyCacheClearVTab;

impl VTab for LabkeyCacheClearVTab {
    type InitData = CacheClearInitData;
    type BindData = CacheClearBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let schema = bind.get_named_parameter("schema").map(|v| v.to_string());
        let query = bind.get_named_parameter("query").map(|v| v.to_string());

        let target = parse_clear_target(schema, query)?;

        Ok(CacheClearBindData { target })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<CacheClearBindData>() };
        let mgr = cache::CacheManager::new()?;

        let status = match &bind_data.target {
            ClearTarget::Specific { schema, query } => clear_specific(&mgr, schema, query)?,
            ClearTarget::All => clear_all(&mgr)?,
        };

        Ok(CacheClearInitData {
            status,
            done: AtomicBool::new(false),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();

        if init_data
            .done
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            output.set_len(0);
        } else {
            output.flat_vector(0).insert(0, &init_data.status);
            output.set_len(1);
        }

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            (
                "schema".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "query".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
        ])
    }
}

/// Clears cache entries matching a specific schema + query name.
///
/// Iterates `list_entries()` and removes every entry whose `schema_name` and
/// `query_name` fields match. Multiple entries can match (e.g. the same
/// schema.query cached from different servers), so we remove all of them.
fn clear_specific(
    mgr: &cache::CacheManager,
    schema: &str,
    query: &str,
) -> Result<String, Box<dyn Error>> {
    let entries = mgr.list_entries();
    let matching_keys: Vec<String> = entries
        .into_iter()
        .filter(|(_key, entry)| entry.schema_name == schema && entry.query_name == query)
        .map(|(key, _entry)| key)
        .collect();

    if matching_keys.is_empty() {
        return Ok(format!(
            "No cached data found for {schema}.{query}. Nothing to clear."
        ));
    }

    let mut total_rows: i64 = 0;
    let mut total_bytes: u64 = 0;

    for key in &matching_keys {
        if let Some(removed) = mgr.remove_entry(key)? {
            total_rows += removed.row_count;
            total_bytes += removed.size_bytes;
        }
    }

    Ok(format!(
        "Cleared cache for {schema}.{query} ({total_rows} rows, {})",
        format_size(total_bytes)
    ))
}

/// Clears all cached entries.
fn clear_all(mgr: &cache::CacheManager) -> Result<String, Box<dyn Error>> {
    let cleared = mgr.clear_all()?;

    if cleared.is_empty() {
        return Ok("No cached tables to clear.".to_owned());
    }

    let count = cleared.len();
    let total_bytes: u64 = cleared.iter().map(|e| e.size_bytes).sum();

    Ok(format!(
        "Cleared {count} cached table{} ({} total)",
        if count == 1 { "" } else { "s" },
        format_size(total_bytes)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{CacheColumn, CacheEntry, CacheManager};

    /// Creates a `CacheManager` backed by a temporary directory.
    fn test_manager() -> (CacheManager, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = CacheManager::with_dir(dir.path().to_path_buf()).expect("CacheManager::with_dir");
        (mgr, dir)
    }

    /// Creates a sample `CacheEntry` for testing.
    fn sample_entry(schema: &str, query: &str) -> CacheEntry {
        CacheEntry {
            parquet_path: format!("host/{schema}/{query}.parquet"),
            fetched_at: "2026-03-10T12:00:00Z".into(),
            server_modified: Some("2026-03-10T11:00:00Z".into()),
            row_count: 100,
            size_bytes: 4096,
            base_url: "https://labkey.example.com".into(),
            container_path: "/Project".into(),
            schema_name: schema.into(),
            query_name: query.into(),
            columns: vec![CacheColumn {
                name: "col1".into(),
                json_type: Some("string".into()),
            }],
        }
    }

    // -- format_size tests --

    #[test]
    fn format_size_bytes() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(1023), "1023 B");
    }

    #[test]
    fn format_size_kilobytes() {
        assert_eq!(format_size(1024), "1.0 KB");
        assert_eq!(format_size(1536), "1.5 KB");
    }

    #[test]
    fn format_size_megabytes() {
        assert_eq!(format_size(1_048_576), "1.0 MB");
        assert_eq!(format_size(2_411_724), "2.3 MB");
    }

    #[test]
    fn format_size_gigabytes() {
        assert_eq!(format_size(1_073_741_824), "1.0 GB");
    }

    // -- parse_clear_target tests --

    #[test]
    fn parse_clear_target_both_provided() {
        let target =
            parse_clear_target(Some("lists".into()), Some("People".into())).expect("should be Ok");
        assert!(
            matches!(target, ClearTarget::Specific { schema, query } if schema == "lists" && query == "People")
        );
    }

    #[test]
    fn parse_clear_target_neither_provided() {
        let target = parse_clear_target(None, None).expect("should be Ok");
        assert!(matches!(target, ClearTarget::All));
    }

    #[test]
    fn parse_clear_target_schema_only_is_error() {
        let result = parse_clear_target(Some("lists".into()), None);
        assert!(result.is_err());
    }

    #[test]
    fn parse_clear_target_query_only_is_error() {
        let result = parse_clear_target(None, Some("People".into()));
        assert!(result.is_err());
    }

    // -- clear_specific tests --

    #[test]
    fn clear_specific_removes_matching_entry() {
        let (mgr, _dir) = test_manager();
        let entry = sample_entry("lists", "People");
        mgr.put_entry("key1", &entry).expect("put");

        let status = clear_specific(&mgr, "lists", "People").expect("clear_specific");

        assert!(
            status.contains("lists.People"),
            "status should mention the table: {status}"
        );
        assert!(
            status.contains("100 rows"),
            "status should mention row count: {status}"
        );
        assert!(mgr.list_entries().is_empty(), "cache should be empty");
    }

    #[test]
    fn clear_specific_no_match_returns_informative_message() {
        let (mgr, _dir) = test_manager();
        let status = clear_specific(&mgr, "lists", "Nonexistent").expect("clear_specific");
        assert!(
            status.contains("No cached data found"),
            "should indicate nothing found: {status}"
        );
        assert!(
            status.contains("lists.Nonexistent"),
            "should mention the table name: {status}"
        );
    }

    #[test]
    fn clear_specific_only_removes_matching_not_others() {
        let (mgr, _dir) = test_manager();
        let entry1 = sample_entry("lists", "People");
        let entry2 = sample_entry("lists", "Animals");
        mgr.put_entry("k1", &entry1).expect("put");
        mgr.put_entry("k2", &entry2).expect("put");

        clear_specific(&mgr, "lists", "People").expect("clear_specific");

        let remaining = mgr.list_entries();
        assert_eq!(remaining.len(), 1, "only one entry should remain");
        assert_eq!(remaining[0].1.query_name, "Animals");
    }

    #[test]
    fn clear_specific_removes_multiple_matching_entries() {
        let (mgr, _dir) = test_manager();
        // Same schema.query but different servers (different cache keys).
        let mut entry1 = sample_entry("lists", "People");
        entry1.base_url = "https://server1.example.com".into();
        let mut entry2 = sample_entry("lists", "People");
        entry2.base_url = "https://server2.example.com".into();

        mgr.put_entry("server1_key", &entry1).expect("put");
        mgr.put_entry("server2_key", &entry2).expect("put");

        let status = clear_specific(&mgr, "lists", "People").expect("clear_specific");

        assert!(
            status.contains("200 rows"),
            "should sum rows from both entries: {status}"
        );
        assert!(mgr.list_entries().is_empty(), "all matches removed");
    }

    // -- clear_all tests --

    #[test]
    fn clear_all_empty_cache() {
        let (mgr, _dir) = test_manager();
        let status = clear_all(&mgr).expect("clear_all");
        assert!(
            status.contains("No cached tables"),
            "should indicate empty cache: {status}"
        );
    }

    #[test]
    fn clear_all_single_entry() {
        let (mgr, _dir) = test_manager();
        let entry = sample_entry("lists", "People");
        mgr.put_entry("k1", &entry).expect("put");

        let status = clear_all(&mgr).expect("clear_all");

        assert!(
            status.contains("1 cached table"),
            "should say '1 cached table' (singular): {status}"
        );
        assert!(mgr.list_entries().is_empty());
    }

    #[test]
    fn clear_all_multiple_entries() {
        let (mgr, _dir) = test_manager();
        mgr.put_entry("k1", &sample_entry("lists", "A"))
            .expect("put");
        mgr.put_entry("k2", &sample_entry("core", "B"))
            .expect("put");
        mgr.put_entry("k3", &sample_entry("study", "C"))
            .expect("put");

        let status = clear_all(&mgr).expect("clear_all");

        assert!(
            status.contains("3 cached tables"),
            "should mention count: {status}"
        );
        assert!(mgr.list_entries().is_empty());
    }

    #[test]
    fn clear_all_reports_total_size() {
        let (mgr, _dir) = test_manager();
        let mut entry1 = sample_entry("lists", "A");
        entry1.size_bytes = 1_048_576; // 1 MB
        let mut entry2 = sample_entry("lists", "B");
        entry2.size_bytes = 1_048_576; // 1 MB

        mgr.put_entry("k1", &entry1).expect("put");
        mgr.put_entry("k2", &entry2).expect("put");

        let status = clear_all(&mgr).expect("clear_all");

        assert!(
            status.contains("2.0 MB"),
            "should report total size: {status}"
        );
    }
}
