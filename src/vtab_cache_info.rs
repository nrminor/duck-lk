//! `labkey_cache_info` table function — read-only cache inspection.
//!
//! Returns one row per cached entry with metadata about the cached data:
//! server URL, container, schema/query names, row count, file size, timestamps,
//! staleness check availability, and the absolute Parquet path on disk.
//!
//! This `VTab` performs no network I/O and requires no credentials — it reads
//! only from the local `cache.json` index via [`CacheManager`].

use std::{
    error::Error,
    sync::atomic::{AtomicUsize, Ordering},
};

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};

use super::cache;

/// Number of result columns in the `labkey_cache_info` schema.
const COLUMN_COUNT: usize = 10;

const STALENESS_AVAILABLE: &str = "available";
const STALENESS_UNAVAILABLE: &str = "unavailable (no Modified column)";

/// One row of cache info output, with typed fields matching the `DuckDB`
/// column schema declared in `bind()`.
pub(crate) struct CacheInfoRow {
    pub(crate) base_url: String,
    pub(crate) container_path: String,
    pub(crate) schema_name: String,
    pub(crate) query_name: String,
    pub(crate) row_count: i64,
    pub(crate) size_bytes: i64,
    pub(crate) fetched_at: String,
    pub(crate) server_modified: String,
    pub(crate) staleness_check: String,
    pub(crate) parquet_path: String,
}

/// Derives a [`CacheInfoRow`] from a [`CacheEntry`] and a [`CacheManager`].
///
/// Used by `init()` and directly testable without a `DuckDB` runtime.
pub(crate) fn cache_entry_to_row(
    mgr: &cache::CacheManager,
    entry: &cache::CacheEntry,
) -> CacheInfoRow {
    let staleness_check = if entry.server_modified.is_some() {
        STALENESS_AVAILABLE.to_owned()
    } else {
        STALENESS_UNAVAILABLE.to_owned()
    };
    let parquet_path = mgr.parquet_path(entry).to_string_lossy().into_owned();
    let server_modified = entry.server_modified.clone().unwrap_or_default();

    CacheInfoRow {
        base_url: entry.base_url.clone(),
        container_path: entry.container_path.clone(),
        schema_name: entry.schema_name.clone(),
        query_name: entry.query_name.clone(),
        row_count: entry.row_count,
        size_bytes: i64::try_from(entry.size_bytes).unwrap_or(i64::MAX),
        fetched_at: entry.fetched_at.clone(),
        server_modified,
        staleness_check,
        parquet_path,
    }
}

/// Bind data for `labkey_cache_info`. The column schema is fixed so no
/// runtime state is needed.
pub(crate) struct CacheInfoBindData;

/// Init data for `labkey_cache_info`. Holds the materialized rows read
/// from `cache.json` and an atomic offset for chunked output.
pub(crate) struct CacheInfoInitData {
    rows: Vec<CacheInfoRow>,
    /// Current row offset for chunked emission. Uses `AtomicUsize` because
    /// the `VTab` trait requires `Send + Sync`. Safe with `Relaxed` because
    /// `set_max_threads(1)` is not called here (`cache_info` is cheap enough
    /// that `DuckDB` manages thread safety itself).
    row_offset: AtomicUsize,
}

pub(crate) struct LabkeyCacheInfoVTab;

impl VTab for LabkeyCacheInfoVTab {
    type InitData = CacheInfoInitData;
    type BindData = CacheInfoBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("base_url", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        bind.add_result_column(
            "container_path",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "schema_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "query_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column("row_count", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        bind.add_result_column("size_bytes", LogicalTypeHandle::from(LogicalTypeId::Bigint));
        bind.add_result_column(
            "fetched_at",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "server_modified",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "staleness_check",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "parquet_path",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );

        Ok(CacheInfoBindData)
    }

    fn init(_init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let mgr = cache::CacheManager::new()?;
        let entries = mgr.list_entries();

        let rows: Vec<CacheInfoRow> = entries
            .into_iter()
            .map(|(_key, entry)| cache_entry_to_row(&mgr, &entry))
            .collect();

        Ok(CacheInfoInitData {
            rows,
            row_offset: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();

        let offset = init_data.row_offset.load(Ordering::Relaxed);
        let total_rows = init_data.rows.len();
        let remaining = total_rows.saturating_sub(offset);

        if remaining == 0 {
            output.set_len(0);
            return Ok(());
        }

        let chunk_size = std::cmp::min(2048, remaining);

        // VARCHAR columns — use Inserter::insert() inline.
        for i in 0..chunk_size {
            let row = &init_data.rows[offset + i];
            output.flat_vector(0).insert(i, &row.base_url);
            output.flat_vector(1).insert(i, &row.container_path);
            output.flat_vector(2).insert(i, &row.schema_name);
            output.flat_vector(3).insert(i, &row.query_name);
            output.flat_vector(6).insert(i, &row.fetched_at);
            output.flat_vector(7).insert(i, &row.server_modified);
            output.flat_vector(8).insert(i, &row.staleness_check);
            output.flat_vector(9).insert(i, &row.parquet_path);
        }

        // BIGINT columns — typed slice write via let-binding for lifetime.
        {
            let mut vec4 = output.flat_vector(4);
            let slice = vec4.as_mut_slice::<i64>();
            for (i, slot) in slice.iter_mut().enumerate().take(chunk_size) {
                *slot = init_data.rows[offset + i].row_count;
            }
        }
        {
            let mut vec5 = output.flat_vector(5);
            let slice = vec5.as_mut_slice::<i64>();
            for (i, slot) in slice.iter_mut().enumerate().take(chunk_size) {
                *slot = init_data.rows[offset + i].size_bytes;
            }
        }

        init_data
            .row_offset
            .store(offset + chunk_size, Ordering::Relaxed);
        output.set_len(chunk_size);

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        None
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{CacheColumn, CacheEntry, CacheManager};

    /// Creates a `CacheManager` backed by a temporary directory and a sample
    /// entry for testing.
    fn test_manager_with_entry() -> (CacheManager, tempfile::TempDir, CacheEntry) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = CacheManager::with_dir(dir.path().to_path_buf()).expect("CacheManager::with_dir");
        let entry = CacheEntry {
            parquet_path: "labkey.example.com/myproject/lists/People.parquet".into(),
            fetched_at: "2026-03-09T10:30:00Z".into(),
            server_modified: Some("2026-03-09T09:15:00Z".into()),
            row_count: 42,
            size_bytes: 1024,
            base_url: "https://labkey.example.com/labkey".into(),
            container_path: "/MyProject".into(),
            schema_name: "lists".into(),
            query_name: "People".into(),
            columns: vec![
                CacheColumn {
                    name: "Name".into(),
                    json_type: Some("string".into()),
                },
                CacheColumn {
                    name: "Age".into(),
                    json_type: Some("int".into()),
                },
            ],
        };
        (mgr, dir, entry)
    }

    #[test]
    fn row_contains_all_entry_fields_in_column_order() {
        let (mgr, _dir, entry) = test_manager_with_entry();
        let row = cache_entry_to_row(&mgr, &entry);
        assert_eq!(row.base_url, "https://labkey.example.com/labkey");
        assert_eq!(row.container_path, "/MyProject");
        assert_eq!(row.schema_name, "lists");
        assert_eq!(row.query_name, "People");
        assert_eq!(row.row_count, 42);
        assert_eq!(row.size_bytes, 1024);
        assert_eq!(row.fetched_at, "2026-03-09T10:30:00Z");
        assert_eq!(row.server_modified, "2026-03-09T09:15:00Z");
        assert_eq!(row.staleness_check, STALENESS_AVAILABLE);
    }

    #[test]
    fn parquet_path_is_absolute() {
        let (mgr, _dir, entry) = test_manager_with_entry();
        let row = cache_entry_to_row(&mgr, &entry);
        assert!(
            row.parquet_path
                .ends_with("labkey.example.com/myproject/lists/People.parquet"),
            "parquet_path should end with relative path: {}",
            row.parquet_path
        );
        assert!(
            std::path::Path::new(&row.parquet_path).is_absolute(),
            "parquet_path should be absolute: {}",
            row.parquet_path
        );
    }

    #[test]
    fn server_modified_none_produces_empty_string() {
        let (mgr, _dir, mut entry) = test_manager_with_entry();
        entry.server_modified = None;
        let row = cache_entry_to_row(&mgr, &entry);
        assert_eq!(row.server_modified, "");
    }

    #[test]
    fn staleness_check_unavailable_when_no_modified() {
        let (mgr, _dir, mut entry) = test_manager_with_entry();
        entry.server_modified = None;
        let row = cache_entry_to_row(&mgr, &entry);
        assert_eq!(row.staleness_check, STALENESS_UNAVAILABLE);
    }

    #[test]
    fn row_count_zero() {
        let (mgr, _dir, mut entry) = test_manager_with_entry();
        entry.row_count = 0;
        let row = cache_entry_to_row(&mgr, &entry);
        assert_eq!(row.row_count, 0);
    }

    #[test]
    fn row_count_negative() {
        let (mgr, _dir, mut entry) = test_manager_with_entry();
        entry.row_count = -1;
        let row = cache_entry_to_row(&mgr, &entry);
        assert_eq!(row.row_count, -1);
    }

    #[test]
    fn size_bytes_zero() {
        let (mgr, _dir, mut entry) = test_manager_with_entry();
        entry.size_bytes = 0;
        let row = cache_entry_to_row(&mgr, &entry);
        assert_eq!(row.size_bytes, 0);
    }

    #[test]
    fn size_bytes_exceeding_i64_max_saturates() {
        let (mgr, _dir, mut entry) = test_manager_with_entry();
        entry.size_bytes = u64::MAX;
        let row = cache_entry_to_row(&mgr, &entry);
        assert_eq!(
            row.size_bytes,
            i64::MAX,
            "u64 values beyond i64::MAX should saturate to i64::MAX"
        );
    }

    #[test]
    fn size_bytes_at_i64_max_boundary() {
        let (mgr, _dir, mut entry) = test_manager_with_entry();
        entry.size_bytes = i64::MAX as u64;
        let row = cache_entry_to_row(&mgr, &entry);
        assert_eq!(row.size_bytes, i64::MAX);
    }

    #[test]
    fn empty_string_fields_are_valid() {
        let (mgr, _dir, mut entry) = test_manager_with_entry();
        entry.base_url = String::new();
        entry.container_path = String::new();
        entry.schema_name = String::new();
        entry.query_name = String::new();
        entry.fetched_at = String::new();
        let row = cache_entry_to_row(&mgr, &entry);
        assert_eq!(row.base_url, "");
        assert_eq!(row.container_path, "");
        assert_eq!(row.schema_name, "");
        assert_eq!(row.query_name, "");
        assert_eq!(row.fetched_at, "");
    }
}
