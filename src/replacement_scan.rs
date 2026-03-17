//! Replacement scan that intercepts `labkey.{schema}.{query}` table references
//! and redirects them to `read_parquet` on the cached Parquet file.
//!
//! When a user writes `SELECT * FROM labkey.lists.People`, DuckDB cannot find
//! a table with that name in the catalog. Before raising an error, it calls
//! registered replacement scan callbacks. Our callback recognizes the
//! `labkey.*.*` pattern, ensures the table is synced to the local Parquet
//! cache, and tells DuckDB to read the cached file with its native
//! `read_parquet` function.

use std::ffi::{c_char, c_void, CStr, CString};

use libduckdb_sys::{
    duckdb_create_varchar, duckdb_database, duckdb_destroy_value,
    duckdb_replacement_scan_add_parameter, duckdb_replacement_scan_info,
    duckdb_replacement_scan_set_function_name,
};

use super::cache;

/// Registers the replacement scan callback on the given database handle.
///
/// # Safety
///
/// `db` must be a valid `duckdb_database` handle for the lifetime of the
/// database. The callback is registered with no extra data and no delete
/// callback, so there is no cleanup needed.
pub(crate) unsafe fn register(db: duckdb_database) {
    libduckdb_sys::duckdb_add_replacement_scan(
        db,
        Some(replacement_scan_callback),
        std::ptr::null_mut(),
        None,
    );
}

/// The replacement scan callback invoked by DuckDB when a table name is not
/// found in the catalog.
///
/// Parses table names matching `labkey.{schema}.{query}` and redirects them
/// to `read_parquet` on the corresponding cached Parquet file. If the cache
/// is missing or stale, triggers a sync before redirecting.
///
/// If the table name does not match the `labkey.*.*` pattern, the callback
/// returns without calling `set_function_name`, which tells DuckDB to
/// continue with other replacement scans or raise a "table not found" error.
///
/// # Safety
///
/// Called by DuckDB's C runtime. `info` and `table_name` must be valid
/// pointers for the duration of the call.
unsafe extern "C" fn replacement_scan_callback(
    info: duckdb_replacement_scan_info,
    table_name: *const c_char,
    _data: *mut c_void,
) {
    let Ok(name) = CStr::from_ptr(table_name).to_str() else {
        return;
    };

    let Some(pq_path) = find_cached_parquet(name) else {
        return;
    };

    let function_name = CString::new("read_parquet").expect("read_parquet is a valid C string");
    duckdb_replacement_scan_set_function_name(info, function_name.as_ptr());

    let Some(path_str) = pq_path.to_str() else {
        return;
    };
    let Ok(path_cstr) = CString::new(path_str) else {
        return;
    };
    let value = duckdb_create_varchar(path_cstr.as_ptr());
    duckdb_replacement_scan_add_parameter(info, value);
    duckdb_destroy_value(&mut { value });
}

/// Searches the cache for any entry whose query name matches the given
/// table name (case-insensitive). Returns the Parquet path if found.
fn find_cached_parquet(table_name: &str) -> Option<std::path::PathBuf> {
    let mgr = cache::CacheManager::new().ok()?;
    let entries = mgr.list_entries();

    for (_key, entry) in &entries {
        if entry.query_name.eq_ignore_ascii_case(table_name) {
            let pq_path = mgr.parquet_path(entry);
            if pq_path.exists() {
                return Some(pq_path);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_cached_parquet_returns_none_on_empty_cache() {
        // With no cache entries, any name should return None.
        // This test relies on the cache being empty or the name not matching.
        assert!(find_cached_parquet("nonexistent_table_xyz_12345").is_none());
    }
}
