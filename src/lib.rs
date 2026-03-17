// Custom extension entry point that captures the duckdb_database handle for
// replacement scan registration. This replaces the duckdb_entrypoint_c_api
// proc macro so we have access to the raw database handle before it's wrapped
// in a Connection.
//
// The allow block covers generated-style code in the entry point and test
// assertions that use .unwrap().
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::missing_safety_doc,
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    clippy::unwrap_used
)]

// NOTE: These modules are intentionally `mod` (not `pub mod`). All `pub(crate)`
// items are crate-internal. Do not make these `pub mod` without a semver review
// — it would expose labkey_rs, arrow, and duckdb types as part of our API.
//
// The `#[path]` is required because wasm_lib.rs re-includes lib.rs via
// `mod lib;`, which changes the module root to src/lib/ instead of src/.
#[path = "types.rs"]
mod types;

#[path = "credential.rs"]
mod credential;

#[path = "cache.rs"]
mod cache;

#[path = "vtab_query.rs"]
mod vtab_query;

#[path = "vtab_cache_info.rs"]
mod vtab_cache_info;

#[path = "vtab_cache_clear.rs"]
mod vtab_cache_clear;

#[path = "replacement_scan.rs"]
#[allow(clippy::doc_markdown)]
mod replacement_scan;

#[path = "vtab_sync.rs"]
#[allow(clippy::doc_markdown)]
mod vtab_sync;

#[path = "scalar_helpers.rs"]
#[allow(clippy::doc_markdown)]
mod scalar_helpers;

use duckdb::Connection;
use std::error::Error;

/// Register the extension's table functions and replacement scan.
///
/// # Errors
///
/// Returns an error if table function registration fails.
fn register_extension(
    con: Connection,
    db: libduckdb_sys::duckdb_database,
) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<vtab_query::LabkeyQueryVTab>("labkey_query")?;
    con.register_table_function::<vtab_sync::LabkeySyncVTab>("labkey_sync")?;
    con.register_table_function::<vtab_cache_clear::LabkeyCacheClearVTab>("labkey_cache_clear")?;
    con.register_table_function::<vtab_cache_info::LabkeyCacheInfoVTab>("labkey_cache_info")?;
    con.register_scalar_function::<scalar_helpers::LabkeyParquetPath>("labkey_parquet_path")?;
    con.register_scalar_function::<scalar_helpers::LabkeyIsStale>("labkey_is_stale")?;

    // Safety: db is a valid database handle obtained from the extension access
    // struct during initialization. The replacement scan callback uses only
    // the libduckdb_sys FFI functions and does not store the handle.
    unsafe {
        replacement_scan::register(db);
    }

    Ok(())
}

// --- Custom entry point (replaces duckdb_entrypoint_c_api macro) ---
//
// This is modeled directly on the code the proc macro generates, with one
// addition: we capture the duckdb_database handle and pass it to
// register_extension so we can register the replacement scan.

const MINIMUM_DUCKDB_VERSION: &str = "v1.2.0";

/// # Safety
///
/// Internal entry point for error handling. Called by the extern "C" entry
/// point below.
unsafe fn duck_lk_init_c_api_internal(
    info: libduckdb_sys::duckdb_extension_info,
    access: *const libduckdb_sys::duckdb_extension_access,
) -> Result<bool, Box<dyn Error>> {
    let have_api_struct =
        libduckdb_sys::duckdb_rs_extension_api_init(info, access, MINIMUM_DUCKDB_VERSION)
            .unwrap();

    if !have_api_struct {
        return Ok(false);
    }

    let db: libduckdb_sys::duckdb_database = *(*access).get_database.unwrap()(info);
    let connection = Connection::open_from_raw(db.cast())?;

    register_extension(connection, db)?;

    Ok(true)
}

/// # Safety
///
#[allow(clippy::doc_markdown)]
/// Entry point called by DuckDB's extension loader via the C API.
#[no_mangle]
pub unsafe extern "C" fn duck_lk_init_c_api(
    info: libduckdb_sys::duckdb_extension_info,
    access: *const libduckdb_sys::duckdb_extension_access,
) -> bool {
    let init_result = duck_lk_init_c_api_internal(info, access);

    if let Err(x) = init_result {
        let error_c_string = std::ffi::CString::new(x.to_string());

        match error_c_string {
            Ok(e) => {
                (*access).set_error.unwrap()(info, e.as_ptr());
            }
            Err(_e) => {
                let error_alloc_failure = c"An error occurred but the extension failed to allocate memory for an error string";
                (*access).set_error.unwrap()(info, error_alloc_failure.as_ptr());
            }
        }
        return false;
    }

    init_result.unwrap_or(false)
}
