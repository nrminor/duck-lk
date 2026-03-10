// The `duckdb_entrypoint_c_api` proc macro generates hidden top-level FFI glue
// functions that contain `.unwrap()` calls and signatures we cannot control.
// These allows must be crate-level (`#![allow]`) because the generated functions
// are at module scope, not inside `extension_entrypoint` — a function-level
// `#[allow]` would not cover them. Production code avoids `.unwrap()` entirely;
// only the proc-macro output and test assertions (`.unwrap_err()`) rely on this.
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

use duckdb::{duckdb_entrypoint_c_api, Connection};
use std::error::Error;

/// Register the `duck_lk` extension's table functions with `DuckDB`.
///
/// # Safety
///
/// Called by `DuckDB`'s extension loader via the C API. The `Connection` handle
/// must be valid for the lifetime of the call.
///
/// # Errors
///
/// Returns an error if table function registration fails.
#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<vtab_query::LabkeyQueryVTab>("labkey_query")?;
    con.register_table_function::<vtab_cache_clear::LabkeyCacheClearVTab>("labkey_cache_clear")?;
    con.register_table_function::<vtab_cache_info::LabkeyCacheInfoVTab>("labkey_cache_info")?;
    Ok(())
}
