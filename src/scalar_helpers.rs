//! Scalar helper functions for duck-lk.
//!
//! These are lightweight functions that expose cache state to DuckDB SQL
//! without going through the VTab machinery.

use std::error::Error;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    types::DuckString,
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
};
use libduckdb_sys::duckdb_string_t;

use super::{cache, credential};

/// Reads a string value from a varchar column at the given row index.
fn read_varchar(input: &mut DataChunkHandle, col: usize, row: usize) -> String {
    let vec = input.flat_vector(col);
    let slice = vec.as_slice_with_len::<duckdb_string_t>(input.len());
    DuckString::new(&mut { slice[row] }).as_str().to_string()
}

// ---------------------------------------------------------------------------
// labkey_parquet_path(schema_name, query_name) -> VARCHAR
// ---------------------------------------------------------------------------

/// Returns the absolute path to the cached Parquet file for a given LabKey
/// table. Returns NULL if the table is not cached.
pub(crate) struct LabkeyParquetPath;

impl VScalar for LabkeyParquetPath {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let count = input.len();
        let mut out_vec = output.flat_vector();

        for i in 0..count {
            let schema_name = read_varchar(input, 0, i);
            let query_name = read_varchar(input, 1, i);

            match resolve_parquet_path(&schema_name, &query_name) {
                Some(path) => {
                    let path_str = path.to_string_lossy();
                    out_vec.insert(i, path_str.as_ref());
                }
                None => {
                    out_vec.set_null(i);
                }
            }
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ],
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        )]
    }
}

// ---------------------------------------------------------------------------
// labkey_is_stale(schema_name, query_name) -> BOOLEAN
// ---------------------------------------------------------------------------

/// Checks whether the cached Parquet file for a given LabKey table is stale
/// relative to the server. Returns true if stale, false if fresh, and NULL
/// if staleness cannot be determined (no cache, no Modified column, or no
/// credentials).
pub(crate) struct LabkeyIsStale;

impl VScalar for LabkeyIsStale {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn Error>> {
        let count = input.len();
        let mut out_vec = output.flat_vector();

        for i in 0..count {
            let schema_name = read_varchar(input, 0, i);
            let query_name = read_varchar(input, 1, i);

            match check_staleness(&schema_name, &query_name) {
                Some(is_stale) => {
                    let slice = out_vec.as_mut_slice::<bool>();
                    slice[i] = is_stale;
                }
                None => {
                    out_vec.set_null(i);
                }
            }
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ],
            LogicalTypeHandle::from(LogicalTypeId::Boolean),
        )]
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn resolve_parquet_path(schema_name: &str, query_name: &str) -> Option<std::path::PathBuf> {
    let config = credential::resolve_config(None, None, None).ok()?;
    let mgr = cache::CacheManager::new().ok()?;
    let key = cache::cache_key(
        &config.base_url,
        &config.container_path,
        schema_name,
        query_name,
    )
    .ok()?;

    let entry = mgr.get_entry(&key)?;
    let pq_path = mgr.parquet_path(&entry);
    if pq_path.exists() {
        Some(pq_path)
    } else {
        None
    }
}

fn check_staleness(schema_name: &str, query_name: &str) -> Option<bool> {
    let config = credential::resolve_config(None, None, None).ok()?;
    let mgr = cache::CacheManager::new().ok()?;
    let key = cache::cache_key(
        &config.base_url,
        &config.container_path,
        schema_name,
        query_name,
    )
    .ok()?;

    let entry = mgr.get_entry(&key)?;
    let cached_modified = entry.server_modified.as_ref()?;

    let rt = tokio::runtime::Runtime::new().ok()?;
    let client_config = labkey_rs::ClientConfig::new(
        config.base_url.clone(),
        config.credential.clone(),
        config.container_path.clone(),
    );
    let client = labkey_rs::LabkeyClient::new(client_config).ok()?;

    let server_modified =
        cache::CacheManager::check_staleness(&client, &rt, schema_name, query_name)?;

    Some(server_modified != *cached_modified)
}
