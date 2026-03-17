//! `labkey_sync` table function — sync a LabKey table to the local cache.
//!
//! Fetches a LabKey table and writes it to the local Parquet cache without
//! returning the data rows. Returns a single status row confirming what was
//! synced. This is the recommended way to populate the cache for large tables
//! before querying via the replacement scan (bare table name).

use std::{error::Error, sync::atomic::AtomicBool};

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
};

use super::{cache, credential, vtab_query};

pub(crate) struct LabkeySyncBindData {
    schema_name: String,
    query_name: String,
    config: credential::ResolvedConfig,
}

pub(crate) struct LabkeySyncInitData {
    status: String,
    done: AtomicBool,
}

pub(crate) struct LabkeySyncVTab;

impl VTab for LabkeySyncVTab {
    type BindData = LabkeySyncBindData;
    type InitData = LabkeySyncInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column("status", LogicalTypeHandle::from(LogicalTypeId::Varchar));

        let schema_name = bind.get_parameter(0).to_string();
        let query_name = bind.get_parameter(1).to_string();

        let base_url = bind.get_named_parameter("base_url").map(|v| v.to_string());
        let container_path = bind
            .get_named_parameter("container_path")
            .map(|v| v.to_string());
        let api_key = bind.get_named_parameter("api_key").map(|v| v.to_string());

        let config = credential::resolve_config(base_url, container_path, api_key)?;

        Ok(LabkeySyncBindData {
            schema_name,
            query_name,
            config,
        })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        let bind_data = unsafe { &*init.get_bind_data::<LabkeySyncBindData>() };

        init.set_max_threads(1);

        let pq_path = vtab_query::sync_to_cache(
            &bind_data.config,
            &bind_data.schema_name,
            &bind_data.query_name,
        )?;

        let mgr = cache::CacheManager::new()?;
        let key = cache::cache_key(
            &bind_data.config.base_url,
            &bind_data.config.container_path,
            &bind_data.schema_name,
            &bind_data.query_name,
        )?;
        let entry = mgr.get_entry(&key);
        let row_count = entry.as_ref().map_or(0, |e| e.row_count);

        let path_str = pq_path.to_string_lossy().to_string();
        let status = format!(
            "Synced {}.{} ({row_count} rows) to {path_str}",
            bind_data.schema_name, bind_data.query_name,
        );

        Ok(LabkeySyncInitData {
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
            return Ok(());
        }

        let vector = output.flat_vector(0);
        vector.insert(0, init_data.status.as_str());
        output.set_len(1);
        Ok(())
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
        ])
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }
}
