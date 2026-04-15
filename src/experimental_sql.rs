//! Bulk-ingest path for large-table synchronization.
//!
//! Uses LabKey's `sql-execute.view` endpoint through the feature-gated
//! `labkey-rs` experimental API. This path asks LabKey for only the raw columns
//! we want and avoids the grid-oriented metadata and formatting overhead of
//! `selectRows`.

use std::{error::Error, path::Path, time::Instant};

use indicatif::{ProgressBar, ProgressStyle};
use labkey_rs::{
    query::experimental::{ExperimentalQueryExt, SqlExecuteOptions},
    ClientConfig, LabkeyClient,
};
use tokio::runtime::Runtime;

use super::{cache, credential, types, vtab_query};

const SQL_BATCH_SIZE: usize = 50_000;
const SQL_PROGRESS_TICK_MS: u64 = 100;

/// Result of a bulk export.
pub(crate) struct ExperimentalSqlResult {
    pub(crate) row_count: usize,
    pub(crate) size_bytes: u64,
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn build_select_sql(schema_name: &str, query_name: &str, column_names: &[String]) -> String {
    let projection = column_names
        .iter()
        .map(|name| quote_identifier(name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {projection} FROM {}.{}",
        quote_identifier(schema_name),
        quote_identifier(query_name)
    )
}

pub(crate) fn fetch_experimental_sql_to_parquet(
    config: &credential::ResolvedConfig,
    schema_name: &str,
    query_name: &str,
    column_names: &[String],
    column_json_types: &[String],
    pq_path: &Path,
) -> Result<ExperimentalSqlResult, Box<dyn Error>> {
    vtab_query::debug_log(format!(
        "fetch start schema={schema_name} query={query_name} columns={} batch_size={SQL_BATCH_SIZE}",
        column_names.len(),
    ));

    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner()),
    );
    spinner.set_message(format!(
        "Fetching {schema_name}.{query_name} from LabKey..."
    ));
    spinner.enable_steady_tick(std::time::Duration::from_millis(SQL_PROGRESS_TICK_MS));

    let sql = build_select_sql(schema_name, query_name, column_names);
    let started = Instant::now();
    let rt = Runtime::new()?;
    let client_config = ClientConfig::new(
        config.base_url.clone(),
        config.credential.clone(),
        config.container_path.clone(),
    );
    let client = LabkeyClient::new(client_config)?;
    let response = rt.block_on(async {
        client
            .experimental_sql_execute(
                SqlExecuteOptions::builder()
                    .schema(schema_name.to_owned())
                    .sql(sql)
                    .build(),
            )
            .await
    })?;

    vtab_query::debug_log(format!(
        "fetch response schema={schema_name} query={query_name} rows={} columns={}",
        response.row_count(),
        response.column_count(),
    ));

    spinner.set_message(format!(
        "Writing rows for {schema_name}.{query_name}... 0/{} rows",
        response.row_count(),
    ));

    let mut writer = cache::IncrementalParquetWriter::try_new(
        pq_path,
        types::experimental_sql_schema(column_names, column_json_types),
    )?;

    let mut written_rows = 0;
    while written_rows < response.rows.len() {
        let end = (written_rows + SQL_BATCH_SIZE).min(response.rows.len());
        let chunk_rows = end - written_rows;
        let batch = types::sql_rows_to_record_batch(
            &response.rows[written_rows..end],
            column_names,
            column_json_types,
        )?;
        writer.write(&batch)?;
        written_rows = end;
        vtab_query::debug_log(format!(
            "rows written schema={schema_name} query={query_name} rows={chunk_rows} written_total={written_rows}",
        ));
        spinner.set_message(format!(
            "Writing rows for {schema_name}.{query_name}... {written_rows}/{} rows ({}, {})",
            response.row_count(),
            vtab_query::format_elapsed(started.elapsed()),
            vtab_query::format_rows_per_second(written_rows, started.elapsed()),
        ));
    }

    let size_bytes = writer.finish()?;
    let elapsed = started.elapsed();
    vtab_query::debug_log(format!(
        "fetch complete schema={schema_name} query={query_name} rows={} size_bytes={} elapsed={} rate={}",
        response.row_count(),
        size_bytes,
        vtab_query::format_elapsed(elapsed),
        vtab_query::format_rows_per_second(response.row_count(), elapsed),
    ));
    spinner.finish_with_message(format!(
        "Fetched {} rows from {schema_name}.{query_name} ({}, {})",
        response.row_count(),
        vtab_query::format_elapsed(elapsed),
        vtab_query::format_rows_per_second(response.row_count(), elapsed),
    ));

    Ok(ExperimentalSqlResult {
        row_count: response.row_count(),
        size_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_select_sql_quotes_identifiers_and_preserves_order() {
        let sql = build_select_sql(
            "lists",
            "People",
            &[
                "RowId".to_string(),
                "Display Name".to_string(),
                "weird\"column".to_string(),
            ],
        );
        assert_eq!(
            sql,
            "SELECT \"RowId\", \"Display Name\", \"weird\"\"column\" FROM \"lists\".\"People\""
        );
    }
}
