//! Cache management for locally-stored `LabKey` query results.
//!
//! Manages a `cache.json` index file alongside Parquet data files in the
//! platform cache directory (`dirs::cache_dir() + "duck-lk"`). All writes use
//! the atomic temp-file + rename pattern so concurrent readers never see
//! partial state.
//!
//! Staleness checking queries `MAX(Modified)` from the `LabKey` server and is
//! intentionally infallible — a failed check never blocks a cached read.

use std::{
    collections::HashMap,
    error::Error,
    path::{Path, PathBuf},
};

use arrow_array::RecordBatch;
use arrow_select::concat::concat_batches;
use labkey_rs::query::ExecuteSqlOptions;
use labkey_rs::LabkeyClient;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use url::Url;

const CURRENT_CACHE_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CacheFile {
    pub(crate) version: u32,
    pub(crate) entries: HashMap<String, CacheEntry>,
}

impl CacheFile {
    fn empty() -> Self {
        Self {
            version: CURRENT_CACHE_VERSION,
            entries: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CacheEntry {
    pub(crate) parquet_path: String,
    pub(crate) fetched_at: String,
    /// `None` if the table has no `Modified` column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) server_modified: Option<String>,
    pub(crate) row_count: i64,
    pub(crate) size_bytes: u64,
    pub(crate) base_url: String,
    pub(crate) container_path: String,
    pub(crate) schema_name: String,
    pub(crate) query_name: String,
    pub(crate) columns: Vec<CacheColumn>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CacheColumn {
    pub(crate) name: String,
    pub(crate) json_type: Option<String>,
}

/// Computes a cache key from connection parameters.
///
/// The hostname is extracted via `Url::parse` and lowercased. The container
/// path is normalized by stripping leading/trailing `/`.
/// Returns `"{hostname}|{container}|{schema}|{query}"`.
pub(crate) fn cache_key(
    base_url: &str,
    container_path: &str,
    schema: &str,
    query: &str,
) -> Result<String, Box<dyn Error>> {
    let parsed = Url::parse(base_url)?;
    let host = parsed
        .host_str()
        .ok_or_else(|| format!("No host in URL: {base_url}"))?
        .to_lowercase();
    let container = container_path.trim_matches('/');
    Ok(format!("{host}|{container}|{schema}|{query}"))
}

pub(crate) struct CacheManager {
    cache_dir: PathBuf,
}

impl CacheManager {
    /// Creates a new `CacheManager`.
    ///
    /// Uses `dirs::cache_dir()` + `"duck-lk"` as the cache root. Creates the
    /// directory (including parents) if it does not exist.
    pub(crate) fn new() -> Result<Self, Box<dyn Error>> {
        let base = dirs::cache_dir()
            .ok_or("Could not determine platform cache directory (dirs::cache_dir)")?;
        let cache_dir = base.join("duck-lk");
        std::fs::create_dir_all(&cache_dir)?;
        Ok(Self { cache_dir })
    }

    /// Creates a `CacheManager` rooted at an arbitrary directory. Intended for
    /// unit tests so they can point at a `tempdir`.
    #[cfg(test)]
    pub(crate) fn with_dir(cache_dir: PathBuf) -> Result<Self, Box<dyn Error>> {
        std::fs::create_dir_all(&cache_dir)?;
        Ok(Self { cache_dir })
    }

    /// Path to `cache.json` within the cache directory.
    fn cache_json_path(&self) -> PathBuf {
        self.cache_dir.join("cache.json")
    }

    /// Reads and deserialises `cache.json`. Returns an empty [`CacheFile`] if
    /// the file is missing or unparseable.
    fn read_cache_file(&self) -> CacheFile {
        let path = self.cache_json_path();
        match std::fs::read_to_string(&path) {
            Ok(contents) => serde_json::from_str(&contents).unwrap_or_else(|_| CacheFile::empty()),
            Err(_) => CacheFile::empty(),
        }
    }

    /// Writes `cache.json` atomically via temp-file + rename.
    fn write_cache_file(&self, file: &CacheFile) -> Result<(), Box<dyn Error>> {
        let path = self.cache_json_path();
        let tmp = path.with_extension("json.tmp");
        let json = serde_json::to_string_pretty(file)?;
        std::fs::write(&tmp, json)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    /// Returns the entry for `key`, if it exists.
    pub(crate) fn get_entry(&self, key: &str) -> Option<CacheEntry> {
        self.read_cache_file().entries.get(key).cloned()
    }

    /// Writes or updates an entry. Uses atomic temp-file + rename.
    pub(crate) fn put_entry(&self, key: &str, entry: &CacheEntry) -> Result<(), Box<dyn Error>> {
        let mut file = self.read_cache_file();
        file.entries.insert(key.to_owned(), entry.clone());
        self.write_cache_file(&file)
    }

    /// Removes a single entry and its Parquet file. Returns the removed entry
    /// (if it existed). Tolerates a missing Parquet file on disk.
    pub(crate) fn remove_entry(&self, key: &str) -> Result<Option<CacheEntry>, Box<dyn Error>> {
        let mut file = self.read_cache_file();
        let removed = file.entries.remove(key);
        if let Some(ref entry) = removed {
            let pq_path = self.parquet_path(entry);
            // Silently ignore missing files — the Parquet may have been
            // manually deleted.
            let _ = std::fs::remove_file(&pq_path);
        }
        self.write_cache_file(&file)?;
        Ok(removed)
    }

    /// Removes **all** entries and their Parquet files. Returns the removed
    /// entries.
    pub(crate) fn clear_all(&self) -> Result<Vec<CacheEntry>, Box<dyn Error>> {
        let file = self.read_cache_file();
        let entries: Vec<CacheEntry> = file.entries.values().cloned().collect();
        for entry in &entries {
            let pq_path = self.parquet_path(entry);
            let _ = std::fs::remove_file(&pq_path);
        }
        self.write_cache_file(&CacheFile::empty())?;
        Ok(entries)
    }

    /// Returns all cache entries as `(key, entry)` pairs. Returns an empty
    /// `Vec` if `cache.json` does not exist or is unparseable.
    pub(crate) fn list_entries(&self) -> Vec<(String, CacheEntry)> {
        self.read_cache_file().entries.into_iter().collect()
    }

    /// Returns the absolute path for a Parquet file given a cache entry.
    pub(crate) fn parquet_path(&self, entry: &CacheEntry) -> PathBuf {
        self.cache_dir.join(&entry.parquet_path)
    }

    /// Writes a `RecordBatch` to a Parquet file at `path`. Uses atomic
    /// temp-file + rename. Returns the file size in bytes.
    ///
    /// The caller is responsible for constructing the full path (e.g. via
    /// [`parquet_path`](Self::parquet_path)).
    pub(crate) fn write_parquet(path: &Path, batch: &RecordBatch) -> Result<u64, Box<dyn Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp_path = path.with_extension("parquet.tmp");
        let file = std::fs::File::create(&tmp_path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;
        let size = std::fs::metadata(&tmp_path)?.len();
        std::fs::rename(&tmp_path, path)?;
        Ok(size)
    }

    /// Reads a Parquet file into a single `RecordBatch` (concatenating row
    /// groups). Returns an empty batch with the correct schema when the file
    /// contains zero rows.
    pub(crate) fn read_parquet(path: &Path) -> Result<RecordBatch, Box<dyn Error>> {
        let file = std::fs::File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let arrow_schema = builder.schema().clone();
        let reader = builder.build()?;
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(arrow_schema));
        }
        let batch = concat_batches(&arrow_schema, &batches)?;
        Ok(batch)
    }

    /// Checks whether the `LabKey` table has been modified since the cached
    /// version by running `SELECT MAX(Modified) FROM {schema}.{query}`.
    ///
    /// Returns `Some(timestamp_string)` on success. Returns `None` on any
    /// failure (network, permission, missing column, etc.). Never returns
    /// `Err` — all failures are swallowed so that a failed staleness check
    /// does not block queries when cached data exists.
    pub(crate) fn check_staleness(
        client: &LabkeyClient,
        runtime: &tokio::runtime::Runtime,
        schema: &str,
        query: &str,
    ) -> Option<String> {
        let sql = format!("SELECT MAX(Modified) FROM {schema}.\"{query}\"");
        let opts = ExecuteSqlOptions::builder()
            .schema_name(schema.to_owned())
            .sql(sql)
            .build();

        let response = runtime.block_on(client.execute_sql(opts)).ok()?;
        let row = response.rows.first()?;
        // The response has a single column whose name we don't know ahead of
        // time — just grab the first value from the row's data map.
        let cell = row.data.values().next()?;
        let val = &cell.value;
        if val.is_null() {
            return None;
        }
        Some(val.as_str().map_or_else(|| val.to_string(), str::to_owned))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Creates a `CacheManager` backed by a temporary directory.
    fn test_manager() -> (CacheManager, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mgr = CacheManager::with_dir(dir.path().to_path_buf()).expect("CacheManager::with_dir");
        (mgr, dir)
    }

    /// Creates a sample `CacheEntry` for testing.
    fn sample_entry() -> CacheEntry {
        CacheEntry {
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
        }
    }

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob"), None])),
                Arc::new(Int64Array::from(vec![Some(30), None, Some(25)])),
            ],
        )
        .expect("RecordBatch")
    }

    // -- cache_key tests --

    #[test]
    fn cache_key_basic() {
        let key = cache_key(
            "https://labkey.example.com/labkey",
            "/MyProject",
            "lists",
            "People",
        )
        .expect("cache_key");
        assert_eq!(key, "labkey.example.com|MyProject|lists|People");
    }

    #[test]
    fn cache_key_hostname_lowercased() {
        let key = cache_key(
            "https://LABKEY.Example.COM/labkey",
            "/MyProject",
            "lists",
            "People",
        )
        .expect("cache_key");
        assert_eq!(key, "labkey.example.com|MyProject|lists|People");
    }

    #[test]
    fn cache_key_strips_slashes() {
        let key = cache_key(
            "https://labkey.example.com",
            "/MyProject/",
            "lists",
            "People",
        )
        .expect("cache_key");
        assert_eq!(key, "labkey.example.com|MyProject|lists|People");
    }

    #[test]
    fn cache_key_root_container() {
        let key = cache_key("https://labkey.example.com", "/", "core", "Users").expect("cache_key");
        assert_eq!(key, "labkey.example.com||core|Users");
    }

    #[test]
    fn cache_key_nested_container() {
        let key = cache_key(
            "https://labkey.example.com",
            "/Project/SubFolder/",
            "lists",
            "Data",
        )
        .expect("cache_key");
        assert_eq!(key, "labkey.example.com|Project/SubFolder|lists|Data");
    }

    #[test]
    fn cache_key_preserves_schema_and_query_case() {
        let key = cache_key("https://labkey.example.com", "/proj", "MySchema", "MyQuery")
            .expect("cache_key");
        assert_eq!(key, "labkey.example.com|proj|MySchema|MyQuery");
    }

    #[test]
    fn cache_key_invalid_url_is_error() {
        let result = cache_key("not-a-url", "/", "s", "q");
        assert!(result.is_err());
    }

    #[test]
    fn cache_key_url_without_host_is_error() {
        let result = cache_key("file:///foo/bar", "/", "s", "q");
        assert!(result.is_err());
    }

    // -- entry CRUD tests --

    #[test]
    fn get_entry_returns_none_for_empty_cache() {
        let (mgr, _dir) = test_manager();
        assert!(mgr.get_entry("nonexistent").is_none());
    }

    #[test]
    fn put_and_get_entry_roundtrips_all_fields() {
        let (mgr, _dir) = test_manager();
        let entry = sample_entry();
        mgr.put_entry("key1", &entry).expect("put_entry");
        let retrieved = mgr.get_entry("key1").expect("get_entry should return Some");
        assert_eq!(retrieved, entry);
    }

    #[test]
    fn put_entry_overwrites_existing() {
        let (mgr, _dir) = test_manager();
        let mut entry = sample_entry();
        mgr.put_entry("key1", &entry).expect("put_entry");

        entry.row_count = 100;
        mgr.put_entry("key1", &entry).expect("put_entry update");

        let retrieved = mgr.get_entry("key1").expect("get_entry");
        assert_eq!(retrieved.row_count, 100);
    }

    #[test]
    fn remove_entry_existing() {
        let (mgr, _dir) = test_manager();
        let entry = sample_entry();
        mgr.put_entry("key1", &entry).expect("put_entry");

        let removed = mgr.remove_entry("key1").expect("remove_entry");
        assert!(removed.is_some());
        assert!(mgr.get_entry("key1").is_none());
    }

    #[test]
    fn remove_entry_nonexistent() {
        let (mgr, _dir) = test_manager();
        let removed = mgr.remove_entry("nope").expect("remove_entry");
        assert!(removed.is_none());
    }

    #[test]
    fn remove_entry_tolerates_missing_parquet_file() {
        let (mgr, _dir) = test_manager();
        let entry = sample_entry();
        mgr.put_entry("k", &entry).expect("put_entry");

        // The parquet file was never written — only the cache.json entry
        // exists. remove_entry should succeed without error.
        let removed = mgr
            .remove_entry("k")
            .expect("remove_entry should not error");
        assert!(removed.is_some());
        assert!(mgr.get_entry("k").is_none());
    }

    #[test]
    fn remove_entry_deletes_parquet_file() {
        let (mgr, _dir) = test_manager();
        let batch = sample_batch();

        let entry = sample_entry();
        let pq_path = mgr.parquet_path(&entry);
        CacheManager::write_parquet(&pq_path, &batch).expect("write_parquet");
        assert!(pq_path.exists());

        mgr.put_entry("k", &entry).expect("put_entry");
        mgr.remove_entry("k").expect("remove_entry");
        assert!(!pq_path.exists());
    }

    #[test]
    fn clear_all_removes_everything() {
        let (mgr, _dir) = test_manager();
        let entry = sample_entry();
        mgr.put_entry("key1", &entry).expect("put");
        mgr.put_entry("key2", &entry).expect("put");

        let cleared = mgr.clear_all().expect("clear_all");
        assert_eq!(cleared.len(), 2);
        assert!(mgr.list_entries().is_empty());
    }

    #[test]
    fn clear_all_deletes_parquet_files() {
        let (mgr, _dir) = test_manager();
        let batch = sample_batch();

        let mut entry1 = sample_entry();
        entry1.parquet_path = "host/project/lists/Table1.parquet".into();
        let pq1 = mgr.parquet_path(&entry1);
        CacheManager::write_parquet(&pq1, &batch).expect("write pq1");

        let mut entry2 = sample_entry();
        entry2.parquet_path = "host/project/lists/Table2.parquet".into();
        let pq2 = mgr.parquet_path(&entry2);
        CacheManager::write_parquet(&pq2, &batch).expect("write pq2");

        mgr.put_entry("k1", &entry1).expect("put");
        mgr.put_entry("k2", &entry2).expect("put");

        assert!(pq1.exists());
        assert!(pq2.exists());

        mgr.clear_all().expect("clear_all");

        assert!(!pq1.exists(), "Parquet file for k1 should be deleted");
        assert!(!pq2.exists(), "Parquet file for k2 should be deleted");
    }

    #[test]
    fn clear_all_on_empty_cache() {
        let (mgr, _dir) = test_manager();
        let cleared = mgr.clear_all().expect("clear_all");
        assert!(cleared.is_empty());
    }

    #[test]
    fn list_entries_empty_cache() {
        let (mgr, _dir) = test_manager();
        assert!(mgr.list_entries().is_empty());
    }

    #[test]
    fn list_entries_returns_keys_and_values() {
        let (mgr, _dir) = test_manager();
        let entry = sample_entry();
        mgr.put_entry("alpha", &entry).expect("put");
        mgr.put_entry("beta", &entry).expect("put");

        let entries = mgr.list_entries();
        assert_eq!(entries.len(), 2);
        let keys: Vec<&str> = entries.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"alpha"), "should contain 'alpha'");
        assert!(keys.contains(&"beta"), "should contain 'beta'");
    }

    // -- Parquet I/O tests --

    #[test]
    fn write_and_read_parquet_roundtrip_preserves_values() {
        let (mgr, _dir) = test_manager();
        let batch = sample_batch();
        let path = mgr.cache_dir.join("test/data.parquet");

        let size = CacheManager::write_parquet(&path, &batch).expect("write_parquet");
        assert!(size > 0);

        let read_batch = CacheManager::read_parquet(&path).expect("read_parquet");
        assert_eq!(read_batch.schema(), batch.schema());
        assert_eq!(read_batch.num_rows(), 3);
        assert_eq!(read_batch.num_columns(), 2);

        let names = read_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");
        assert!(names.is_null(2));

        let ages = read_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        assert_eq!(ages.value(0), 30);
        assert!(ages.is_null(1));
        assert_eq!(ages.value(2), 25);
    }

    #[test]
    fn write_parquet_creates_parent_dirs() {
        let (mgr, _dir) = test_manager();
        let batch = sample_batch();
        let path = mgr.cache_dir.join("deep/nested/dir/data.parquet");

        let size = CacheManager::write_parquet(&path, &batch).expect("write_parquet");
        assert!(size > 0);
        assert!(path.exists());
    }

    #[test]
    fn write_parquet_empty_batch() {
        let (mgr, _dir) = test_manager();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(Vec::<Option<&str>>::new()))],
        )
        .expect("empty batch");
        let path = mgr.cache_dir.join("empty.parquet");

        let size = CacheManager::write_parquet(&path, &batch).expect("write_parquet");
        assert!(size > 0);

        let read_batch = CacheManager::read_parquet(&path).expect("read_parquet");
        assert_eq!(read_batch.num_rows(), 0);
        assert_eq!(read_batch.num_columns(), 1);
    }

    #[test]
    fn read_parquet_missing_file_is_error() {
        let (mgr, _dir) = test_manager();
        let path = mgr.cache_dir.join("does_not_exist.parquet");
        let result = CacheManager::read_parquet(&path);
        assert!(result.is_err());
    }

    #[test]
    fn write_parquet_cleans_up_temp_file() {
        let (mgr, _dir) = test_manager();
        let batch = sample_batch();
        let path = mgr.cache_dir.join("cleanup_test.parquet");

        CacheManager::write_parquet(&path, &batch).expect("write_parquet");

        let tmp_path = path.with_extension("parquet.tmp");
        assert!(
            !tmp_path.exists(),
            "temp file should not linger after write"
        );
        assert!(path.exists(), "parquet file should exist");
    }

    #[test]
    fn write_cache_file_cleans_up_temp_file() {
        let (mgr, _dir) = test_manager();
        let entry = sample_entry();
        mgr.put_entry("k", &entry).expect("put_entry");

        let tmp_path = mgr.cache_json_path().with_extension("json.tmp");
        assert!(
            !tmp_path.exists(),
            "temp file should not linger after write"
        );
        assert!(mgr.cache_json_path().exists(), "cache.json should exist");
    }

    // -- parquet_path tests --

    #[test]
    fn parquet_path_joins_correctly() {
        let (mgr, _dir) = test_manager();
        let entry = sample_entry();
        let path = mgr.parquet_path(&entry);
        assert!(path.ends_with("labkey.example.com/myproject/lists/People.parquet"));
    }

    // -- serde tests --

    #[test]
    fn serde_entry_without_server_modified() {
        let mut entry = sample_entry();
        entry.server_modified = None;

        let json = serde_json::to_string(&entry).expect("serialize");
        assert!(!json.contains("server_modified"));

        let deserialized: CacheEntry = serde_json::from_str(&json).expect("deserialize");
        assert!(deserialized.server_modified.is_none());
    }

    #[test]
    fn serde_entry_with_server_modified() {
        let entry = sample_entry();
        let json = serde_json::to_string(&entry).expect("serialize");
        assert!(json.contains("server_modified"));

        let deserialized: CacheEntry = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            deserialized.server_modified,
            Some("2026-03-09T09:15:00Z".into())
        );
    }

    // -- cache.json resilience tests --

    #[test]
    fn read_cache_file_handles_corrupt_json() {
        let (mgr, _dir) = test_manager();
        std::fs::write(mgr.cache_json_path(), "not valid json!").expect("write");
        let file = mgr.read_cache_file();
        assert!(file.entries.is_empty());
    }

    #[test]
    fn read_cache_file_handles_missing_file() {
        let (mgr, _dir) = test_manager();
        let file = mgr.read_cache_file();
        assert_eq!(file.version, CURRENT_CACHE_VERSION);
        assert!(file.entries.is_empty());
    }

    #[test]
    fn read_cache_file_tolerates_extra_fields() {
        let (mgr, _dir) = test_manager();
        let json = r#"{"version": 2, "entries": {}, "new_field": "surprise"}"#;
        std::fs::write(mgr.cache_json_path(), json).expect("write");
        let file = mgr.read_cache_file();
        assert!(file.entries.is_empty());
        assert_eq!(file.version, 2);
    }
}
