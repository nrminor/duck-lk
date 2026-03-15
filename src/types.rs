//! Type mapping and data conversion between `LabKey`, Arrow, and `DuckDB`.
//!
//! This module is pure data transformation with no I/O or network dependencies.
//! It provides:
//! - Mapping `LabKey` `json_type` strings to `DuckDB` `LogicalTypeId`
//! - Mapping `LabKey` `json_type` strings to Arrow `DataType`
//! - Date/time string parsing with chrono
//! - Conversion of `labkey-rs` `Vec<Row>` into Arrow `RecordBatch`

use std::sync::Arc;

use arrow_array::{
    builder::{
        BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
        TimestampMicrosecondBuilder,
    },
    RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime};
use duckdb::core::LogicalTypeId;
use labkey_rs::query::{QueryColumn, Row};

/// Format strings tried (in order) when parsing a `LabKey` datetime value.
/// The `Z` suffix is stripped before attempting these.
const DATETIME_FORMATS: &[&str] = &[
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S",
];

/// Format strings tried when falling back to date-only parsing (midnight UTC).
const DATE_ONLY_FORMATS: &[&str] = &["%Y-%m-%d", "%m/%d/%Y"];

/// Maps a `LabKey` `json_type` string to a `DuckDB` `LogicalTypeId`.
///
/// Returns `Varchar` for `None`, unknown, or `"time"` types.
#[must_use]
pub(crate) fn map_json_type_to_duckdb(json_type: Option<&str>) -> LogicalTypeId {
    match json_type {
        Some("int") => LogicalTypeId::Bigint,
        Some("float") => LogicalTypeId::Double,
        Some("boolean") => LogicalTypeId::Boolean,
        Some("dateTime") => LogicalTypeId::Timestamp,
        Some("date") => LogicalTypeId::Date,
        Some("string" | "time" | _) | None => LogicalTypeId::Varchar,
    }
}

/// Maps a `LabKey` `json_type` string to an Arrow `DataType`.
///
/// Returns `Utf8` for `None`, unknown, or `"time"` types.
#[must_use]
pub(crate) fn map_json_type_to_arrow(json_type: Option<&str>) -> DataType {
    match json_type {
        Some("int") => DataType::Int64,
        Some("float") => DataType::Float64,
        Some("boolean") => DataType::Boolean,
        Some("dateTime") => DataType::Timestamp(TimeUnit::Microsecond, None),
        Some("date") => DataType::Date32,
        Some("string" | "time" | _) | None => DataType::Utf8,
    }
}

/// Parses a date/time string from `LabKey` into microseconds since the Unix epoch.
///
/// Tries multiple formats in order of likelihood:
/// 1. ISO 8601 with `T` separator and `Z` suffix (`2024-01-15T09:30:00.000Z`)
/// 2. ISO 8601 with `T` separator, no timezone (`2024-01-15T09:30:00.123`)
/// 3. Space separator, no timezone (`2024-01-15 09:30:00.000`)
/// 4. No fractional seconds (`2024-01-15T09:30:00`)
/// 5. Date only (`2024-01-15`) — treated as midnight UTC
/// 6. US format (`1/15/2024` or `01/15/2024`) — treated as midnight UTC
///
/// Returns `None` if none of the formats match.
#[must_use]
pub(crate) fn parse_datetime_micros(s: &str) -> Option<i64> {
    let normalized = s.strip_suffix('Z').unwrap_or(s);

    DATETIME_FORMATS
        .iter()
        .find_map(|fmt| NaiveDateTime::parse_from_str(normalized, fmt).ok())
        .map(|dt| dt.and_utc().timestamp_micros())
        .or_else(|| {
            DATE_ONLY_FORMATS
                .iter()
                .find_map(|fmt| NaiveDate::parse_from_str(normalized, fmt).ok())
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .map(|dt| dt.and_utc().timestamp_micros())
        })
}

/// Parses a date string from `LabKey` into days since the Unix epoch.
///
/// Tries date-only formats first (`%Y-%m-%d`, `%m/%d/%Y`), then falls back
/// to datetime formats and extracts the date component. This handles servers
/// that send date columns as datetime strings like `"2023-12-05 00:00:00.000"`.
///
/// Returns `None` if parsing fails.
#[must_use]
pub(crate) fn parse_date_days(s: &str) -> Option<i32> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    let normalized = s.strip_suffix('Z').unwrap_or(s);

    DATE_ONLY_FORMATS
        .iter()
        .find_map(|fmt| NaiveDate::parse_from_str(normalized, fmt).ok())
        .or_else(|| {
            DATETIME_FORMATS
                .iter()
                .find_map(|fmt| NaiveDateTime::parse_from_str(normalized, fmt).ok())
                .map(|dt| dt.date())
        })
        .and_then(|d| i32::try_from((d - epoch).num_days()).ok())
}

/// Converts `LabKey` rows to an Arrow `RecordBatch`.
///
/// `columns` is the filtered column list (no hidden/URL columns — the caller
/// is responsible for filtering those before calling this function).
///
/// Column order in the `RecordBatch` matches the order of `columns`.
///
/// # Null rules
/// - Missing keys in `row.data` produce null.
/// - Cells where `mv_indicator.is_some()` produce null regardless of `value`.
/// - JSON `null` values produce null.
/// - Failed type conversions produce null (never default values).
///
/// # Errors
///
/// Returns an error if `RecordBatch` construction fails (e.g. schema mismatch).
pub(crate) fn rows_to_record_batch(
    rows: &[Row],
    columns: &[QueryColumn],
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| {
            let dt = map_json_type_to_arrow(col.json_type.as_deref());
            Field::new(&col.name, dt, true)
        })
        .collect();

    let schema = Arc::new(Schema::new(fields));

    let arrays: Vec<Arc<dyn arrow_array::Array>> = columns
        .iter()
        .map(|col| build_column_array(rows, col))
        .collect();

    let batch = RecordBatch::try_new(schema, arrays)?;
    Ok(batch)
}

/// Builds an Arrow array for a single column from all rows.
fn build_column_array(rows: &[Row], col: &QueryColumn) -> Arc<dyn arrow_array::Array> {
    let arrow_type = map_json_type_to_arrow(col.json_type.as_deref());

    match arrow_type {
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(rows.len());
            for row in rows {
                append_int64(&mut builder, row, &col.name);
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(rows.len());
            for row in rows {
                append_float64(&mut builder, row, &col.name);
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(rows.len());
            for row in rows {
                append_boolean(&mut builder, row, &col.name);
            }
            Arc::new(builder.finish())
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(rows.len());
            for row in rows {
                append_timestamp(&mut builder, row, &col.name);
            }
            Arc::new(builder.finish())
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::with_capacity(rows.len());
            for row in rows {
                append_date32(&mut builder, row, &col.name);
            }
            Arc::new(builder.finish())
        }
        // Utf8 covers "string", "time", None, and unknown json_types
        _ => {
            let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
            for row in rows {
                append_utf8(&mut builder, row, &col.name);
            }
            Arc::new(builder.finish())
        }
    }
}

/// Returns `Some(&CellValue)` if the cell is present, not null, and not marked
/// as missing-value. Returns `None` if the key is missing from the row, the
/// cell has `mv_indicator.is_some()`, or the value is JSON `null`.
fn get_cell<'a>(row: &'a Row, col_name: &str) -> Option<&'a labkey_rs::query::CellValue> {
    let cell = row.data.get(col_name)?;
    if cell.mv_indicator.is_some() || cell.value.is_null() {
        return None;
    }
    Some(cell)
}

fn append_int64(builder: &mut Int64Builder, row: &Row, col_name: &str) {
    let value = get_cell(row, col_name).and_then(|cell| {
        cell.value
            .as_i64()
            .or_else(|| cell.value.as_str().and_then(|s| s.parse().ok()))
    });
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_float64(builder: &mut Float64Builder, row: &Row, col_name: &str) {
    let value = get_cell(row, col_name).and_then(|cell| {
        cell.value
            .as_f64()
            .or_else(|| cell.value.as_str().and_then(|s| s.parse().ok()))
    });
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_boolean(builder: &mut BooleanBuilder, row: &Row, col_name: &str) {
    let value = get_cell(row, col_name).and_then(|cell| cell.value.as_bool());
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_timestamp(builder: &mut TimestampMicrosecondBuilder, row: &Row, col_name: &str) {
    let value = get_cell(row, col_name)
        .and_then(|cell| cell.value.as_str())
        .and_then(parse_datetime_micros);
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_date32(builder: &mut Date32Builder, row: &Row, col_name: &str) {
    let value = get_cell(row, col_name)
        .and_then(|cell| cell.value.as_str())
        .and_then(parse_date_days);
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn append_utf8(builder: &mut StringBuilder, row: &Row, col_name: &str) {
    match get_cell(row, col_name) {
        None => builder.append_null(),
        Some(cell) => {
            if let Some(s) = cell.value.as_str() {
                builder.append_value(s);
            } else {
                // Non-string, non-null values: serialize as string via to_string()
                builder.append_value(cell.value.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Array, BooleanArray, Date32Array, Float64Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use labkey_rs::query::CellValue;
    use serde_json::json;

    fn cell(value: serde_json::Value) -> CellValue {
        CellValue {
            value,
            display_value: None,
            formatted_value: None,
            url: None,
            mv_value: None,
            mv_indicator: None,
        }
    }

    fn cell_with_mv(value: serde_json::Value, mv: &str) -> CellValue {
        CellValue {
            value,
            display_value: None,
            formatted_value: None,
            url: None,
            mv_value: None,
            mv_indicator: Some(mv.to_string()),
        }
    }

    fn make_row(data: Vec<(&str, CellValue)>) -> Row {
        Row {
            data: data.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            links: None,
        }
    }

    fn make_column(name: &str, json_type: Option<&str>) -> QueryColumn {
        let mut col_json = json!({
            "name": name,
            "fieldKey": name,
            "hidden": false,
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
        serde_json::from_value(col_json).expect("QueryColumn deserialization must not change")
    }

    /// Expected microsecond timestamp for 2024-01-15 09:30:00 UTC.
    fn expected_micros_2024_01_15_0930() -> i64 {
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .and_then(|d| d.and_hms_opt(9, 30, 0))
            .map(|dt| dt.and_utc().timestamp_micros())
            .expect("hardcoded date is valid")
    }

    /// Expected microsecond timestamp for 2024-01-15 00:00:00 UTC.
    fn expected_micros_2024_01_15_midnight() -> i64 {
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map(|dt| dt.and_utc().timestamp_micros())
            .expect("hardcoded date is valid")
    }

    #[test]
    fn map_json_type_to_duckdb_all_variants() {
        let cases: &[(Option<&str>, LogicalTypeId)] = &[
            (Some("int"), LogicalTypeId::Bigint),
            (Some("float"), LogicalTypeId::Double),
            (Some("string"), LogicalTypeId::Varchar),
            (Some("boolean"), LogicalTypeId::Boolean),
            (Some("dateTime"), LogicalTypeId::Timestamp),
            (Some("date"), LogicalTypeId::Date),
            (Some("time"), LogicalTypeId::Varchar),
            (None, LogicalTypeId::Varchar),
            (Some("imaginary"), LogicalTypeId::Varchar),
            (Some(""), LogicalTypeId::Varchar),
        ];
        for (input, expected) in cases {
            assert_eq!(
                map_json_type_to_duckdb(*input),
                *expected,
                "json_type: {input:?}"
            );
        }
    }

    #[test]
    fn map_json_type_to_arrow_all_variants() {
        let cases: &[(Option<&str>, DataType)] = &[
            (Some("int"), DataType::Int64),
            (Some("float"), DataType::Float64),
            (Some("string"), DataType::Utf8),
            (Some("boolean"), DataType::Boolean),
            (
                Some("dateTime"),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (Some("date"), DataType::Date32),
            (Some("time"), DataType::Utf8),
            (None, DataType::Utf8),
            (Some("imaginary"), DataType::Utf8),
            (Some(""), DataType::Utf8),
        ];
        for (input, expected) in cases {
            assert_eq!(
                map_json_type_to_arrow(*input),
                *expected,
                "json_type: {input:?}"
            );
        }
    }

    #[test]
    fn duckdb_and_arrow_types_agree() {
        let json_types: &[Option<&str>] = &[
            Some("int"),
            Some("float"),
            Some("string"),
            Some("boolean"),
            Some("dateTime"),
            Some("date"),
            Some("time"),
            None,
            Some("unknown"),
        ];
        // Each DuckDB type must correspond to the matching Arrow type.
        let expected_pairs: &[(LogicalTypeId, DataType)] = &[
            (LogicalTypeId::Bigint, DataType::Int64),
            (LogicalTypeId::Double, DataType::Float64),
            (LogicalTypeId::Varchar, DataType::Utf8),
            (LogicalTypeId::Boolean, DataType::Boolean),
            (
                LogicalTypeId::Timestamp,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (LogicalTypeId::Date, DataType::Date32),
            (LogicalTypeId::Varchar, DataType::Utf8),
            (LogicalTypeId::Varchar, DataType::Utf8),
            (LogicalTypeId::Varchar, DataType::Utf8),
        ];
        for (jt, (expected_duck, expected_arrow)) in json_types.iter().zip(expected_pairs) {
            assert_eq!(
                map_json_type_to_duckdb(*jt),
                *expected_duck,
                "DuckDB mapping mismatch for {jt:?}"
            );
            assert_eq!(
                map_json_type_to_arrow(*jt),
                *expected_arrow,
                "Arrow mapping mismatch for {jt:?}"
            );
        }
    }

    #[test]
    fn datetime_iso8601_with_z() {
        let micros = parse_datetime_micros("2024-01-15T09:30:00.000Z");
        assert_eq!(micros, Some(expected_micros_2024_01_15_0930()));
    }

    #[test]
    fn datetime_iso8601_with_z_no_fractional() {
        let micros = parse_datetime_micros("2024-01-15T09:30:00Z");
        assert_eq!(micros, Some(expected_micros_2024_01_15_0930()));
    }

    #[test]
    fn datetime_t_separator_fractional_no_z() {
        let micros = parse_datetime_micros("2024-01-15T09:30:00.123");
        assert!(micros.is_some());
        // Should differ from the .000 variant by exactly 123_000 microseconds
        let base = expected_micros_2024_01_15_0930();
        assert_eq!(micros, Some(base + 123_000));
    }

    #[test]
    fn datetime_microsecond_precision() {
        let micros = parse_datetime_micros("2024-01-15T09:30:00.123456Z");
        let base = expected_micros_2024_01_15_0930();
        assert_eq!(micros, Some(base + 123_456));
    }

    #[test]
    fn datetime_space_separator() {
        let micros = parse_datetime_micros("2024-01-15 09:30:00.000");
        assert_eq!(micros, Some(expected_micros_2024_01_15_0930()));
    }

    #[test]
    fn datetime_no_fractional_seconds() {
        let micros = parse_datetime_micros("2024-01-15T09:30:00");
        assert_eq!(micros, Some(expected_micros_2024_01_15_0930()));
    }

    #[test]
    fn datetime_date_only() {
        let micros = parse_datetime_micros("2024-01-15");
        assert_eq!(micros, Some(expected_micros_2024_01_15_midnight()));
    }

    #[test]
    fn datetime_us_format() {
        let micros = parse_datetime_micros("1/15/2024");
        assert_eq!(micros, Some(expected_micros_2024_01_15_midnight()));
        let micros_padded = parse_datetime_micros("01/15/2024");
        assert_eq!(micros, micros_padded);
    }

    #[test]
    fn datetime_malformed() {
        assert!(parse_datetime_micros("not-a-date").is_none());
        assert!(parse_datetime_micros("").is_none());
        assert!(parse_datetime_micros("2024-13-40").is_none());
    }

    #[test]
    fn date_iso_format() {
        let days = parse_date_days("2024-01-15");
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date");
        let expected = NaiveDate::from_ymd_opt(2024, 1, 15).expect("valid date");
        assert_eq!(days, i32::try_from((expected - epoch).num_days()).ok());
    }

    #[test]
    fn date_us_format() {
        let days = parse_date_days("01/15/2024");
        assert_eq!(days, parse_date_days("2024-01-15"));
    }

    #[test]
    fn date_us_format_no_padding() {
        let days = parse_date_days("1/5/2024");
        assert_eq!(days, parse_date_days("2024-01-05"));
    }

    #[test]
    fn date_epoch_boundary() {
        assert_eq!(parse_date_days("1970-01-01"), Some(0));
    }

    #[test]
    fn date_pre_epoch() {
        let days = parse_date_days("1969-12-31");
        assert_eq!(days, Some(-1));
    }

    #[test]
    fn date_from_datetime_string() {
        // LabKey servers sometimes send date columns as datetime strings
        assert_eq!(
            parse_date_days("2023-12-05 00:00:00.000"),
            parse_date_days("2023-12-05")
        );
    }

    #[test]
    fn date_from_iso_datetime_string() {
        assert_eq!(
            parse_date_days("2024-01-15T09:30:00.000Z"),
            parse_date_days("2024-01-15")
        );
    }

    #[test]
    fn date_malformed() {
        assert!(parse_date_days("not-a-date").is_none());
        assert!(parse_date_days("").is_none());
        assert!(parse_date_days("2024-13-40").is_none());
    }

    #[test]
    fn record_batch_int64_extraction() {
        let columns = vec![make_column("id", Some("int"))];
        let rows = vec![
            make_row(vec![("id", cell(json!(42)))]),
            make_row(vec![("id", cell(json!("99")))]),
            make_row(vec![("id", cell(json!("not_a_number")))]),
        ];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        assert_eq!(batch.num_rows(), 3);

        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        assert_eq!(arr.value(0), 42);
        assert_eq!(arr.value(1), 99);
        assert!(arr.is_null(2));
    }

    #[test]
    fn record_batch_int64_null_value() {
        let columns = vec![make_column("id", Some("int"))];
        let rows = vec![make_row(vec![("id", cell(json!(null)))])];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        assert!(arr.is_null(0));
    }

    #[test]
    fn record_batch_int64_float_encoded() {
        // LabKey sometimes sends 42.0 for int columns; as_i64() returns None
        // for f64 values in serde_json, so this becomes null. Document this.
        let columns = vec![make_column("id", Some("int"))];
        let rows = vec![make_row(vec![("id", cell(json!(42.0)))])];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        // serde_json::Value(42.0).as_i64() returns None — this is null, not 42
        assert!(arr.is_null(0));
    }

    #[test]
    fn record_batch_float64_extraction() {
        let columns = vec![make_column("score", Some("float"))];
        let rows = vec![
            make_row(vec![("score", cell(json!(3.125)))]),
            make_row(vec![("score", cell(json!("2.5")))]),
            make_row(vec![("score", cell(json!("nan_value")))]),
        ];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");

        assert!((arr.value(0) - 3.125).abs() < f64::EPSILON);
        assert!((arr.value(1) - 2.5).abs() < f64::EPSILON);
        assert!(arr.is_null(2));
    }

    #[test]
    fn record_batch_float64_from_integer() {
        // json!(42) in a float column: as_f64() on a JSON integer returns Some(42.0)
        let columns = vec![make_column("score", Some("float"))];
        let rows = vec![make_row(vec![("score", cell(json!(42)))])];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");
        assert!((arr.value(0) - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn record_batch_float64_null_value() {
        let columns = vec![make_column("score", Some("float"))];
        let rows = vec![make_row(vec![("score", cell(json!(null)))])];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");
        assert!(arr.is_null(0));
    }

    #[test]
    fn record_batch_boolean_extraction() {
        let columns = vec![make_column("active", Some("boolean"))];
        let rows = vec![
            make_row(vec![("active", cell(json!(true)))]),
            make_row(vec![("active", cell(json!(false)))]),
            make_row(vec![("active", cell(json!("true")))]),
        ];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("BooleanArray");

        assert!(arr.value(0));
        assert!(!arr.value(1));
        assert!(arr.is_null(2)); // as_bool() on a string returns None
    }

    #[test]
    fn record_batch_boolean_null_value() {
        let columns = vec![make_column("active", Some("boolean"))];
        let rows = vec![make_row(vec![("active", cell(json!(null)))])];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("BooleanArray");
        assert!(arr.is_null(0));
    }

    #[test]
    fn record_batch_utf8_extraction() {
        let columns = vec![make_column("name", Some("string"))];
        let rows = vec![
            make_row(vec![("name", cell(json!("Alice")))]),
            make_row(vec![("name", cell(json!(42)))]),
        ];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");

        assert_eq!(arr.value(0), "Alice");
        assert_eq!(arr.value(1), "42");
    }

    #[test]
    fn record_batch_utf8_null_value_is_sql_null() {
        // json!(null) in a string column must produce SQL NULL, not the
        // four-character string "null".
        let columns = vec![make_column("name", Some("string"))];
        let rows = vec![make_row(vec![("name", cell(json!(null)))])];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert!(arr.is_null(0));
    }

    #[test]
    fn record_batch_timestamp_extraction() {
        let columns = vec![make_column("created", Some("dateTime"))];
        let rows = vec![
            make_row(vec![("created", cell(json!("2024-01-15T09:30:00.000Z")))]),
            make_row(vec![("created", cell(json!("bad-datetime")))]),
        ];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("TimestampMicrosecondArray");

        assert_eq!(arr.value(0), expected_micros_2024_01_15_0930());
        assert!(arr.is_null(1));
    }

    #[test]
    fn record_batch_date32_extraction() {
        let columns = vec![make_column("dob", Some("date"))];
        let rows = vec![
            make_row(vec![("dob", cell(json!("2024-01-15")))]),
            make_row(vec![("dob", cell(json!("01/15/2024")))]),
            make_row(vec![("dob", cell(json!("bad-date")))]),
        ];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("Date32Array");

        assert!(!arr.is_null(0));
        assert_eq!(arr.value(0), arr.value(1));
        assert!(arr.is_null(2));
    }

    #[test]
    fn record_batch_mv_indicator_nulls_int() {
        let columns = vec![make_column("id", Some("int"))];
        let rows = vec![make_row(vec![("id", cell_with_mv(json!(42), "Q"))])];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        assert!(arr.is_null(0));
    }

    #[test]
    fn record_batch_mv_indicator_nulls_string() {
        let columns = vec![make_column("name", Some("string"))];
        let rows = vec![make_row(vec![("name", cell_with_mv(json!("Alice"), "Q"))])];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert!(arr.is_null(0));
    }

    #[test]
    fn record_batch_missing_key_is_null() {
        let columns = vec![
            make_column("present", Some("int")),
            make_column("absent", Some("int")),
        ];
        let rows = vec![make_row(vec![("present", cell(json!(1)))])];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let present_arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        let absent_arr = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");

        assert_eq!(present_arr.value(0), 1);
        assert!(absent_arr.is_null(0));
    }

    #[test]
    fn record_batch_sparse_rows() {
        let columns = vec![
            make_column("a", Some("string")),
            make_column("b", Some("int")),
            make_column("c", Some("float")),
        ];
        let rows = vec![
            make_row(vec![("a", cell(json!("hello")))]),
            make_row(vec![("b", cell(json!(42)))]),
            make_row(vec![("c", cell(json!(3.125)))]),
        ];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);

        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(a.value(0), "hello");
        assert!(a.is_null(1));
        assert!(a.is_null(2));

        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        assert!(b.is_null(0));
        assert_eq!(b.value(1), 42);
        assert!(b.is_null(2));

        let c = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64Array");
        assert!(c.is_null(0));
        assert!(c.is_null(1));
        assert!((c.value(2) - 3.125).abs() < f64::EPSILON);
    }

    #[test]
    fn record_batch_empty_rows() {
        let columns = vec![make_column("id", Some("int"))];
        let rows: Vec<Row> = vec![];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn record_batch_no_columns_returns_error() {
        let columns: Vec<QueryColumn> = vec![];
        let rows = vec![make_row(vec![("x", cell(json!(1)))])];

        let result = rows_to_record_batch(&rows, &columns);
        assert!(result.is_err());
    }

    #[test]
    fn record_batch_none_json_type_fallback() {
        let columns = vec![make_column("mystery", None)];
        let rows = vec![
            make_row(vec![("mystery", cell(json!("text")))]),
            make_row(vec![("mystery", cell(json!(123)))]),
        ];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");

        assert_eq!(arr.value(0), "text");
        assert_eq!(arr.value(1), "123");
    }

    #[test]
    fn record_batch_time_type_as_utf8() {
        let columns = vec![make_column("t", Some("time"))];
        let rows = vec![make_row(vec![("t", cell(json!("14:30:00")))])];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(arr.value(0), "14:30:00");
    }

    #[test]
    fn record_batch_multiple_types_smoke() {
        let columns = vec![
            make_column("id", Some("int")),
            make_column("name", Some("string")),
            make_column("score", Some("float")),
            make_column("active", Some("boolean")),
            make_column("created", Some("dateTime")),
            make_column("dob", Some("date")),
        ];
        let rows = vec![make_row(vec![
            ("id", cell(json!(1))),
            ("name", cell(json!("Alice"))),
            ("score", cell(json!(95.5))),
            ("active", cell(json!(true))),
            ("created", cell(json!("2024-01-15T09:30:00.000Z"))),
            ("dob", cell(json!("1990-05-20"))),
        ])];

        let batch = rows_to_record_batch(&rows, &columns).expect("batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 6);

        // Spot-check a couple values to go beyond shape testing
        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        assert_eq!(id.value(0), 1);

        let name = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(name.value(0), "Alice");
    }
}
