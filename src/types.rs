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
use serde_json::Value;

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

pub(crate) fn experimental_sql_schema(
    column_names: &[String],
    column_json_types: &[String],
) -> Arc<Schema> {
    let fields: Vec<Field> = column_names
        .iter()
        .zip(column_json_types.iter())
        .map(|(name, json_type)| Field::new(name, map_json_type_to_arrow(Some(json_type)), true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// Converts experimental SQL response rows into an Arrow `RecordBatch`.
pub(crate) fn sql_rows_to_record_batch(
    rows: &[Vec<Value>],
    column_names: &[String],
    column_json_types: &[String],
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let schema = experimental_sql_schema(column_names, column_json_types);
    let arrays: Vec<Arc<dyn arrow_array::Array>> = column_json_types
        .iter()
        .enumerate()
        .map(|(column_index, json_type)| build_sql_column_array(rows, column_index, json_type))
        .collect();

    let batch = RecordBatch::try_new(schema, arrays)?;
    Ok(batch)
}

fn sql_value(row: &[Value], column_index: usize) -> Option<&Value> {
    row.get(column_index).filter(|value| !value.is_null())
}

fn sql_int64_value(row: &[Value], column_index: usize) -> Option<i64> {
    sql_value(row, column_index).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
    })
}

fn sql_float64_value(row: &[Value], column_index: usize) -> Option<f64> {
    sql_value(row, column_index).and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
    })
}

fn sql_bool_value(row: &[Value], column_index: usize) -> Option<bool> {
    sql_value(row, column_index).and_then(|value| {
        value
            .as_bool()
            .or_else(|| value.as_i64().map(|number| number != 0))
            .or_else(|| {
                value
                    .as_str()
                    .and_then(|text| match text.trim().to_ascii_lowercase().as_str() {
                        "true" | "t" | "1" | "yes" | "y" => Some(true),
                        "false" | "f" | "0" | "no" | "n" => Some(false),
                        _ => None,
                    })
            })
    })
}

fn build_sql_int64_array(rows: &[Vec<Value>], column_index: usize) -> Arc<dyn arrow_array::Array> {
    let mut builder = Int64Builder::with_capacity(rows.len());
    for row in rows {
        match sql_int64_value(row, column_index) {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn build_sql_float64_array(
    rows: &[Vec<Value>],
    column_index: usize,
) -> Arc<dyn arrow_array::Array> {
    let mut builder = Float64Builder::with_capacity(rows.len());
    for row in rows {
        match sql_float64_value(row, column_index) {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn build_sql_bool_array(rows: &[Vec<Value>], column_index: usize) -> Arc<dyn arrow_array::Array> {
    let mut builder = BooleanBuilder::with_capacity(rows.len());
    for row in rows {
        match sql_bool_value(row, column_index) {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn build_sql_timestamp_array(
    rows: &[Vec<Value>],
    column_index: usize,
) -> Arc<dyn arrow_array::Array> {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(rows.len());
    for row in rows {
        let value = sql_value(row, column_index)
            .and_then(|value| value.as_str())
            .and_then(parse_datetime_micros);
        match value {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn build_sql_date_array(rows: &[Vec<Value>], column_index: usize) -> Arc<dyn arrow_array::Array> {
    let mut builder = Date32Builder::with_capacity(rows.len());
    for row in rows {
        let value = sql_value(row, column_index)
            .and_then(|value| value.as_str())
            .and_then(parse_date_days);
        match value {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn build_sql_utf8_array(rows: &[Vec<Value>], column_index: usize) -> Arc<dyn arrow_array::Array> {
    let mut builder = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
    for row in rows {
        match sql_value(row, column_index) {
            None => builder.append_null(),
            Some(value) => {
                if let Some(text) = value.as_str() {
                    builder.append_value(text);
                } else {
                    builder.append_value(value.to_string());
                }
            }
        }
    }
    Arc::new(builder.finish())
}

fn build_sql_column_array(
    rows: &[Vec<Value>],
    column_index: usize,
    json_type: &str,
) -> Arc<dyn arrow_array::Array> {
    let arrow_type = map_json_type_to_arrow(Some(json_type));

    match arrow_type {
        DataType::Int64 => build_sql_int64_array(rows, column_index),
        DataType::Float64 => build_sql_float64_array(rows, column_index),
        DataType::Boolean => build_sql_bool_array(rows, column_index),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            build_sql_timestamp_array(rows, column_index)
        }
        DataType::Date32 => build_sql_date_array(rows, column_index),
        _ => build_sql_utf8_array(rows, column_index),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Array, BooleanArray, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use serde_json::json;

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
    fn sql_rows_to_record_batch_parses_basic_types() {
        let rows = vec![
            vec![json!(1), json!("Alice"), json!("true")],
            vec![json!(2), serde_json::Value::Null, json!(0)],
        ];
        let column_names = vec!["id".into(), "name".into(), "active".into()];
        let column_json_types = vec!["int".into(), "string".into(), "boolean".into()];

        let batch = sql_rows_to_record_batch(&rows, &column_names, &column_json_types)
            .expect("sql RecordBatch");

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray");
        assert_eq!(names.value(0), "Alice");
        assert!(names.is_null(1));

        let active = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("BooleanArray");
        assert!(active.value(0));
        assert!(!active.value(1));
    }

    #[test]
    fn sql_rows_to_record_batch_parses_dates_and_timestamps() {
        let rows = vec![vec![json!("2024-01-15"), json!("2024-01-15T09:30:00.000Z")]];
        let column_names = vec!["created_on".into(), "created_at".into()];
        let column_json_types = vec!["date".into(), "dateTime".into()];

        let batch = sql_rows_to_record_batch(&rows, &column_names, &column_json_types)
            .expect("sql RecordBatch");

        let dates = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("Date32Array");
        assert_eq!(
            dates.value(0),
            parse_date_days("2024-01-15").expect("date days")
        );

        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("TimestampMicrosecondArray");
        assert_eq!(
            timestamps.value(0),
            parse_datetime_micros("2024-01-15T09:30:00.000Z").expect("timestamp micros")
        );
    }
}
