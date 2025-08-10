use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use polars::prelude::*;
use polars_schema_validate::PolarsSchema;

#[derive(Debug, PolarsSchema)]
#[allow(dead_code)]
struct EventRecord {
    id: i32,
    name: String,
    event_date: NaiveDate,
    event_datetime: NaiveDateTime,
    event_time: NaiveTime,
    created_at: NaiveDateTime,  // Use NaiveDateTime instead of DateTime<Utc> for simpler testing
}

#[test]
fn test_chrono_types_valid() {
    // Create DataFrame using string representation and then parse to temporal types
    let mut df = df![
        "id" => [1, 2, 3],
        "name" => ["Event A", "Event B", "Event C"],
        "event_date" => ["2023-01-01", "2023-02-01", "2023-03-01"],
        "event_datetime" => ["2023-01-01 12:00:00", "2023-02-01 13:30:00", "2023-03-01 14:15:30"],
        "event_time" => ["09:00:00", "10:30:00", "11:45:30"],
        "created_at" => ["2023-01-01T12:00:00Z", "2023-02-01T13:30:00Z", "2023-03-01T14:15:30Z"],
    ].unwrap();
    
    // Parse string columns to proper temporal types
    df = df.lazy()
        .with_column(col("event_date").str().to_date(StrptimeOptions::default()))
        .with_column(col("event_datetime").str().to_datetime(None, None, StrptimeOptions::default(), lit("raise")))
        .with_column(col("event_time").str().to_time(StrptimeOptions::default()))
        .with_column(col("created_at").str().to_datetime(None, None, StrptimeOptions { format: Some("%Y-%m-%dT%H:%M:%SZ".into()), ..StrptimeOptions::default() }, lit("raise")))
        .collect().unwrap();

    assert!(EventRecord::validate(&df).is_ok());
}

#[test]
fn test_chrono_schema_generation() {
    let schema = EventRecord::schema();
    assert_eq!(schema.len(), 6);
    
    let field_names: Vec<_> = schema.iter().map(|(name, _)| *name).collect();
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"name"));
    assert!(field_names.contains(&"event_date"));
    assert!(field_names.contains(&"event_datetime"));
    assert!(field_names.contains(&"event_time"));
    assert!(field_names.contains(&"created_at"));
    
    // Check specific types
    let schema_map: std::collections::HashMap<_, _> = schema.into_iter().collect();
    assert_eq!(schema_map["event_date"], DataType::Date);
    assert!(matches!(schema_map["event_datetime"], DataType::Datetime(_, None)));
    assert_eq!(schema_map["event_time"], DataType::Time);
    assert!(matches!(schema_map["created_at"], DataType::Datetime(_, None)));
}

#[test]
fn test_optional_chrono_types() {
    #[derive(Debug, PolarsSchema)]
    #[allow(dead_code)]
    struct OptionalEventRecord {
        id: i32,
        event_date: Option<NaiveDate>,
        event_datetime: Option<NaiveDateTime>,
    }

    // Create DataFrame with nullable temporal strings
    let mut df = df![
        "id" => [1, 2, 3],
        "event_date" => [Some("2023-01-01"), None, Some("2023-03-01")],
        "event_datetime" => [Some("2023-01-01 12:00:00"), None, Some("2023-03-01 14:15:30")],
    ].unwrap();
    
    // Parse string columns to proper temporal types with null handling
    df = df.lazy()
        .with_column(col("event_date").str().to_date(StrptimeOptions::default()))
        .with_column(col("event_datetime").str().to_datetime(None, None, StrptimeOptions::default(), lit("raise")))
        .collect().unwrap();

    assert!(OptionalEventRecord::validate(&df).is_ok());
}

#[test]
fn test_chrono_type_mismatch() {
    let df = df![
        "id" => [1, 2, 3],
        "name" => ["Event A", "Event B", "Event C"],
        "event_date" => ["2023-01-01", "2023-02-01", "2023-03-01"], // String instead of Date
        "event_datetime" => ["invalid_datetime", "invalid_datetime2", "invalid_datetime3"], // Invalid datetime strings
        "event_time" => ["invalid_time", "invalid_time2", "invalid_time3"], // Invalid time strings
        "created_at" => ["invalid_datetime", "invalid_datetime2", "invalid_datetime3"], // Invalid datetime strings
    ].unwrap();

    let result = EventRecord::validate(&df);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("type"));
}