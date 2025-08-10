use polars::prelude::*;
use polars_schema_validate::{PolarsSchema, ValidationError};
use rayon::prelude::*;

#[derive(Debug, PolarsSchema)]
#[allow(dead_code)]
struct TestData {
    id: i32,
    name: String,
    value: f64,
}

#[test]
fn test_validation_in_rayon_parallel_iterator() {
    // Create multiple DataFrames - some valid, some invalid
    let valid_df1 = df![
        "id" => [1, 2],
        "name" => ["Alice", "Bob"],
        "value" => [10.5, 20.3],
    ].unwrap();
    
    let valid_df2 = df![
        "id" => [3, 4],
        "name" => ["Charlie", "Diana"],
        "value" => [15.2, 25.7],
    ].unwrap();
    
    let invalid_df = df![
        "id" => [5, 6],
        "name" => ["Eve", "Frank"],
        "wrong_column" => [30.1, 40.2], // Missing 'value' column
    ].unwrap();
    
    let dataframes = vec![valid_df1, valid_df2, invalid_df];
    
    // Process DataFrames in parallel using rayon
    let results: Vec<Result<String, ValidationError>> = dataframes
        .into_par_iter()
        .enumerate()
        .map(|(i, df)| {
            // Validate the DataFrame
            TestData::validate(&df)?;
            
            // If validation passes, return success message
            Ok(format!("DataFrame {} is valid with {} rows", i, df.height()))
        })
        .collect();
    
    // Check results
    assert_eq!(results.len(), 3);
    
    // First two should be valid
    assert!(results[0].is_ok());
    assert!(results[1].is_ok());
    assert_eq!(results[0].as_ref().unwrap(), "DataFrame 0 is valid with 2 rows");
    assert_eq!(results[1].as_ref().unwrap(), "DataFrame 1 is valid with 2 rows");
    
    // Third should be invalid with a MissingColumn error
    assert!(results[2].is_err());
    let error = results[2].as_ref().unwrap_err();
    match error {
        ValidationError::MissingColumn { column_name } => {
            assert_eq!(column_name, "value");
        }
        _ => panic!("Expected MissingColumn error, got: {:?}", error),
    }
}

#[test]
fn test_error_propagation_across_threads() {
    // Create multiple invalid DataFrames with different error types
    let missing_column_df = df![
        "id" => [1, 2],
        "name" => ["Alice", "Bob"],
        // Missing 'value' column
    ].unwrap();
    
    let type_mismatch_df = df![
        "id" => [3, 4],
        "name" => ["Charlie", "Diana"],
        "value" => ["not_a_number", "also_not_a_number"], // String instead of f64
    ].unwrap();
    
    let dataframes = vec![missing_column_df, type_mismatch_df];
    
    // Process in parallel and collect errors
    let errors: Vec<ValidationError> = dataframes
        .into_par_iter()
        .map(|df| TestData::validate(&df))
        .filter_map(|result| result.err())
        .collect();
    
    assert_eq!(errors.len(), 2);
    
    // Verify we can pattern match on the errors from different threads
    let mut has_missing_column = false;
    let mut has_type_mismatch = false;
    
    for error in errors {
        match error {
            ValidationError::MissingColumn { column_name } => {
                assert_eq!(column_name, "value");
                has_missing_column = true;
            }
            ValidationError::TypeMismatch { column_name, .. } => {
                assert_eq!(column_name, "value");
                has_type_mismatch = true;
            }
            _ => panic!("Unexpected error type: {:?}", error),
        }
    }
    
    assert!(has_missing_column);
    assert!(has_type_mismatch);
}

#[test]
fn test_strict_validation_in_parallel() {
    // Test strict validation across multiple threads
    let exact_match_df = df![
        "id" => [1, 2],
        "name" => ["Alice", "Bob"],
        "value" => [10.5, 20.3],
    ].unwrap();
    
    let extra_column_df = df![
        "id" => [3, 4],
        "name" => ["Charlie", "Diana"],
        "value" => [15.2, 25.7],
        "extra" => ["unwanted", "column"], // Extra column
    ].unwrap();
    
    let dataframes = vec![exact_match_df, extra_column_df];
    
    // Run strict validation in parallel
    let results: Vec<Result<(), ValidationError>> = dataframes
        .into_par_iter()
        .map(|df| TestData::validate_strict(&df))
        .collect();
    
    assert_eq!(results.len(), 2);
    
    // First should pass strict validation
    assert!(results[0].is_ok());
    
    // Second should fail - could be either ColumnCountMismatch or UnexpectedColumn 
    assert!(results[1].is_err());
    let error = results[1].as_ref().unwrap_err();
    match error {
        ValidationError::ColumnCountMismatch { expected_count, actual_count } => {
            assert_eq!(*expected_count, 3);
            assert_eq!(*actual_count, 4);
        }
        ValidationError::UnexpectedColumn { column_name } => {
            assert_eq!(column_name, "extra");
        }
        _ => panic!("Expected ColumnCountMismatch or UnexpectedColumn error, got: {:?}", error),
    }
}

#[test]
fn test_large_scale_parallel_validation() {
    // Test with many DataFrames to stress test thread safety
    let dataframes: Vec<DataFrame> = (0..100)
        .map(|i| {
            if i % 10 == 0 {
                // Every 10th DataFrame is invalid (missing column)
                df![
                    "id" => [i, i + 1000],
                    "name" => [format!("User_{}", i), format!("User_{}", i + 1000)],
                    // Missing 'value' column
                ].unwrap()
            } else {
                // Valid DataFrames
                df![
                    "id" => [i, i + 1000],
                    "name" => [format!("User_{}", i), format!("User_{}", i + 1000)],
                    "value" => [i as f64 * 1.5, (i + 1000) as f64 * 1.5],
                ].unwrap()
            }
        })
        .collect();
    
    // Validate all in parallel
    let results: Vec<Result<(), ValidationError>> = dataframes
        .into_par_iter()
        .map(|df| TestData::validate(&df))
        .collect();
    
    assert_eq!(results.len(), 100);
    
    // Count successes and failures
    let successes = results.iter().filter(|r| r.is_ok()).count();
    let failures = results.iter().filter(|r| r.is_err()).count();
    
    assert_eq!(successes, 90); // 90 valid DataFrames
    assert_eq!(failures, 10);  // 10 invalid DataFrames (every 10th)
    
    // Verify all errors are MissingColumn errors for 'value'
    for result in results {
        if let Err(ValidationError::MissingColumn { column_name }) = result {
            assert_eq!(column_name, "value");
        }
    }
}