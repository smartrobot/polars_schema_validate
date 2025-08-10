use polars::prelude::*;

pub use polars_schema_derive::PolarsSchema;

mod error;
pub use error::{ValidationError, Result};

/// Trait for validating Polars DataFrames against a schema derived from Rust structs.
///
/// This trait can be automatically implemented using the `#[derive(PolarsSchema)]` macro.
///
/// # Example
///
/// ```rust
/// use polars_schema_validate::PolarsSchema;
/// use polars::prelude::*;
///
/// #[derive(PolarsSchema)]
/// struct Person {
///     id: i32,
///     name: String,
///     age: i32,
/// }
///
/// let df = df![
///     "id" => [1, 2, 3],
///     "name" => ["Alice", "Bob", "Charlie"],
///     "age" => [30, 25, 35],
/// ].unwrap();
///
/// assert!(Person::validate(&df).is_ok());
/// ```
pub trait PolarsSchema {
    /// Returns the expected schema as a vector of (column_name, data_type) pairs.
    fn schema() -> Vec<(&'static str, DataType)>;
    
    /// Validates a DataFrame against the struct's schema.
    ///
    /// # Arguments
    /// * `df` - The DataFrame to validate
    ///
    /// # Returns
    /// * `Ok(())` if the DataFrame matches the schema
    /// * `Err(ValidationError)` with details about the mismatch
    fn validate(df: &DataFrame) -> Result<()> {
        let df_schema = df.schema();
        let expected_schema = Self::schema();
        
        for (name, expected_type) in expected_schema {
            match df_schema.get(name) {
                None => return Err(ValidationError::MissingColumn {
                    column_name: name.to_string(),
                }),
                Some(actual_type) => {
                    if actual_type != &expected_type {
                        return Err(ValidationError::TypeMismatch {
                            column_name: name.to_string(),
                            expected_type: format!("{:?}", expected_type),
                            actual_type: format!("{:?}", actual_type),
                        });
                    }
                }
            }
        }
        Ok(())
    }
    
    /// Validates a DataFrame against the struct's schema in strict mode.
    /// 
    /// In strict mode, the DataFrame must have exactly the same columns as the schema,
    /// no more, no less.
    ///
    /// # Arguments
    /// * `df` - The DataFrame to validate
    ///
    /// # Returns
    /// * `Ok(())` if the DataFrame exactly matches the schema
    /// * `Err(ValidationError)` with details about the mismatch
    fn validate_strict(df: &DataFrame) -> Result<()> {
        let df_schema = df.schema();
        let expected_schema = Self::schema();
        
        // Check column count
        if df_schema.len() != expected_schema.len() {
            return Err(ValidationError::ColumnCountMismatch {
                expected_count: expected_schema.len(),
                actual_count: df_schema.len(),
            });
        }
        
        // Validate all expected columns exist with correct types
        for (name, expected_type) in &expected_schema {
            match df_schema.get(*name) {
                None => return Err(ValidationError::MissingColumn {
                    column_name: name.to_string(),
                }),
                Some(actual_type) => {
                    if actual_type != expected_type {
                        return Err(ValidationError::TypeMismatch {
                            column_name: name.to_string(),
                            expected_type: format!("{:?}", expected_type),
                            actual_type: format!("{:?}", actual_type),
                        });
                    }
                }
            }
        }
        
        // Check for unexpected columns
        let expected_names: std::collections::HashSet<_> = 
            expected_schema.iter().map(|(name, _)| *name).collect();
        
        for (col_name, _) in df_schema.iter() {
            if !expected_names.contains(col_name.as_str()) {
                return Err(ValidationError::UnexpectedColumn {
                    column_name: col_name.to_string(),
                });
            }
        }
        
        Ok(())
    }
}