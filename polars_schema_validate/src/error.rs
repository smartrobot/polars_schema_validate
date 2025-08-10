use std::fmt;

/// Error types that can occur during schema validation
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationError {
    /// A required column was not found in the DataFrame
    MissingColumn {
        column_name: String,
    },
    /// A column has an incorrect data type
    TypeMismatch {
        column_name: String,
        expected_type: String,
        actual_type: String,
    },
    /// DataFrame has incorrect number of columns (strict mode only)
    ColumnCountMismatch {
        expected_count: usize,
        actual_count: usize,
    },
    /// DataFrame has unexpected columns (strict mode only)
    UnexpectedColumn {
        column_name: String,
    },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::MissingColumn { column_name } => {
                write!(f, "Column '{}' not found in DataFrame", column_name)
            }
            ValidationError::TypeMismatch { column_name, expected_type, actual_type } => {
                write!(f, "Column '{}' has type {} but expected {}", column_name, actual_type, expected_type)
            }
            ValidationError::ColumnCountMismatch { expected_count, actual_count } => {
                write!(f, "Column count mismatch: DataFrame has {} columns but schema expects {}", actual_count, expected_count)
            }
            ValidationError::UnexpectedColumn { column_name } => {
                write!(f, "Unexpected column '{}' found in DataFrame", column_name)
            }
        }
    }
}

impl std::error::Error for ValidationError {}

// ValidationError is automatically Send + Sync because:
// - String is Send + Sync  
// - usize is Send + Sync (Copy types are automatically thread-safe)
// - No raw pointers, references, or non-thread-safe types

pub type Result<T> = std::result::Result<T, ValidationError>;

#[cfg(test)]
mod tests {
    use super::*;
    
    // Compile-time test that ValidationError is thread-safe
    #[test]
    fn test_thread_safety() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        
        assert_send::<ValidationError>();
        assert_sync::<ValidationError>();
        assert_send::<Result<()>>();
        assert_sync::<Result<()>>();
    }
    
    #[test]
    fn test_error_can_be_sent_across_threads() {
        use std::thread;
        
        let error = ValidationError::MissingColumn {
            column_name: "test_column".to_string(),
        };
        
        let handle = thread::spawn(move || {
            // Use the error in another thread
            format!("{}", error)
        });
        
        let result = handle.join().unwrap();
        assert_eq!(result, "Column 'test_column' not found in DataFrame");
    }
}