# polars_schema_validate

A Rust library for validating [Polars](https://github.com/pola-rs/polars) DataFrame schemas using derive macros. Define your expected schema as a Rust struct and automatically validate DataFrames against it.

## Features

- 🎯 **Type-safe schema validation** - Use Rust structs to define expected DataFrame schemas
- 🚀 **Zero boilerplate** - Automatic schema generation via `#[derive(PolarsSchema)]`
- 🔍 **Flexible validation** - Support for both strict and non-strict validation modes
- 🦀 **Idiomatic Rust** - Leverages Rust's type system and derive macros
- ⚡ **Compile-time safety** - Catch schema mismatches at compile time where possible

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
polars_schema_validate = "0.1.0"
polars = "0.44"
```

## Quick Start

```rust
use polars::prelude::*;
use polars_schema_validate::{PolarsSchema, Result};

// Define your schema as a Rust struct
#[derive(PolarsSchema)]
struct Person {
    id: i32,
    name: String,
    age: i32,
    email: String,
    salary: f64,
    is_active: bool,
}

fn main() -> Result<()> {
    // Create a DataFrame
    let df = df![
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
        "age" => [30, 25, 35],
        "email" => ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "salary" => [75000.0, 65000.0, 85000.0],
        "is_active" => [true, true, false],
    ].unwrap();

    // Validate the DataFrame against the schema
    Person::validate(&df)?;
    println!("DataFrame is valid!");

    Ok(())
}
```

## Validation Modes

### Standard Validation

Checks that all required columns exist with the correct types. Extra columns are allowed.

```rust
Person::validate(&df)?;  // Allows extra columns
```

### Strict Validation

Ensures the DataFrame has exactly the columns defined in the schema - no more, no less.

```rust
Person::validate_strict(&df)?;  // Exact schema match required
```

## Supported Types

The derive macro automatically maps Rust types to Polars DataTypes:

### Basic Types
| Rust Type | Polars DataType |
|-----------|-----------------|
| `i8`      | `Int8`          |
| `i16`     | `Int16`         |
| `i32`     | `Int32`         |
| `i64`     | `Int64`         |
| `u8`      | `UInt8`         |
| `u16`     | `UInt16`        |
| `u32`     | `UInt32`        |
| `u64`     | `UInt64`        |
| `f32`     | `Float32`       |
| `f64`     | `Float64`       |
| `bool`    | `Boolean`       |
| `String`  | `String`        |
| `&str`    | `String`        |

### Temporal Types (with `chrono` feature)
| Rust Type | Polars DataType |
|-----------|-----------------|
| `chrono::NaiveDate` | `Date` |
| `chrono::NaiveDateTime` | `Datetime(Microseconds, None)` |
| `chrono::NaiveTime` | `Time` |
| `chrono::DateTime<Utc>` | `Datetime(Microseconds, Some("UTC"))` |

### Optional Types
| Rust Type | Polars DataType |
|-----------|-----------------|
| `Option<T>` | Same as `T` but nullable |

### Features

Temporal type support is enabled by default. To use chrono types, add:

```toml
[dependencies]
polars_schema_validate = "0.1.0"  # chrono feature enabled by default
chrono = "0.4"
```

To disable chrono support:

```toml
[dependencies]
polars_schema_validate = { version = "0.1.0", default-features = false }
```

## Error Handling

The library provides structured error handling with the `ValidationError` enum:

```rust
use polars_schema_validate::{PolarsSchema, ValidationError};

match Person::validate(&df) {
    Ok(()) => println!("Valid!"),
    Err(ValidationError::MissingColumn { column_name }) => {
        println!("Missing column: {}", column_name);
    }
    Err(ValidationError::TypeMismatch { column_name, expected_type, actual_type }) => {
        println!("Type mismatch in '{}': expected {}, got {}", column_name, expected_type, actual_type);
    }
    Err(e) => println!("Validation failed: {}", e),
}
```

### Error Types

- `ValidationError::MissingColumn` - A required column is missing
- `ValidationError::TypeMismatch` - A column has the wrong data type  
- `ValidationError::ColumnCountMismatch` - Wrong number of columns (strict mode)
- `ValidationError::UnexpectedColumn` - Extra column found (strict mode)

All errors implement `Display` for user-friendly messages and `std::error::Error` for compatibility.

## Advanced Examples

### Basic Validation

```rust
use polars::prelude::*;
use polars_schema_validate::{PolarsSchema, Result};

#[derive(PolarsSchema)]
struct Transaction {
    transaction_id: i64,
    amount: f64,
    customer_name: String,
    is_completed: bool,
}

fn process_transactions(df: DataFrame) -> Result<()> {
    // Validate before processing
    Transaction::validate(&df)?;
    
    // Now we know the DataFrame has the expected schema
    // Process the data safely...
    println!("Processing {} valid transactions", df.height());
    
    Ok(())
}
```

### Temporal Data Validation

```rust
use polars::prelude::*;
use polars_schema_validate::{PolarsSchema, Result};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

#[derive(PolarsSchema)]
struct Event {
    id: i32,
    name: String,
    event_date: NaiveDate,
    event_datetime: NaiveDateTime,
    event_time: NaiveTime,
}

fn process_events(df: DataFrame) -> Result<()> {
    // Validate temporal columns are correctly typed
    Event::validate(&df)?;
    
    println!("All temporal columns are valid!");
    Ok(())
}
```

## API Reference

### Trait: `PolarsSchema`

The main trait that provides schema validation functionality.

#### Methods

- `fn schema() -> Vec<(&'static str, DataType)>` - Returns the expected schema
- `fn validate(df: &DataFrame) -> Result<()>` - Validates a DataFrame (allows extra columns)
- `fn validate_strict(df: &DataFrame) -> Result<()>` - Validates a DataFrame (exact match required)

### Derive Macro: `#[derive(PolarsSchema)]`

Automatically implements the `PolarsSchema` trait for your struct based on its fields.

## Development

### Running Tests

```bash
cargo test
```

### Building

```bash
cargo build --release
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

Built on top of the excellent [Polars](https://github.com/pola-rs/polars) DataFrame library.