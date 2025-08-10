use polars::prelude::*;
use polars_schema_validate::PolarsSchema;

#[derive(Debug, PolarsSchema)]
#[allow(dead_code)]
struct Person {
    id: i32,
    name: String,
    age: i32,
    email: String,
    salary: f64,
    is_active: bool,
}

#[derive(Debug, PolarsSchema)]
#[allow(dead_code)]
struct Product {
    product_id: u32,
    product_name: String,
    price: f64,
    in_stock: bool,
    category: String,
}

#[derive(Debug, PolarsSchema)]
#[allow(dead_code)]
struct Transaction {
    transaction_id: i64,
    amount: f64,
    customer_name: String,
    is_completed: bool,
}

#[test]
fn test_person_valid_dataframe() {
    let df = df![
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
        "age" => [30, 25, 35],
        "email" => ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "salary" => [75000.0, 65000.0, 85000.0],
        "is_active" => [true, true, false],
    ].unwrap();

    assert!(Person::validate(&df).is_ok());
}

#[test]
fn test_person_invalid_type() {
    let df = df![
        "id" => ["1", "2", "3"], // Wrong type: String instead of Int32
        "name" => ["Alice", "Bob", "Charlie"],
        "age" => [30, 25, 35],
        "email" => ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "salary" => [75000.0, 65000.0, 85000.0],
        "is_active" => [true, true, false],
    ].unwrap();

    let result = Person::validate(&df);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("type"));
}

#[test]
fn test_person_missing_column() {
    let df = df![
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
        "age" => [30, 25, 35],
        // Missing email, salary, is_active
    ].unwrap();

    let result = Person::validate(&df);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
}

#[test]
fn test_product_valid_dataframe() {
    let df = df![
        "product_id" => [101u32, 102u32, 103u32],
        "product_name" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [999.99, 25.50, 79.99],
        "in_stock" => [true, true, false],
        "category" => ["Electronics", "Accessories", "Accessories"],
    ].unwrap();

    assert!(Product::validate(&df).is_ok());
}

#[test]
fn test_product_wrong_numeric_type() {
    let df = df![
        "product_id" => [101i32, 102i32, 103i32], // Wrong: i32 instead of u32
        "product_name" => ["Laptop", "Mouse", "Keyboard"],
        "price" => [999.99, 25.50, 79.99],
        "in_stock" => [true, true, false],
        "category" => ["Electronics", "Accessories", "Accessories"],
    ].unwrap();

    let result = Product::validate(&df);
    assert!(result.is_err());
}

#[test]
fn test_transaction_valid() {
    let df = df![
        "transaction_id" => [1001i64, 1002i64, 1003i64],
        "amount" => [150.50, 299.99, 45.00],
        "customer_name" => ["Alice", "Bob", "Charlie"],
        "is_completed" => [true, false, true],
    ].unwrap();

    assert!(Transaction::validate(&df).is_ok());
}

#[test]
fn test_strict_mode_extra_columns() {
    let df = df![
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
        "age" => [30, 25, 35],
        "email" => ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "salary" => [75000.0, 65000.0, 85000.0],
        "is_active" => [true, true, false],
        "extra_field" => ["x", "y", "z"], // Extra column
    ].unwrap();

    // Non-strict should pass
    assert!(Person::validate(&df).is_ok());
    
    // Strict should fail
    let result = Person::validate_strict(&df);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Column count mismatch"));
}

#[test]
fn test_strict_mode_exact_match() {
    let df = df![
        "id" => [1, 2, 3],
        "name" => ["Alice", "Bob", "Charlie"],
        "age" => [30, 25, 35],
        "email" => ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "salary" => [75000.0, 65000.0, 85000.0],
        "is_active" => [true, true, false],
    ].unwrap();

    // Strict mode should pass with exact match
    assert!(Person::validate_strict(&df).is_ok());
}

#[test]
fn test_schema_generation() {
    let schema = Person::schema();
    assert_eq!(schema.len(), 6);
    
    let field_names: Vec<_> = schema.iter().map(|(name, _)| *name).collect();
    assert!(field_names.contains(&"id"));
    assert!(field_names.contains(&"name"));
    assert!(field_names.contains(&"age"));
    assert!(field_names.contains(&"email"));
    assert!(field_names.contains(&"salary"));
    assert!(field_names.contains(&"is_active"));
}

#[test]
fn test_different_numeric_types() {
    #[derive(Debug, PolarsSchema)]
    #[allow(dead_code)]
    struct NumericTypes {
        val_i8: i8,
        val_i16: i16,
        val_i32: i32,
        val_i64: i64,
        val_u8: i32,  // Use i32 for testing since polars doesn't directly support u8 Series
        val_u16: i32, // Use i32 for testing since polars doesn't directly support u16 Series
        val_u32: u32,
        val_u64: u64,
        val_f32: f32,
        val_f64: f64,
    }

    let val_i8_col = Series::new("val_i8".into(), [1i8, 2i8]);
    let val_i16_col = Series::new("val_i16".into(), [1i16, 2i16]);
    let val_i32_col = Series::new("val_i32".into(), [1i32, 2i32]);
    let val_i64_col = Series::new("val_i64".into(), [1i64, 2i64]);
    // Skip u8 and u16 for now as they're not directly supported by polars Series creation
    // We can test them by converting larger integers
    let val_u8_col = Series::new("val_u8".into(), [1i32, 2i32]);
    let val_u16_col = Series::new("val_u16".into(), [1i32, 2i32]);
    let val_u32_col = Series::new("val_u32".into(), [1u32, 2u32]);
    let val_u64_col = Series::new("val_u64".into(), [1u64, 2u64]);
    let val_f32_col = Series::new("val_f32".into(), [1.0f32, 2.0f32]);
    let val_f64_col = Series::new("val_f64".into(), [1.0f64, 2.0f64]);
    
    let df = DataFrame::new(vec![
        val_i8_col.into(),
        val_i16_col.into(),
        val_i32_col.into(),
        val_i64_col.into(),
        val_u8_col.into(),
        val_u16_col.into(),
        val_u32_col.into(),
        val_u64_col.into(),
        val_f32_col.into(),
        val_f64_col.into(),
    ]).unwrap();

    assert!(NumericTypes::validate(&df).is_ok());
}