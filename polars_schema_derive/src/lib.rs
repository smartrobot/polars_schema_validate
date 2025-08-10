use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Type};

#[proc_macro_derive(PolarsSchema)]
pub fn derive_polars_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => panic!("PolarsSchema only supports structs with named fields"),
        },
        _ => panic!("PolarsSchema can only be derived for structs"),
    };
    
    let schema_entries = fields.iter().map(|field| {
        let field_name = field.ident.as_ref().unwrap().to_string();
        let field_type = &field.ty;
        let dtype = type_to_polars_dtype(field_type);
        
        quote! {
            (#field_name, #dtype)
        }
    });
    
    let expanded = quote! {
        impl PolarsSchema for #name {
            fn schema() -> Vec<(&'static str, ::polars::prelude::DataType)> {
                vec![
                    #(#schema_entries),*
                ]
            }
        }
    };
    
    TokenStream::from(expanded)
}

fn type_to_polars_dtype(ty: &Type) -> proc_macro2::TokenStream {
    let type_str = quote!(#ty).to_string();
    
    match type_str.as_str() {
        "i8" => quote!(::polars::prelude::DataType::Int8),
        "i16" => quote!(::polars::prelude::DataType::Int16),
        "i32" => quote!(::polars::prelude::DataType::Int32),
        "i64" => quote!(::polars::prelude::DataType::Int64),
        "u8" => quote!(::polars::prelude::DataType::UInt8),
        "u16" => quote!(::polars::prelude::DataType::UInt16),
        "u32" => quote!(::polars::prelude::DataType::UInt32),
        "u64" => quote!(::polars::prelude::DataType::UInt64),
        "f32" => quote!(::polars::prelude::DataType::Float32),
        "f64" => quote!(::polars::prelude::DataType::Float64),
        "bool" => quote!(::polars::prelude::DataType::Boolean),
        "String" => quote!(::polars::prelude::DataType::String),
        "& str" | "&str" => quote!(::polars::prelude::DataType::String),
        
        // Chrono types support
        #[cfg(feature = "chrono")]
        "NaiveDate" | "chrono :: NaiveDate" => {
            quote!(::polars::prelude::DataType::Date)
        }
        #[cfg(feature = "chrono")]
        "NaiveDateTime" | "chrono :: NaiveDateTime" => {
            quote!(::polars::prelude::DataType::Datetime(::polars::prelude::TimeUnit::Microseconds, None))
        }
        #[cfg(feature = "chrono")]
        "NaiveTime" | "chrono :: NaiveTime" => {
            quote!(::polars::prelude::DataType::Time)
        }
        #[cfg(feature = "chrono")]
        s if s.contains("DateTime") && s.contains("Utc") => {
            quote!(::polars::prelude::DataType::Datetime(::polars::prelude::TimeUnit::Microseconds, Some("UTC".into())))
        }
        
        s if s.starts_with("Option <") => {
            let inner = s.trim_start_matches("Option <").trim_end_matches('>').trim();
            type_to_polars_dtype(&syn::parse_str::<Type>(inner).unwrap())
        }
        _ => quote!(::polars::prelude::DataType::String),
    }
}