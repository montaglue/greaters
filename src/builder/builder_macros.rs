/// This macro is used to implement the `AppendableBuilder` trait for a primitive builder types.
#[macro_export]
macro_rules! impl_builder_append {
    ($builder:ty, $scalar:ident) => {
        impl AppendableBuilder for $builder {
            fn append_scalar_value(&mut self, value: ScalarValue) -> Result<()> {
                match value {
                    ScalarValue::$scalar(Some(v)) => self.append_value(v),
                    ScalarValue::$scalar(None) => self.append_null(),
                    ScalarValue::Null => self.append_null(),
                    _ => {
                        return Err(datafusion::error::DataFusionError::Internal(format!(
                            "Invalid ScalarValue for {}",
                            stringify!($builder)
                        )))
                    }
                }
                Ok(())
            }
        }
    };
}
