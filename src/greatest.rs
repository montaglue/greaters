use datafusion::error::Result;
use datafusion::{
    arrow::{array::ArrayRef, datatypes::DataType},
    logical_expr::ColumnarValue,
    scalar::ScalarValue,
};

use crate::builder::{make_builder, AppendableBuilder};

/// arguments validation to match behavior of greatest() in pyspark
pub fn validate_args(args: &[ColumnarValue]) -> Result<()> {
    if args.len() < 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "greatest() requires at least two argument".to_string(),
        ));
    }

    Ok(())
}

/// arguments validation to match behavior of greatest() in pyspark
pub fn validate_args_types(types: &[DataType]) -> Result<()> {
    if types.len() < 2 {
        return Err(datafusion::error::DataFusionError::Execution(
            "greatest() requires at least two argument".to_string(),
        ));
    }
    if types.windows(2).any(|w| w[0] != w[1]) {
        return Err(datafusion::error::DataFusionError::Execution(
            "greatest() requires all arguments to have the same type".to_string(),
        ));
    }

    Ok(())
}

/// algorithm of finding the greatest in each row with abstract appendable builder
fn greatest_with_builder<T: AppendableBuilder>(
    args: &[ArrayRef],
    mut builder: T,
) -> Result<ArrayRef> {
    let rows = args[0].len();

    for i in 0..rows {
        let mut max = ScalarValue::Null;
        for arg in args {
            let value = ScalarValue::try_from_array(arg, i)?;
            if max.is_null() || max < value {
                max = value;
            }
        }
        builder.append_scalar_value(max)?;
    }

    Ok(builder.finish())
}

/// greatest function implementation. Type validation is performed only when it runs as a scalar user-defined function.
pub fn greatest(args: &[ColumnarValue]) -> Result<ArrayRef> {
    validate_args(args)?;
    let args = ColumnarValue::values_to_arrays(args)?;
    let typ = args[0].data_type();
    let builder = make_builder(typ, args[0].len())?;
    greatest_with_builder(&args, builder)
}
