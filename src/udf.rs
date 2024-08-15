use datafusion::error::Result;
use datafusion::logical_expr::ColumnarValue;
use datafusion::{
    arrow::datatypes::DataType,
    logical_expr::{ScalarUDFImpl, Signature, TypeSignature, Volatility},
};

use crate::greatest::{greatest, validate_args_types};

/// GreatestUDF is a user-defined function that analogues to the greatest() function in PySpark.
#[derive(Debug)]
pub struct GreatestUDF;

impl ScalarUDFImpl for GreatestUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "greatest"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &Signature {
            type_signature: TypeSignature::VariadicAny,
            volatility: Volatility::Immutable,
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        validate_args_types(arg_types)?;
        Ok(arg_types[0].clone())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        greatest(args).map(ColumnarValue::Array)
    }
}
