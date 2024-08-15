mod builder;
mod greatest;
mod udf;

pub use builder::make_builder;
pub use builder::AppendableBuilder;
pub use greatest::greatest;
pub use greatest::validate_args;
pub use greatest::validate_args_types;
pub use udf::GreatestUDF;

mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        ArrayBuilder, ArrayRef, Float64Array, GenericListBuilder, Int64Array, Int64Builder,
    };
    use datafusion::arrow::datatypes::ArrowNativeType;
    use datafusion::arrow::ipc::ListBuilder;
    use datafusion::error::{DataFusionError, Result};

    use datafusion::logical_expr::{ColumnarValue, ScalarUDF};
    use datafusion::prelude::*;
    use udf::GreatestUDF;

    use super::*;

    fn create_array(data: Vec<Option<i64>>) -> ArrayRef {
        Arc::new(Int64Array::from(data))
    }

    fn create_column_values(data: Vec<Vec<Option<i64>>>) -> Vec<ColumnarValue> {
        data.into_iter()
            .map(|data| {
                let array = create_array(data);
                ColumnarValue::Array(array)
            })
            .collect()
    }

    #[test]
    fn test_greatest_basics() {
        let input = create_column_values(vec![vec![Some(100)], vec![Some(1)]]);

        let result = greatest(&input);
        assert!(result.is_ok());
        let result = result.unwrap();

        let answer = create_array(vec![Some(100)]);

        assert_eq!(&result, &answer);
    }

    #[test]
    fn test_greatest_basics2() {
        let input = create_column_values(vec![
            vec![Some(1), Some(4), Some(7)],
            vec![Some(2), Some(5), Some(8)],
            vec![Some(3), Some(6), Some(9)],
        ]);

        let result = greatest(&input);
        assert!(result.is_ok());
        let result = result.unwrap();

        let answer = create_array(vec![Some(3), Some(6), Some(9)]);

        assert_eq!(&result, &answer);
    }

    #[test]
    fn test_greatest_basics3() {
        let input = vec![vec![1.0, 50.0, 100.0], vec![2.0, 25.0, 100.0]];
        let input = input
            .into_iter()
            .map(|data| {
                let array = Arc::new(Float64Array::from(data));
                ColumnarValue::Array(array)
            })
            .collect::<Vec<_>>();

        let result = greatest(&input);
        assert!(result.is_ok());
        let result = result.unwrap();

        let answer = Arc::new(Float64Array::from(vec![2.0, 50.0, 100.0])) as ArrayRef;

        assert_eq!(&result, &answer);
    }

    #[test]
    fn test_greatest_args_validation1() {
        let input = create_column_values(vec![vec![Some(1), Some(4), Some(7)]]);

        let result = greatest(&input);
        assert!(result.is_err());
        let result = result.unwrap_err();

        assert!(matches!(result, DataFusionError::Execution(_)));

        let DataFusionError::Execution(msg) = result else {
            unreachable!();
        };

        let answer = "greatest() requires at least two argument".to_string();

        assert_eq!(msg, answer);
    }

    #[test]
    fn test_greatest_args_validation2() {
        let result = greatest(&[]);
        assert!(result.is_err());
        let result = result.unwrap_err();

        assert!(matches!(result, DataFusionError::Execution(_)));

        let DataFusionError::Execution(msg) = result else {
            unreachable!();
        };

        let answer = "greatest() requires at least two argument".to_string();

        assert_eq!(msg, answer);
    }

    #[test]
    fn test_greatest_nulls() {
        let input = create_column_values(vec![
            vec![None, Some(4), Some(7)],
            vec![Some(2), None, Some(8)],
            vec![Some(3), Some(6), None],
        ]);

        let result = greatest(&input);
        assert!(result.is_ok());
        let result = result.unwrap();

        let answer = create_array(vec![Some(3), Some(6), Some(8)]);

        assert_eq!(&result, &answer);
    }

    #[test]
    fn test_greatest_null_rows() {
        let input = create_column_values(vec![
            vec![None, Some(4), Some(7)],
            vec![Some(2), None, Some(8)],
            vec![None, None, None],
        ]);

        let result = greatest(&input);
        assert!(result.is_ok());
        let result = result.unwrap();

        let answer = create_array(vec![Some(2), Some(4), Some(8)]);

        assert_eq!(&result, &answer);
    }

    #[test]
    fn test_greatest_all_nulls() {
        let input = create_column_values(vec![
            vec![None, None, None],
            vec![None, None, None],
            vec![None, None, None],
        ]);

        let result = greatest(&input);
        assert!(result.is_ok());
        let result = result.unwrap();

        let answer = create_array(vec![None, None, None]);

        assert_eq!(&result, &answer);
    }

    #[test]
    fn test_greatest_lists() {
        let mut builder_column1 = GenericListBuilder::<i32, _>::new(Int64Builder::new());
        builder_column1.values().append_value(2);
        builder_column1.values().append_value(1);
        builder_column1.append(true);

        let mut builder_column2 = GenericListBuilder::<i32, _>::new(Int64Builder::new());
        builder_column2.values().append_value(1);
        builder_column2.values().append_value(4);
        builder_column2.append(true);

        let input = vec![
            ColumnarValue::Array(Arc::new(builder_column1.finish()) as ArrayRef),
            ColumnarValue::Array(Arc::new(builder_column2.finish()) as ArrayRef),
        ];

        let result = greatest(&input);
        assert!(result.is_ok());
        let result = result.unwrap();

        let mut answer_builder = GenericListBuilder::<i32, _>::new(Int64Builder::new());
        answer_builder.values().append_value(2);
        answer_builder.values().append_value(1);
        answer_builder.append(true);

        let answer = Arc::new(answer_builder.finish()) as ArrayRef;

        assert_eq!(&result, &answer);
    }

    #[tokio::test]
    async fn test_greatest_udf() -> Result<()> {
        let ctx = SessionContext::new();

        let greatest_udf = ScalarUDF::from(GreatestUDF);

        ctx.register_udf(greatest_udf.clone());

        let df = ctx
            .read_csv("tests/data/data.csv", CsvReadOptions::new())
            .await?;

        let result = df.select(vec![greatest_udf
            .call(vec![col("a"), col("b"), col("c")])
            .alias("greatest")])?;

        let result = result.collect().await?;
        let result = result[0].columns()[0].clone();

        let answer = create_array(vec![Some(3), Some(6), Some(9)]);
        assert_eq!(&result, &answer);

        Ok(())
    }

    #[tokio::test]
    async fn test_greatest_type_validation() -> Result<()> {
        let ctx = SessionContext::new();

        let greatest_udf = ScalarUDF::from(GreatestUDF);

        ctx.register_udf(greatest_udf.clone());

        let df = ctx
            .read_csv("tests/data/types.csv", CsvReadOptions::new())
            .await?;

        let result = df.select(vec![greatest_udf
            .call(vec![col("a"), col("b"), col("c")])
            .alias("greatest")]);

        assert!(result.is_err());
        let result = result.unwrap_err();

        assert!(matches!(result, DataFusionError::Execution(_)));
        let DataFusionError::Execution(msg) = result else {
            unreachable!();
        };

        let answer = "greatest() requires all arguments to have the same type".to_string();

        assert_eq!(&msg, &answer);

        Ok(())
    }
}
