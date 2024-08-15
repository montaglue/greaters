mod builder_macros;
use datafusion::arrow::array::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Date64Builder, Decimal128Builder,
    Decimal256Builder, DurationMicrosecondBuilder, DurationMillisecondBuilder,
    DurationNanosecondBuilder, DurationSecondBuilder, FixedSizeBinaryBuilder, FixedSizeListBuilder,
    Float16Builder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, IntervalDayTimeBuilder, IntervalMonthDayNanoBuilder, IntervalYearMonthBuilder,
    LargeBinaryBuilder, LargeListBuilder, LargeStringBuilder, ListBuilder, NullBuilder,
    StringBuilder, Time32MillisecondBuilder, Time32SecondBuilder, Time64MicrosecondBuilder,
    Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder, UInt16Builder, UInt32Builder,
    UInt64Builder, UInt8Builder,
};
use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::error::Result;
use datafusion::{arrow::array::ArrayBuilder, scalar::ScalarValue};

use crate::impl_builder_append;

/// All builders implement append function, but the general Builder trait doesn't have it.
/// Because we need to abstract over the different builders, I extended ArrayBuilder with this method.
pub trait AppendableBuilder: ArrayBuilder {
    fn append_scalar_value(&mut self, value: ScalarValue) -> Result<()>;
}

/// Returns a appendable builder with capacity `capacity` that corresponds to the datatype `DataType`
/// This function is useful to construct arrays from an arbitrary vectors with known/expected
/// schema.
///
/// This method is analog of datafuse method https://docs.rs/datafusion/latest/datafusion/common/arrow/array/fn.make_builder.html
pub fn make_builder(datatype: &DataType, capacity: usize) -> Result<Box<dyn AppendableBuilder>> {
    use crate::builder::*;
    Ok(match datatype {
        DataType::Null => Box::new(NullBuilder::new()),
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Int8 => Box::new(Int8Builder::with_capacity(capacity)),
        DataType::Int16 => Box::new(Int16Builder::with_capacity(capacity)),
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::UInt8 => Box::new(UInt8Builder::with_capacity(capacity)),
        DataType::UInt16 => Box::new(UInt16Builder::with_capacity(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
        DataType::Float16 => Box::new(Float16Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, 1024)),
        DataType::LargeBinary => Box::new(LargeBinaryBuilder::with_capacity(capacity, 1024)),
        DataType::FixedSizeBinary(len) => {
            Box::new(FixedSizeBinaryBuilder::with_capacity(capacity, *len))
        }

        DataType::Decimal128(p, s) => Box::new(
            Decimal128Builder::with_capacity(capacity).with_data_type(DataType::Decimal128(*p, *s)),
        ),
        DataType::Decimal256(p, s) => Box::new(
            Decimal256Builder::with_capacity(capacity).with_data_type(DataType::Decimal256(*p, *s)),
        ),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, 1024)),
        DataType::LargeUtf8 => Box::new(LargeStringBuilder::with_capacity(capacity, 1024)),
        DataType::Date32 => Box::new(Date32Builder::with_capacity(capacity)),
        DataType::Date64 => Box::new(Date64Builder::with_capacity(capacity)),
        DataType::Time32(TimeUnit::Second) => {
            Box::new(Time32SecondBuilder::with_capacity(capacity))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            Box::new(Time32MillisecondBuilder::with_capacity(capacity))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Box::new(Time64MicrosecondBuilder::with_capacity(capacity))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Box::new(Time64NanosecondBuilder::with_capacity(capacity))
        }
        DataType::Timestamp(TimeUnit::Second, tz) => Box::new(
            TimestampSecondBuilder::with_capacity(capacity)
                .with_data_type(DataType::Timestamp(TimeUnit::Second, tz.clone())),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, tz) => Box::new(
            TimestampMillisecondBuilder::with_capacity(capacity)
                .with_data_type(DataType::Timestamp(TimeUnit::Millisecond, tz.clone())),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => Box::new(
            TimestampMicrosecondBuilder::with_capacity(capacity)
                .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone())),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => Box::new(
            TimestampNanosecondBuilder::with_capacity(capacity)
                .with_data_type(DataType::Timestamp(TimeUnit::Nanosecond, tz.clone())),
        ),
        DataType::Interval(IntervalUnit::YearMonth) => {
            Box::new(IntervalYearMonthBuilder::with_capacity(capacity))
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            Box::new(IntervalDayTimeBuilder::with_capacity(capacity))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            Box::new(IntervalMonthDayNanoBuilder::with_capacity(capacity))
        }
        DataType::Duration(TimeUnit::Second) => {
            Box::new(DurationSecondBuilder::with_capacity(capacity))
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            Box::new(DurationMillisecondBuilder::with_capacity(capacity))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Box::new(DurationMicrosecondBuilder::with_capacity(capacity))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Box::new(DurationNanosecondBuilder::with_capacity(capacity))
        }
        DataType::List(field) => {
            let builder = make_builder(field.data_type(), capacity)?;
            Box::new(ListBuilder::with_capacity(builder, capacity).with_field(field.clone()))
        }
        DataType::LargeList(field) => {
            let builder = make_builder(field.data_type(), capacity)?;
            Box::new(LargeListBuilder::with_capacity(builder, capacity).with_field(field.clone()))
        }
        DataType::FixedSizeList(field, len) => {
            let builder = make_builder(field.data_type(), capacity)?;
            Box::new(
                FixedSizeListBuilder::with_capacity(builder, *len, capacity)
                    .with_field(field.clone()),
            )
        }

        t => {
            return Err(datafusion::error::DataFusionError::Execution(format!(
                "Data type {t:?} is not currently supported"
            )))
        }
    })
}

impl AppendableBuilder for NullBuilder {
    fn append_scalar_value(&mut self, _: ScalarValue) -> Result<()> {
        self.append_null();
        Ok(())
    }
}

impl AppendableBuilder for FixedSizeBinaryBuilder {
    fn append_scalar_value(&mut self, value: ScalarValue) -> Result<()> {
        match value {
            ScalarValue::FixedSizeBinary(_bin, Some(value)) => {
                self.append_value(value)?;
            }
            ScalarValue::FixedSizeBinary(_bin, None) => {
                self.append_null();
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Invalid scalar value for FixedSizeBinaryBuilder".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl AppendableBuilder for Decimal128Builder {
    fn append_scalar_value(&mut self, value: ScalarValue) -> Result<()> {
        match value {
            ScalarValue::Decimal128(Some(value), _p, _s) => {
                self.append_value(value);
            }
            ScalarValue::Decimal128(None, _p, _s) => {
                self.append_null();
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Invalid scalar value for Decimal128Builder".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl AppendableBuilder for Decimal256Builder {
    fn append_scalar_value(&mut self, value: ScalarValue) -> Result<()> {
        match value {
            ScalarValue::Decimal256(Some(value), _p, _s) => {
                self.append_value(value);
            }
            ScalarValue::Decimal256(None, _p, _s) => {
                self.append_null();
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Invalid scalar value for Decimal256Builder".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl<T: AppendableBuilder> AppendableBuilder for ListBuilder<T> {
    fn append_scalar_value(&mut self, value: ScalarValue) -> Result<()> {
        match value {
            ScalarValue::List(list) => {
                //TODO: assert that the array have exectlly one value
                let list = list.value(0);
                let len = list.len();

                for i in 0..len {
                    let value = ScalarValue::try_from_array(&list, i)?;
                    self.values().append_scalar_value(value)?;
                }

                self.append(true);
            }

            _ => {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Invalid scalar value for ListBuilder".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl<T: AppendableBuilder> AppendableBuilder for FixedSizeListBuilder<T> {
    fn append_scalar_value(&mut self, value: ScalarValue) -> Result<()> {
        match value {
            ScalarValue::FixedSizeList(list) => {
                // TODO: assert that the array have exectlly one value
                let list = list.value(0);
                let len = list.len();

                for i in 0..len {
                    let value = ScalarValue::try_from_array(&list, i)?;
                    self.values().append_scalar_value(value)?;
                }

                self.append(true);
            }

            _ => {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Invalid scalar value for FixedSizeListBuilder".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl<T: AppendableBuilder> AppendableBuilder for LargeListBuilder<T> {
    fn append_scalar_value(&mut self, value: ScalarValue) -> Result<()> {
        match value {
            ScalarValue::LargeList(list) => {
                let list = list.value(0);
                let len = list.len();

                for i in 0..len {
                    let value = ScalarValue::try_from_array(&list, i)?;
                    self.values().append_scalar_value(value)?;
                }

                self.append(true);
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Invalid scalar value for LargeListBuilder".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl ArrayBuilder for Box<dyn AppendableBuilder> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn finish(&mut self) -> datafusion::arrow::array::ArrayRef {
        self.as_mut().finish()
    }

    fn finish_cloned(&self) -> datafusion::arrow::array::ArrayRef {
        self.as_ref().finish_cloned()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.as_ref().as_any()
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self.as_mut().as_any_mut()
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

impl AppendableBuilder for Box<dyn AppendableBuilder> {
    fn append_scalar_value(&mut self, value: ScalarValue) -> Result<()> {
        self.as_mut().append_scalar_value(value)
    }
}

impl AppendableBuilder for TimestampMicrosecondBuilder {
    fn append_scalar_value(&mut self, value: ScalarValue) -> Result<()> {
        match value {
            ScalarValue::TimestampMicrosecond(Some(value), _) => {
                self.append_value(value);
            }
            ScalarValue::TimestampMicrosecond(None, _) => {
                self.append_null();
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Invalid scalar value for TimestampMicrosecondBuilder".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl AppendableBuilder for TimestampMillisecondBuilder {
    fn append_scalar_value(&mut self, value: ScalarValue) -> Result<()> {
        match value {
            ScalarValue::TimestampMillisecond(Some(value), _) => {
                self.append_value(value);
            }
            ScalarValue::TimestampMillisecond(None, _) => {
                self.append_null();
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Invalid scalar value for TimestampMillisecondBuilder".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl AppendableBuilder for TimestampNanosecondBuilder {
    fn append_scalar_value(&mut self, value: ScalarValue) -> Result<()> {
        match value {
            ScalarValue::TimestampNanosecond(Some(value), _) => {
                self.append_value(value);
            }
            ScalarValue::TimestampNanosecond(None, _) => {
                self.append_null();
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Invalid scalar value for TimestampNanosecondBuilder".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl AppendableBuilder for TimestampSecondBuilder {
    fn append_scalar_value(&mut self, value: ScalarValue) -> Result<()> {
        match value {
            ScalarValue::TimestampSecond(Some(value), _) => {
                self.append_value(value);
            }
            ScalarValue::TimestampSecond(None, _) => {
                self.append_null();
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Invalid scalar value for TimestampSecondBuilder".to_string(),
                ));
            }
        }
        Ok(())
    }
}

impl_builder_append!(datafusion::arrow::array::BooleanBuilder, Boolean);

impl_builder_append!(datafusion::arrow::array::Float16Builder, Float16);

impl_builder_append!(datafusion::arrow::array::Float32Builder, Float32);

impl_builder_append!(datafusion::arrow::array::Float64Builder, Float64);

impl_builder_append!(datafusion::arrow::array::Int8Builder, Int8);

impl_builder_append!(datafusion::arrow::array::Int16Builder, Int16);

impl_builder_append!(datafusion::arrow::array::Int32Builder, Int32);

impl_builder_append!(datafusion::arrow::array::Int64Builder, Int64);

impl_builder_append!(datafusion::arrow::array::UInt8Builder, UInt8);

impl_builder_append!(datafusion::arrow::array::UInt16Builder, UInt16);

impl_builder_append!(datafusion::arrow::array::UInt32Builder, UInt32);

impl_builder_append!(datafusion::arrow::array::UInt64Builder, UInt64);

impl_builder_append!(datafusion::arrow::array::StringBuilder, Utf8);

impl_builder_append!(datafusion::arrow::array::LargeStringBuilder, LargeUtf8);

impl_builder_append!(datafusion::arrow::array::BinaryBuilder, Binary);

impl_builder_append!(datafusion::arrow::array::LargeBinaryBuilder, LargeBinary);

impl_builder_append!(datafusion::arrow::array::Date32Builder, Date32);

impl_builder_append!(datafusion::arrow::array::Date64Builder, Date64);

impl_builder_append!(datafusion::arrow::array::Time32SecondBuilder, Time32Second);

impl_builder_append!(
    datafusion::arrow::array::Time32MillisecondBuilder,
    Time32Millisecond
);

impl_builder_append!(
    datafusion::arrow::array::Time64MicrosecondBuilder,
    Time64Microsecond
);

impl_builder_append!(
    datafusion::arrow::array::Time64NanosecondBuilder,
    Time64Nanosecond
);

impl_builder_append!(
    datafusion::arrow::array::IntervalYearMonthBuilder,
    IntervalYearMonth
);

impl_builder_append!(
    datafusion::arrow::array::IntervalMonthDayNanoBuilder,
    IntervalMonthDayNano
);

impl_builder_append!(
    datafusion::arrow::array::IntervalDayTimeBuilder,
    IntervalDayTime
);

impl_builder_append!(
    datafusion::arrow::array::DurationSecondBuilder,
    DurationSecond
);

impl_builder_append!(
    datafusion::arrow::array::DurationMillisecondBuilder,
    DurationMillisecond
);

impl_builder_append!(
    datafusion::arrow::array::DurationMicrosecondBuilder,
    DurationMicrosecond
);

impl_builder_append!(
    datafusion::arrow::array::DurationNanosecondBuilder,
    DurationNanosecond
);
