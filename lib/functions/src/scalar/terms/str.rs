use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_plain_term_sparql_op_impl,
    create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpArgs, ScalarSparqlOpSignature, SparqlOpArity};
use datafusion::arrow::array::{Array, ArrayRef, StringArray, UInt8Array};
use datafusion::logical_expr::ColumnarValue;
use itertools::repeat_n;
use rdf_fusion_encoding::plain_term::{
    PlainTermArray, PlainTermArrayBuilder, PlainTermEncoding, PlainTermEncodingField,
    PlainTermType,
};
use rdf_fusion_encoding::typed_value::{TypedValueEncoding, TypedValueEncodingField};
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, EncodingScalar};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{AResult, ThinError};
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{SimpleLiteral, TypedValue, TypedValueRef};
use std::sync::Arc;
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::DataType;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct StrSparqlOp;

impl Default for StrSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Str);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(1))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            if let Some(result) = try_str_fast_path(&args)?{
                return Ok(result);
            }

            dispatch_unary_owned_typed_value(
                &args.args[0],
                |value| {
                    let converted = match value {
                        TypedValueRef::NamedNode(value) => value.as_str().to_owned(),
                        TypedValueRef::BlankNode(value) => value.as_str().to_owned(),
                        TypedValueRef::BooleanLiteral(value) => value.to_string(),
                        TypedValueRef::NumericLiteral(value) => value.format_value(),
                        TypedValueRef::SimpleLiteral(value) => value.value.to_owned(),
                        TypedValueRef::LanguageStringLiteral(value) => {
                            value.value.to_owned()
                        }
                        TypedValueRef::DateTimeLiteral(value) => value.to_string(),
                        TypedValueRef::TimeLiteral(value) => value.to_string(),
                        TypedValueRef::DateLiteral(value) => value.to_string(),
                        TypedValueRef::DurationLiteral(value) => value.to_string(),
                        TypedValueRef::YearMonthDurationLiteral(value) => {
                            value.to_string()
                        }
                        TypedValueRef::DayTimeDurationLiteral(value) => value.to_string(),
                        TypedValueRef::OtherLiteral(value) => value.value().to_owned(),
                    };
                    Ok(TypedValue::SimpleLiteral(SimpleLiteral {
                        value: converted,
                    }))
                },
                ThinError::expected,
            )
        }))
    }

    fn plain_term_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<PlainTermEncoding>>> {
        Some(create_plain_term_sparql_op_impl(|args| {
            match &args.args[0] {
                EncodingDatum::Array(array) => Ok(ColumnarValue::Array(
                    impl_str_plain_term(array).into_array(),
                )),
                EncodingDatum::Scalar(scalar, _) => {
                    let array = scalar.to_array(1)?;
                    impl_str_plain_term(&array)
                        .try_as_scalar(0)
                        .map(|scalar| ColumnarValue::Scalar(scalar.into_scalar_value()))
                }
            }
        }))
    }
}

fn impl_str_plain_term(array: &PlainTermArray) -> PlainTermArray {
    let parts = array.as_parts();

    let value = Arc::clone(
        parts
            .struct_array
            .column(PlainTermEncodingField::Value.index()),
    );

    let term_types_data =
        UInt8Array::from_iter(repeat_n(u8::from(PlainTermType::Literal), value.len()))
            .to_data()
            .into_builder()
            .nulls(value.nulls().cloned())
            .build()
            .unwrap();
    let term_types = UInt8Array::from(term_types_data);

    let data_types_data =
        StringArray::from_iter_values(repeat_n(xsd::STRING.as_str(), value.len()))
            .to_data()
            .into_builder()
            .nulls(value.nulls().cloned())
            .build()
            .unwrap();
    let data_types = StringArray::from(data_types_data);

    PlainTermArrayBuilder::new(Arc::new(term_types), value)
        .with_data_types(Arc::new(data_types))
        .finish()
}

fn try_str_fast_path(args: &ScalarSparqlOpArgs<TypedValueEncoding>)
    -> AResult<Option<ColumnarValue>> {
    let col = args.args[0].to_array();
    let parts = col.parts_as_ref();

    if parts.array.len() == parts.named_nodes.len() {
        let utf8_arr = Arc::clone(parts.array.child(TypedValueEncodingField::NamedNode.type_id())) as ArrayRef;
        return Ok(Some(utf8_arr.into()));
    }

    if parts.array.len() == parts.blank_nodes.len() {
        let utf8_arr = Arc::clone(parts.array.child(TypedValueEncodingField::BlankNode.type_id())) as ArrayRef;
        return Ok(Some(utf8_arr.into()));
    }

    if parts.array.len() == parts.booleans.len() {
        let utf8_arr = compute::cast(parts.booleans as &dyn Array, &DataType::Utf8)?;
        return Ok(Some(utf8_arr.into()));
    }

    if parts.array.len() == parts.integers.len() {
        let utf8_arr = compute::cast(parts.integers as &dyn Array, &DataType::Utf8)?;
        return Ok(Some(utf8_arr.into()));
    }

    if parts.array.len() == parts.ints.len() {
        let utf8_arr = compute::cast(parts.ints as &dyn Array, &DataType::Utf8)?;
        return Ok(Some(utf8_arr.into()));
    }

    if parts.array.len() == parts.floats.len() {
        let utf8_arr = compute::cast(parts.floats as &dyn Array, &DataType::Utf8)?;
        return Ok(Some(utf8_arr.into()));
    }

    if parts.array.len() == parts.doubles.len() {
        let utf8_arr = compute::cast(parts.doubles as &dyn Array, &DataType::Utf8)?;
        return Ok(Some(utf8_arr.into()));
    }

    if parts.array.len() == parts.decimals.len() {
        let utf8_arr = compute::cast(parts.decimals as &dyn Array, &DataType::Utf8)?;
        return Ok(Some(utf8_arr.into()));
    }
/*
    if parts.array.len() == parts.date_times.value.len() {
        let utf8_arr = compute::cast(parts.date_times as &dyn Array, &DataType::Utf8)?;
        return Ok(Some(utf8_arr.into()));
    }

    if parts.array.len() == parts.times.value.len() {
        let utf8_arr = compute::cast(parts.times as &dyn Array, &DataType::Utf8)?;
        return Ok(Some(utf8_arr.into()));
    }

    if parts.array.len() == parts.dates.value.len() {
        let utf8_arr = compute::cast(parts.dates as &dyn Array, &DataType::Utf8)?;
        return Ok(Some(utf8_arr.into()));
    }

    if parts.array.len() == parts.durations.months.len() {
        let utf8_arr = compute::cast(parts.durations as &dyn Array, &DataType::Utf8)?;
        return Ok(Some(utf8_arr.into()));
    }

    if parts.array.len() == parts.strings.value.len() {
        let utf8_arr = Arc::new(StringArray::from_iter_values(
            parts.strings.iter().map(|s| s.as_str())
        )) as ArrayRef;
        return Ok(Some(utf8_arr));
    }

    if parts.array.len() == parts.other_literals.value.len() {
        let utf8_arr = Arc::new(StringArray::from_iter_values(
            parts.other_literals.iter().map(|o| o.value())
        )) as ArrayRef;
        return Ok(Some(utf8_arr));
    }*/

    // no homogenous type → no fast path possible
    Ok(None)
}