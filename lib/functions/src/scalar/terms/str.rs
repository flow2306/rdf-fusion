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
use rdf_fusion_encoding::typed_value::{TypedValueArrayElementBuilder, TypedValueEncoding};
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, EncodingScalar};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{AResult, ThinError};
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{SimpleLiteral, TypedValue, TypedValueRef};
use std::sync::Arc;

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
                    impl_str_plain_term(array).into_array_ref(),
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

    let mut builder = TypedValueArrayElementBuilder::default();

    if parts.array.len() == parts.named_nodes.len() {
        let utf8_arr = Arc::new(parts.named_nodes.clone()) as ArrayRef;
        return Ok(Some(utf8_arr.into()));
    }

    if parts.array.len() == parts.blank_nodes.len() {
        let utf8_arr = Arc::new(parts.blank_nodes.clone()) as ArrayRef;
        return Ok(Some(utf8_arr.into()));
    }
/*
    // Named nodes
    if parts.array.len() == parts.named_nodes.len() {
        for v in parts.named_nodes.iter() {
            builder.append_string(v.expect("string representation").to_string().as_str(), None)?;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }

    // Blank nodes
    if parts.array.len() == parts.blank_nodes.len() {
        for v in parts.blank_nodes.iter() {
            builder.append_string(v.to_string().as_str(), None)?;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }

    // Strings (Simple + Language Literals)
    // TODO: check if language tag can be specified
    if parts.array.len() == parts.strings.len() {
        for s in parts.strings.iter() {
            builder.append_string(s.to_string().as_str(), None)??;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }

    // Booleans
    if parts.array.len() == parts.booleans.len() {
        for b in parts.booleans.iter() {
            builder.append_string(b.to_string())?;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }

    // Numerics: floats
    if parts.array.len() == parts.floats.len() {
        for f in parts.floats.iter() {
            builder.append_string(f.to_string())?;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }

    // Numerics: doubles
    if parts.array.len() == parts.doubles.len() {
        for f in parts.doubles.iter() {
            builder.append_string(f.to_string())?;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }

    // Numerics: decimals
    if parts.array.len() == parts.decimals.len() {
        for d in parts.decimals.iter() {
            builder.append_string(d.to_string())?;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }

    // Numerics: ints
    if parts.array.len() == parts.ints.len() {
        for i in parts.ints.iter() {
            builder.append_string(i.to_string())?;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }

    // Numerics: integers (i64)
    if parts.array.len() == parts.integers.len() {
        for i in parts.integers.iter() {
            builder.append_string(i.to_string())?;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }

    // DateTime
    if parts.array.len() == parts.date_times.len() {
        for dt in parts.date_times.iter() {
            builder.append_string(dt.to_string())?;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }

    // Time
    if parts.array.len() == parts.times.len() {
        for t in parts.times.iter() {
            builder.append_string(t.to_string())?;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }

    // Date
    if parts.array.len() == parts.dates.len() {
        for d in parts.dates.iter() {
            builder.append_string(d.to_string())?;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }

    // Duration
    if parts.array.len() == parts.durations.len() {
        for dur in parts.durations.iter() {
            builder.append_string(dur.to_string())?;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }

    // Other literals
    if parts.array.len() == parts.other_literals.len() {
        for ol in parts.other_literals.iter() {
            builder.append_string(ol.value().to_owned())?;
        }
        return Ok(Some(ColumnarValue::Array(builder.finish().into_array())));
    }*/

    // Kein homogener Typ → kein Fast Path möglich
    Ok(None)
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{create_default_builtin_udf, create_mixed_test_vector};
    use datafusion::dataframe;
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_extensions::functions::BuiltinName;

    #[tokio::test]
    async fn test_str_typed_value() {
        let test_vector = create_mixed_test_vector();
        let udf = create_default_builtin_udf(BuiltinName::Str);

        let input = dataframe!(
            "input" => test_vector,
        )
        .unwrap();

        let result = input
            .select([col("input"), udf.call(vec![col("input")])])
            .unwrap();
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @r"
        +--------------------------------------+-------------------------------------------------------+
        | input                                | STR(?table?.input)                                    |
        +--------------------------------------+-------------------------------------------------------+
        | {named_node=http://example.com/test} | {string={value: http://example.com/test, language: }} |
        | {decimal=1000.0000000000000000}      | {string={value: 10, language: }}                      |
        +--------------------------------------+-------------------------------------------------------+
        "
        )
    }
}
