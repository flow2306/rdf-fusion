use crate::scalar::conversion::common::try_cast_fast_path;
use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use datafusion::arrow::datatypes::DataType;
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_encoding::typed_value::{TypedValueEncoding, TypedValueEncodingField};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{Decimal, Numeric, ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CastDecimalSparqlOp;

impl Default for CastDecimalSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastDecimalSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::CastDecimal);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastDecimalSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(1))
    }

    fn typed_value_encoding_op(
        &self,
        encodings: &RdfFusionEncodings,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(
            encodings.typed_value(),
            |args| {
                if let Some(result) = try_cast_fast_path(
                    &args,
                    DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
                    TypedValueEncodingField::Decimal,
                )? {
                    return Ok(result);
                }

                dispatch_unary_typed_value(
                    &args.encoding,
                    &args.args[0],
                    |value| {
                        let converted = match value {
                            TypedValueRef::BooleanLiteral(v) => Decimal::from(v),
                            TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
                            TypedValueRef::NumericLiteral(numeric) => match numeric {
                                Numeric::Int(v) => Decimal::from(v),
                                Numeric::Integer(v) => Decimal::from(v),
                                Numeric::Float(v) => Decimal::try_from(v)?,
                                Numeric::Double(v) => Decimal::try_from(v)?,
                                Numeric::Decimal(v) => v,
                            },
                            _ => return ThinError::expected(),
                        };
                        Ok(TypedValueRef::NumericLiteral(Numeric::from(converted)))
                    },
                    ThinError::expected,
                )
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{create_default_builtin_udf, create_mixed_test_vector};
    use datafusion::dataframe;
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::typed_value::{TypedValueEncoding, TypedValueEncodingField};
    use rdf_fusion_extensions::functions::BuiltinName;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_cast_decimal_normal() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector = create_mixed_test_vector(&encoding, None);
        let udf = create_default_builtin_udf(encoding, BuiltinName::CastDecimal);

        let input = dataframe!(
            "input" => test_vector,
        )
            .unwrap();

        let result = input
            .select([col("input"), udf.call(vec![col("input")])])
            .unwrap();
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------------------------------------------------------+-----------------------------------+
        | input                                                          | xsd:decimal(?table?.input)        |
        +----------------------------------------------------------------+-----------------------------------+
        | {named_node=http://example.com/test}                           | {null=}                           |
        | {decimal=1000.0000000000000000}                                | {decimal=1000.0000000000000000}   |
        | {string={value: String1, language: }}                          | {null=}                           |
        | {string={value: 01, language: }}                               | {decimal=100.0000000000000000}    |
        | {blank_node=test1}                                             | {null=}                           |
        | {integer=2605}                                                 | {decimal=260500.0000000000000000} |
        | {float=26.05}                                                  | {decimal=2604.9999237060546560}   |
        | {boolean=true}                                                 | {decimal=100.0000000000000000}    |
        | {date_time={value: 6389958449600.0000000000000000, offset: 0}} | {null=}                           |
        | {time={value: 100.0000000000000000, offset: 60}}               | {null=}                           |
        | {date={value: 6389953920000.0000000000000000, offset: }}       | {null=}                           |
        | {duration={months: 7, seconds: 700.0000000000000000}}          | {null=}                           |
        +----------------------------------------------------------------+-----------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_cast_decimal_fast_path_integer() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector =
            create_mixed_test_vector(&encoding, Some(TypedValueEncodingField::Integer));
        let udf = create_default_builtin_udf(encoding, BuiltinName::CastDecimal);

        let input = dataframe!(
            "input" => test_vector,
        )
            .unwrap();

        let result = input
            .select([col("input"), udf.call(vec![col("input")])])
            .unwrap();
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------+---------------------------------+
        | input          | xsd:decimal(?table?.input)      |
        +----------------+---------------------------------+
        | {integer=2605} | {decimal=2605.0000000000000000} |
        +----------------+---------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_cast_decimal_fast_path_decimal() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector =
            create_mixed_test_vector(&encoding, Some(TypedValueEncodingField::Decimal));
        let udf = create_default_builtin_udf(encoding, BuiltinName::CastDecimal);

        let input = dataframe!(
            "input" => test_vector,
        )
            .unwrap();

        let result = input
            .select([col("input"), udf.call(vec![col("input")])])
            .unwrap();
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +---------------------------------+---------------------------------+
        | input                           | xsd:decimal(?table?.input)      |
        +---------------------------------+---------------------------------+
        | {decimal=1000.0000000000000000} | {decimal=1000.0000000000000000} |
        +---------------------------------+---------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_cast_decimal_fast_path_float() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector =
            create_mixed_test_vector(&encoding, Some(TypedValueEncodingField::Float));
        let udf = create_default_builtin_udf(encoding, BuiltinName::CastDecimal);

        let input = dataframe!(
            "input" => test_vector,
        )
            .unwrap();

        let result = input
            .select([col("input"), udf.call(vec![col("input")])])
            .unwrap();
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +---------------+-------------------------------+
        | input         | xsd:decimal(?table?.input)    |
        +---------------+-------------------------------+
        | {float=26.05} | {decimal=26.0499992370605472} |
        +---------------+-------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_cast_decimal_fast_path_string() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector =
            create_mixed_test_vector(&encoding, Some(TypedValueEncodingField::String));
        let udf = create_default_builtin_udf(encoding, BuiltinName::CastDecimal);

        let input = dataframe!(
            "input" => test_vector,
        )
            .unwrap();

        let result = input
            .select([col("input"), udf.call(vec![col("input")])])
            .unwrap();
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +---------------------------------------+--------------------------------+
        | input                                 | xsd:decimal(?table?.input)     |
        +---------------------------------------+--------------------------------+
        | {string={value: String1, language: }} | {null=}                        |
        | {string={value: 01, language: }}      | {decimal=100.0000000000000000} |
        +---------------------------------------+--------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_cast_decimal_fast_path_boolean() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector =
            create_mixed_test_vector(&encoding, Some(TypedValueEncodingField::Boolean));
        let udf = create_default_builtin_udf(encoding, BuiltinName::CastDecimal);

        let input = dataframe!(
            "input" => test_vector,
        )
            .unwrap();

        let result = input
            .select([col("input"), udf.call(vec![col("input")])])
            .unwrap();
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +----------------+--------------------------------+
        | input          | xsd:decimal(?table?.input)     |
        +----------------+--------------------------------+
        | {boolean=true} | {decimal=100.0000000000000000} |
        +----------------+--------------------------------+
        "
        )
    }
}
