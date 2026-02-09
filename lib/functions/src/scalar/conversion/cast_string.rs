use datafusion::arrow::datatypes::DataType;
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_model::ThinError;
use rdf_fusion_model::{SimpleLiteral, TypedValue, TypedValueRef};

use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::{TypedValueEncoding, TypedValueEncodingField};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use crate::scalar::conversion::common::try_cast_fast_path;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CastStringSparqlOp;

impl Default for CastStringSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastStringSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::CastString);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastStringSparqlOp {
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
                    DataType::Utf8,
                    TypedValueEncodingField::String,
                )? {
                    return Ok(result);
                }

                dispatch_unary_owned_typed_value(
                    &args.encoding,
                    &args.args[0],
                    |value| {
                        let converted = match value {
                            TypedValueRef::NamedNode(value) => value.as_str().to_owned(),
                            TypedValueRef::BlankNode(_) => return ThinError::expected(),
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
                            TypedValueRef::DayTimeDurationLiteral(value) => {
                                value.to_string()
                            }
                            TypedValueRef::OtherLiteral(value) => {
                                value.value().to_owned()
                            }
                        };
                        Ok(TypedValue::SimpleLiteral(SimpleLiteral {
                            value: converted,
                        }))
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
    async fn test_cast_string_normal() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector = create_mixed_test_vector(&encoding, None);
        let udf = create_default_builtin_udf(encoding, BuiltinName::CastString);

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
        +----------------------------------------------------------------+-------------------------------------------------------+
        | input                                                          | xsd:string(?table?.input)                             |
        +----------------------------------------------------------------+-------------------------------------------------------+
        | {named_node=http://example.com/test}                           | {string={value: http://example.com/test, language: }} |
        | {decimal=1000.0000000000000000}                                | {string={value: 10, language: }}                      |
        | {string={value: String1, language: }}                          | {string={value: String1, language: }}                 |
        | {string={value: 01, language: }}                               | {string={value: 01, language: }}                      |
        | {blank_node=test1}                                             | {null=}                                               |
        | {integer=2605}                                                 | {string={value: 2605, language: }}                    |
        | {float=26.05}                                                  | {string={value: 26.05, language: }}                   |
        | {boolean=true}                                                 | {string={value: true, language: }}                    |
        | {date_time={value: 6389958449600.0000000000000000, offset: 0}} | {string={value: 2025-11-24T12:34:56Z, language: }}    |
        | {time={value: 100.0000000000000000, offset: 60}}               | {string={value: 01:00:01+01:00, language: }}          |
        | {date={value: 6389953920000.0000000000000000, offset: }}       | {string={value: 2025-11-24, language: }}              |
        | {duration={months: 7, seconds: 700.0000000000000000}}          | {string={value: P7MT7S, language: }}                  |
        +----------------------------------------------------------------+-------------------------------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_cast_string_fast_path_integer() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector =
            create_mixed_test_vector(&encoding, Some(TypedValueEncodingField::Integer));
        let udf = create_default_builtin_udf(encoding, BuiltinName::CastString);

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
        +----------------+------------------------------------+
        | input          | xsd:string(?table?.input)          |
        +----------------+------------------------------------+
        | {integer=2605} | {string={value: 2605, language: }} |
        +----------------+------------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_cast_string_fast_path_decimal() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector =
            create_mixed_test_vector(&encoding, Some(TypedValueEncodingField::Decimal));
        let udf = create_default_builtin_udf(encoding, BuiltinName::CastString);

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
        +---------------------------------+------------------------+
        | input                           | xsd:int(?table?.input) |
        +---------------------------------+------------------------+
        | {decimal=1000.0000000000000000} | {int=1000}             |
        +---------------------------------+------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_cast_string_fast_path_float() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector =
            create_mixed_test_vector(&encoding, Some(TypedValueEncodingField::Float));
        let udf = create_default_builtin_udf(encoding, BuiltinName::CastString);

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
        +---------------+-------------------------------------+
        | input         | xsd:string(?table?.input)           |
        +---------------+-------------------------------------+
        | {float=26.05} | {string={value: 26.05, language: }} |
        +---------------+-------------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_cast_string_fast_path_string() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector =
            create_mixed_test_vector(&encoding, Some(TypedValueEncodingField::String));
        let udf = create_default_builtin_udf(encoding, BuiltinName::CastString);

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
        +---------------------------------------+---------------------------------------+
        | input                                 | xsd:string(?table?.input)             |
        +---------------------------------------+---------------------------------------+
        | {string={value: String1, language: }} | {string={value: String1, language: }} |
        | {string={value: 01, language: }}      | {string={value: 01, language: }}      |
        +---------------------------------------+---------------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_cast_string_fast_path_boolean() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector =
            create_mixed_test_vector(&encoding, Some(TypedValueEncodingField::Boolean));
        let udf = create_default_builtin_udf(encoding, BuiltinName::CastString);

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
        +----------------+------------------------------------+
        | input          | xsd:string(?table?.input)          |
        +----------------+------------------------------------+
        | {boolean=true} | {string={value: true, language: }} |
        +----------------+------------------------------------+
        "
        )
    }
}
