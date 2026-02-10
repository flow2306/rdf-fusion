use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::numeric::common::try_arithmetic_fast_path;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use datafusion::arrow::compute::kernels::numeric::mul;
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{Numeric, NumericPair, ThinError, TypedValueRef};

/// Implementation of the SPARQL `*` operator.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct MulSparqlOp;

impl Default for MulSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl MulSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Mul);

    /// Creates a new [MulSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for MulSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(2))
    }
    fn typed_value_encoding_op(
        &self,
        encodings: &RdfFusionEncodings,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(
            encodings.typed_value(),
            |args| {
                if let Some(result) = try_arithmetic_fast_path(&args, mul)? {
                    return Ok(result);
                }

                dispatch_binary_typed_value(
                    &args.encoding,
                    &args.args[0],
                    &args.args[1],
                    |lhs_value, rhs_value| {
                        if let (
                            TypedValueRef::NumericLiteral(lhs_numeric),
                            TypedValueRef::NumericLiteral(rhs_numeric),
                        ) = (lhs_value, rhs_value)
                        {
                            match NumericPair::with_casts_from(lhs_numeric, rhs_numeric) {
                                NumericPair::Int(lhs, rhs) => {
                                    lhs.checked_mul(rhs).map(Numeric::Int)
                                }
                                NumericPair::Integer(lhs, rhs) => {
                                    lhs.checked_mul(rhs).map(Numeric::Integer)
                                }
                                NumericPair::Float(lhs, rhs) => {
                                    Ok(Numeric::Float(lhs * rhs))
                                }
                                NumericPair::Double(lhs, rhs) => {
                                    Ok(Numeric::Double(lhs * rhs))
                                }
                                NumericPair::Decimal(lhs, rhs) => {
                                    lhs.checked_mul(rhs).map(Numeric::Decimal)
                                }
                            }
                            .map(TypedValueRef::NumericLiteral)
                        } else {
                            ThinError::expected()
                        }
                    },
                    |_, _| ThinError::expected(),
                )
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{
        create_binary_numeric_mixed_test_vector, create_default_builtin_udf,
    };
    use datafusion::dataframe;
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::typed_value::{TypedValueEncoding, TypedValueEncodingField};
    use rdf_fusion_extensions::functions::BuiltinName;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_mul_mixed() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector = create_binary_numeric_mixed_test_vector(&encoding, None);
        let udf = create_default_builtin_udf(encoding, BuiltinName::Mul);

        let input = dataframe!(
            "input1" => test_vector[0].clone(),
            "input2" => test_vector[1].clone(),
        )
        .unwrap();

        let result = input
            .select([
                col("input1"),
                col("input2"),
                udf.call(vec![col("input1"), col("input2")]),
            ])
            .unwrap();
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +-----------------+-----------------+------------------------------------+
        | input1          | input2          | MUL(?table?.input1,?table?.input2) |
        +-----------------+-----------------+------------------------------------+
        | {integer=435}   | {integer=267}   | {integer=116145}                   |
        | {integer=245}   | {integer=155}   | {integer=37975}                    |
        | {integer=123}   | {integer=1777}  | {integer=218571}                   |
        | {integer=34}    | {integer=0}     | {integer=0}                        |
        | {float=123.56}  | {float=594.39}  | {float=73442.83}                   |
        | {float=95.303}  | {float=234.134} | {float=22313.674}                  |
        | {float=2658.48} | {float=26.4}    | {float=70183.87}                   |
        | {float=3.14}    | {float=0.0}     | {float=0.0}                        |
        +-----------------+-----------------+------------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_mul_fast_path_integer() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector = create_binary_numeric_mixed_test_vector(
            &encoding,
            Some(TypedValueEncodingField::Integer),
        );
        let udf = create_default_builtin_udf(encoding, BuiltinName::Mul);

        let input = dataframe!(
            "input1" => test_vector[0].clone(),
            "input2" => test_vector[1].clone(),
        )
        .unwrap();

        let result = input
            .select([
                col("input1"),
                col("input2"),
                udf.call(vec![col("input1"), col("input2")]),
            ])
            .unwrap();
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +---------------+----------------+------------------------------------+
        | input1        | input2         | MUL(?table?.input1,?table?.input2) |
        +---------------+----------------+------------------------------------+
        | {integer=435} | {integer=267}  | {integer=116145}                   |
        | {integer=245} | {integer=155}  | {integer=37975}                    |
        | {integer=123} | {integer=1777} | {integer=218571}                   |
        | {integer=34}  | {integer=0}    | {integer=0}                        |
        +---------------+----------------+------------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_mul_fast_path_float() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector = create_binary_numeric_mixed_test_vector(
            &encoding,
            Some(TypedValueEncodingField::Float),
        );
        let udf = create_default_builtin_udf(encoding, BuiltinName::Mul);

        let input = dataframe!(
            "input1" => test_vector[0].clone(),
            "input2" => test_vector[1].clone(),
        )
        .unwrap();

        let result = input
            .select([
                col("input1"),
                col("input2"),
                udf.call(vec![col("input1"), col("input2")]),
            ])
            .unwrap();
        assert_snapshot!(
            result.to_string().await.unwrap(),
            @"
        +-----------------+-----------------+------------------------------------+
        | input1          | input2          | MUL(?table?.input1,?table?.input2) |
        +-----------------+-----------------+------------------------------------+
        | {float=123.56}  | {float=594.39}  | {float=73442.83}                   |
        | {float=95.303}  | {float=234.134} | {float=22313.674}                  |
        | {float=2658.48} | {float=26.4}    | {float=70183.87}                   |
        | {float=3.14}    | {float=0.0}     | {float=0.0}                        |
        +-----------------+-----------------+------------------------------------+
        "
        )
    }
}
