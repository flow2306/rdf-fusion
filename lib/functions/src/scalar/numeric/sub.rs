use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::numeric::common::try_arithmetic_fast_path;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use datafusion::arrow::compute::kernels::numeric::sub;
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{Numeric, NumericPair, ThinError, TypedValueRef};

/// Implementation of the SPARQL `-` operator.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct SubSparqlOp;

impl Default for SubSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl SubSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Sub);

    /// Creates a new [SubSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for SubSparqlOp {
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
                if let Some(result) = try_arithmetic_fast_path(&args, sub)? {
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
                                    lhs.checked_sub(rhs).map(Numeric::Int)
                                }
                                NumericPair::Integer(lhs, rhs) => {
                                    lhs.checked_sub(rhs).map(Numeric::Integer)
                                }
                                NumericPair::Float(lhs, rhs) => {
                                    Ok(Numeric::Float(lhs - rhs))
                                }
                                NumericPair::Double(lhs, rhs) => {
                                    Ok(Numeric::Double(lhs - rhs))
                                }
                                NumericPair::Decimal(lhs, rhs) => {
                                    lhs.checked_sub(rhs).map(Numeric::Decimal)
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
    use crate::test_utils::{create_binary_numeric_mixed_test_vector, create_default_builtin_udf};
    use datafusion::dataframe;
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::typed_value::{TypedValueEncoding, TypedValueEncodingField};
    use rdf_fusion_extensions::functions::BuiltinName;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_sub_mixed() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector = create_binary_numeric_mixed_test_vector(&encoding, None);
        let udf = create_default_builtin_udf(encoding, BuiltinName::Sub);

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
        | input1          | input2          | SUB(?table?.input1,?table?.input2) |
        +-----------------+-----------------+------------------------------------+
        | {integer=435}   | {integer=267}   | {integer=168}                      |
        | {integer=245}   | {integer=155}   | {integer=90}                       |
        | {integer=123}   | {integer=1777}  | {integer=-1654}                    |
        | {integer=34}    | {integer=0}     | {integer=34}                       |
        | {float=123.56}  | {float=594.39}  | {float=-470.83002}                 |
        | {float=95.303}  | {float=234.134} | {float=-138.831}                   |
        | {float=2658.48} | {float=26.4}    | {float=2632.08}                    |
        | {float=3.14}    | {float=0.0}     | {float=3.14}                       |
        +-----------------+-----------------+------------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_sub_fast_path_integer() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector = create_binary_numeric_mixed_test_vector(&encoding, Some(TypedValueEncodingField::Integer));
        let udf = create_default_builtin_udf(encoding, BuiltinName::Sub);

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
        | input1        | input2         | SUB(?table?.input1,?table?.input2) |
        +---------------+----------------+------------------------------------+
        | {integer=435} | {integer=267}  | {integer=168}                      |
        | {integer=245} | {integer=155}  | {integer=90}                       |
        | {integer=123} | {integer=1777} | {integer=-1654}                    |
        | {integer=34}  | {integer=0}    | {integer=34}                       |
        +---------------+----------------+------------------------------------+
        "
        )
    }

    #[tokio::test]
    async fn test_sub_fast_path_float() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector = create_binary_numeric_mixed_test_vector(&encoding, Some(TypedValueEncodingField::Float));
        let udf = create_default_builtin_udf(encoding, BuiltinName::Sub);

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
        | input1          | input2          | SUB(?table?.input1,?table?.input2) |
        +-----------------+-----------------+------------------------------------+
        | {float=123.56}  | {float=594.39}  | {float=-470.83002}                 |
        | {float=95.303}  | {float=234.134} | {float=-138.831}                   |
        | {float=2658.48} | {float=26.4}    | {float=2632.08}                    |
        | {float=3.14}    | {float=0.0}     | {float=3.14}                       |
        +-----------------+-----------------+------------------------------------+
        "
        )
    }
}
