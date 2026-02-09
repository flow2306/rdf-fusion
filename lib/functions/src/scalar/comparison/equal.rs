use crate::scalar::comparison::common::try_cmp_fast_path;
use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{
    ScalarSparqlOp, ScalarSparqlOpArgs, ScalarSparqlOpSignature, SparqlOpArity,
};
use datafusion::arrow::compute::kernels::cmp::eq;
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{ThinError, TypedValueRef};
use std::cmp::Ordering;

/// Implementation of the SPARQL `=` operator.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct EqualSparqlOp;

impl Default for EqualSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl EqualSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Equal);

    /// Creates a new [EqualSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for EqualSparqlOp {
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
            |args: ScalarSparqlOpArgs<TypedValueEncoding>| {
                if let Some(result) = try_cmp_fast_path(&args, eq)? {
                    return Ok(result);
                }

                dispatch_binary_typed_value(
                    &args.encoding,
                    &args.args[0],
                    &args.args[1],
                    |lhs_value, rhs_value| {
                        lhs_value
                            .partial_cmp(&rhs_value)
                            .map(|o| o == Ordering::Equal)
                            .map(Into::into)
                            .map(TypedValueRef::BooleanLiteral)
                            .ok_or(ThinError::ExpectedError)
                    },
                    |_, _| ThinError::expected(),
                )
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{create_compare_test_vector, create_default_builtin_udf};
    use datafusion::dataframe;
    use datafusion::logical_expr::col;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::typed_value::TypedValueEncoding;
    use rdf_fusion_extensions::functions::BuiltinName;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_equal_fast_path_integer() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let test_vector = create_compare_test_vector(&encoding);
        let udf = create_default_builtin_udf(encoding, BuiltinName::Equal);

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
        +-------------+-------------+-----------------------------------+
        | input1      | input2      | EQ(?table?.input1,?table?.input2) |
        +-------------+-------------+-----------------------------------+
        | {integer=1} | {integer=2} | {boolean=false}                   |
        | {integer=2} | {integer=1} | {boolean=false}                   |
        | {integer=1} | {integer=1} | {boolean=true}                    |
        +-------------+-------------+-----------------------------------+
        "
        )
    }
}
