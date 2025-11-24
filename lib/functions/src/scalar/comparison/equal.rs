use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{
    ScalarSparqlOp, ScalarSparqlOpArgs, ScalarSparqlOpSignature, SparqlOpArity,
};
use datafusion::arrow::array::Array;
use datafusion::arrow::compute::kernels::cmp::eq;
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_encoding::typed_value::{
    TypedValueArrayElementBuilder, TypedValueEncoding,
};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{AResult, ThinError, TypedValueRef};
use std::cmp::Ordering;
use std::sync::Arc;

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
                if let Some(result) = try_equals_fast_path(&args)? {
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

fn try_equals_fast_path(
    args: &ScalarSparqlOpArgs<TypedValueEncoding>,
) -> AResult<Option<ColumnarValue>> {
    let lhs = args.args[0].to_array();
    let rhs = args.args[1].to_array();
    let lhs_parts = lhs.parts_as_ref();
    let rhs_parts = rhs.parts_as_ref();

    // lhs_parts.array.type_ids().iter().map(|tid|Some(*tid == TypedValueEncodingField::Null.type_id())).collect::<BooleanArray>();

    let lhs_is_all_ints = lhs_parts.array.len() == lhs_parts.integers.len();
    let rhs_is_all_ints = rhs_parts.array.len() == rhs_parts.integers.len();

    if !lhs_is_all_ints || !rhs_is_all_ints {
        return Ok(None);
    }

    let result = eq(lhs_parts.integers, rhs_parts.integers)
        .expect("Arrays have the same type and length");

    let mut array_builder =
        TypedValueArrayElementBuilder::new(Arc::clone(&args.encoding));
    for value in result.values() {
        array_builder.append_boolean(value.into())?;
    }

    Ok(Some(ColumnarValue::Array(
        array_builder.finish().into_array_ref(),
    )))
}
