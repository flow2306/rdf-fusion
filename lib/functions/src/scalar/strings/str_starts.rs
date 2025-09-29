use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{
    CompatibleStringArgs, StringLiteralRef, ThinError, TypedValueRef,
};

/// Implementation of the SPARQL `strstarts` function.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct StrStartsSparqlOp;

impl Default for StrStartsSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrStartsSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::StrStarts);

    /// Creates a new [StrStartsSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrStartsSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(2))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_binary_typed_value(
                &args.args[0],
                &args.args[1],
                |lhs_value, rhs_value| {
                    let lhs_value = StringLiteralRef::try_from(lhs_value)?;
                    let rhs_value = StringLiteralRef::try_from(rhs_value)?;
                    let args = CompatibleStringArgs::try_from(lhs_value, rhs_value)?;
                    Ok(TypedValueRef::BooleanLiteral(
                        args.lhs.starts_with(args.rhs).into(),
                    ))
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
