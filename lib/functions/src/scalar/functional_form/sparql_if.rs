use crate::scalar::dispatch::dispatch_ternary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::Boolean;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct IfSparqlOp;

impl Default for IfSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IfSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::If);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for IfSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(3))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_ternary_typed_value(
                &args.args[0],
                &args.args[1],
                &args.args[2],
                |arg0, arg1, arg2| {
                    let test = Boolean::try_from(arg0)?;
                    if test.as_bool() { Ok(arg1) } else { Ok(arg2) }
                },
                |arg0, arg1, arg2| {
                    let test = Boolean::try_from(arg0?)?;
                    if test.as_bool() { arg1 } else { arg2 }
                },
            )
        }))
    }
}
