use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::{EncodingDatum, EncodingScalar, RdfFusionEncodings};
use rdf_fusion_encoding::typed_value::{TypedValueEncoding, TypedValueEncodingField};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use crate::scalar::terms::common::invoke_typed_value_array;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct IsBlankSparqlOp;

impl Default for IsBlankSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsBlankSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::IsBlank);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for IsBlankSparqlOp {
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
            |args| match &args.args[0] {
                EncodingDatum::Array(array) => {
                    let array = invoke_typed_value_array(
                        array,
                        &args,
                        TypedValueEncodingField::BlankNode
                    )?;
                    Ok(ColumnarValue::Array(array))
                }
                EncodingDatum::Scalar(scalar, _) => {
                    let array = scalar.to_array(1)?;
                    let array_result = invoke_typed_value_array(
                        &array,
                        &args,
                        TypedValueEncodingField::BlankNode
                    )?;
                    let scalar_result = ScalarValue::try_from_array(&array_result, 0)?;
                    Ok(ColumnarValue::Scalar(scalar_result))
                }
            },
        ))
    }
}