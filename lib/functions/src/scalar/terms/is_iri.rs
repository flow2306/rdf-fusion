use std::sync::Arc;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array, NullArray, StringArray, StructArray, UnionArray};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::ColumnarValue;
use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpArgs, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, EncodingScalar, RdfFusionEncodings};
use rdf_fusion_encoding::typed_value::{TypedValueArray, TypedValueArrayElementBuilder, TypedValueEncoding, TypedValueEncodingField, TypedValueEncodingRef};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{DFResult, ThinError, TypedValueRef};

/// Checks whether a given RDF term is an IRI.
///
/// # Relevant Resources
/// - [SPARQL 1.1 - isIRI](https://www.w3.org/TR/sparql11-query/#func-isIRI)
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct IsIriSparqlOp;

impl Default for IsIriSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsIriSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::IsIri);

    /// Creates a new [IsIriSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for IsIriSparqlOp {
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
                let array = invoke_typed_value_array(array, &args)?;
                Ok(ColumnarValue::Array(array))
            }
            EncodingDatum::Scalar(scalar, _) => {
                let array = scalar.to_array(1)?;
                let array_result = invoke_typed_value_array(&array, &args)?;
                let scalar_result = ScalarValue::try_from_array(&array_result, 0)?;
                Ok(ColumnarValue::Scalar(scalar_result))
            }
        }))
    }
}

fn invoke_typed_value_array(array: &TypedValueArray, args: &ScalarSparqlOpArgs<TypedValueEncoding>) -> DFResult<ArrayRef> {
    let parts = array.parts_as_ref();

    // If we do not have nulls, we can simply scan and check the type ids.
    // TODO: Of course there should be some helper functions to make this more readable and
    //       share logic between IS_IRI, IS_LITERAL etc. :)
    if parts.null_count == 0 {
        let results = parts
            .array
            .type_ids()
            .iter()
            .map(|type_id| Some(*type_id == TypedValueEncodingField::NamedNode.type_id()))
            .collect::<BooleanArray>();

        let type_ids = (0..results.len() as i32)
            .map(|_| TypedValueEncodingField::Boolean.type_id())
            .collect();
        let offsets = (0..results.len() as i32)
            .into_iter()
            .collect::<ScalarBuffer<i32>>();

        return Ok(Arc::new(
            UnionArray::try_new(
                TypedValueEncoding::fields(),
                type_ids,
                Some(offsets),
                vec![
                    Arc::new(NullArray::new(0)),
                    Arc::new(StringArray::new_null(0)),
                    Arc::new(StringArray::new_null(0)),
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::string_fields(),
                        0,
                    )),
                    Arc::new(results),
                    Arc::new(Float32Array::new_null(0)),
                    Arc::new(Float64Array::new_null(0)),
                    Arc::new(Decimal128Array::new_null(0)),
                    Arc::new(Int32Array::new_null(0)),
                    Arc::new(Int64Array::new_null(0)),
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::timestamp_fields(),
                        0,
                    )),
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::timestamp_fields(),
                        0,
                    )),
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::timestamp_fields(),
                        0,
                    )),
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::duration_fields(),
                        0,
                    )),
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::typed_literal_fields(),
                        0,
                    )),
                ],
            )
                .expect("Fields and type match"),
        ));
    }

    // The regular path must distinguish between Null and Non-null values.
    let mut result = TypedValueArrayElementBuilder::new(Arc::clone(&args.encoding));
    for type_id in parts.array.type_ids() {
        if *type_id == TypedValueEncodingField::Null.type_id() {
            result.append_null()?;
        } else {
            let boolean = *type_id == TypedValueEncodingField::NamedNode.type_id();
            result.append_boolean(boolean.into())?;
        }
    }

    Ok(result.finish().into_array_ref())
}