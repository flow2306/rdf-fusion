use std::sync::Arc;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array, NullArray, StringArray, StructArray, UnionArray};
use datafusion::arrow::buffer::ScalarBuffer;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::typed_value::{TypedValueArray, TypedValueArrayElementBuilder, TypedValueEncoding, TypedValueEncodingField};
use rdf_fusion_model::DFResult;
use crate::scalar::ScalarSparqlOpArgs;

pub fn invoke_typed_value_array(
    array: &TypedValueArray,
    args: &ScalarSparqlOpArgs<TypedValueEncoding>,
    target_type:TypedValueEncodingField,
) -> DFResult<ArrayRef> {
    let parts = array.parts_as_ref();

    // If we do not have nulls, we can simply scan and check the type ids.
    if parts.null_count == 0 {
        let results = parts
            .array
            .type_ids()
            .iter()
            .map(|type_id| Some(*type_id == target_type.type_id()))
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
            let boolean = *type_id == target_type.type_id();
            result.append_boolean(boolean.into())?;
        }
    }

    Ok(result.finish().into_array_ref())
}