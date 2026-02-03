use crate::scalar::ScalarSparqlOpArgs;
use datafusion::arrow::array::Array;
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::typed_value::{
    TypedValueArrayBuilder, TypedValueEncoding, TypedValueEncodingField,
};
use rdf_fusion_model::{AResult, DFResult};
use std::sync::Arc;

pub fn try_cast_fast_path(
    args: &ScalarSparqlOpArgs<TypedValueEncoding>,
    target_type: DataType,
    target_field: TypedValueEncodingField,
) -> AResult<Option<ColumnarValue>> {
    let col = args.args[0].to_array();
    let parts = col.parts_as_ref();

    let candidates: &[(&dyn Array, usize)] = &[
        (parts.booleans, parts.booleans.len()),
        (parts.ints, parts.ints.len()),
        (parts.integers, parts.integers.len()),
        (parts.floats, parts.floats.len()),
        (parts.doubles, parts.doubles.len()),
        (parts.decimals, parts.decimals.len()),
    ];

    for (array, len) in candidates {
        if parts.array.len() == *len {
            let cast_arr = compute::cast(*array, &target_type)?;
            let builder = TypedValueArrayBuilder::new_with_single_type(
                Arc::clone(&args.encoding),
                target_field.type_id(),
                *len,
            )?;

            let builder =
                build_single_type_array(builder, target_field, Arc::new(cast_arr))?;
            let result = builder.finish().unwrap();

            return Ok(Some(ColumnarValue::Array(result.into_array_ref())));
        }
    }

    Ok(None)
}

fn build_single_type_array(
    builder: TypedValueArrayBuilder,
    field: TypedValueEncodingField,
    array: Arc<dyn Array>,
) -> DFResult<TypedValueArrayBuilder> {
    match field {
        TypedValueEncodingField::Boolean => Ok(builder.with_booleans(array)),
        TypedValueEncodingField::Int => Ok(builder.with_ints(array)),
        TypedValueEncodingField::Integer => Ok(builder.with_integers(array)),
        TypedValueEncodingField::Float => Ok(builder.with_floats(array)),
        TypedValueEncodingField::Double => Ok(builder.with_doubles(array)),
        TypedValueEncodingField::Decimal => Ok(builder.with_decimals(array)),
        _ => exec_err!("Unsupported field for numeric fast path: {:?}", field),
    }
}
