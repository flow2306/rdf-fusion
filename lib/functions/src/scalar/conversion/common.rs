use crate::scalar::ScalarSparqlOpArgs;
use datafusion::arrow::array::{Array, StringArray, StructArray};
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

    // candidates are convertible if homogenous
    let candidates: &[&dyn Array] = &[
        parts.booleans,
        parts.ints,
        parts.integers,
        parts.floats,
        parts.doubles,
        // parts.decimals,
        // parts.strings.value, // results in unexpected behavior
    ];

    for candidate in candidates {
        if parts.array.len() == candidate.len() {
            if let Ok(cast_arr) = compute::cast(*candidate, &target_type) {
                let builder = TypedValueArrayBuilder::new_with_single_type(
                    Arc::clone(&args.encoding),
                    target_field.type_id(),
                    candidate.len(),
                )?;

                let builder =
                    build_single_type_array(builder, target_field, Arc::new(cast_arr))?;
                let result = builder.finish().unwrap();

                return Ok(Some(ColumnarValue::Array(result.into_array_ref())));
            }
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
        TypedValueEncodingField::String => {
            let language_arr = Arc::new(StringArray::new_null(array.len()));
            let strings = StructArray::new(
                TypedValueEncoding::string_fields(),
                vec![array, language_arr],
                None,
            );
            Ok(builder.with_strings(Arc::new(strings)))
        }
        _ => exec_err!(
            "Unsupported field for numeric fast path: {:?}: in conversions/common.rs",
            field
        ),
    }
}
