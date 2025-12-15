use crate::scalar::ScalarSparqlOpArgs;
use datafusion::arrow::array::{Array, BooleanArray, Datum};
use datafusion::arrow::error::ArrowError;
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::typed_value::{
    TypedValueArrayElementBuilder, TypedValueEncoding,
};
use rdf_fusion_model::AResult;
use std::sync::Arc;

pub fn try_cmp_fast_path(
    args: &ScalarSparqlOpArgs<TypedValueEncoding>,
    cmp: fn(&dyn Datum, &dyn Datum) -> Result<BooleanArray, ArrowError>,
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

    let result = cmp(lhs_parts.integers, rhs_parts.integers)
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
