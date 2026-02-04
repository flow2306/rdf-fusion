use crate::scalar::ScalarSparqlOpArgs;
use datafusion::arrow::array::{Array, Datum};
use datafusion::arrow::error::ArrowError;
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::typed_value::{
    TypedValueArrayElementBuilder, TypedValueEncoding,
};
use rdf_fusion_model::AResult;
use std::sync::Arc;

pub fn try_arithmetic_fast_path(
    args: &ScalarSparqlOpArgs<TypedValueEncoding>,
    op: fn(&dyn Datum, &dyn Datum) -> Result<Arc<dyn Array>, ArrowError>,
) -> AResult<Option<ColumnarValue>> {
    let lhs = args.args[0].to_array();
    let rhs = args.args[1].to_array();
    let lhs_parts = lhs.parts_as_ref();
    let rhs_parts = rhs.parts_as_ref();

    let lhs_is_all_ints = lhs_parts.array.len() == lhs_parts.integers.len();
    let rhs_is_all_ints = rhs_parts.array.len() == rhs_parts.integers.len();

    if !lhs_is_all_ints || !rhs_is_all_ints {
        return Ok(None);
    }

    let result: Arc<dyn Array> = op(lhs_parts.integers, rhs_parts.integers)?.into();

    let result = result
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("expected Int64Array from arithmetic kernel");

    let mut array_builder =
        TypedValueArrayElementBuilder::new(Arc::clone(&args.encoding));
    for value in result.values() {
        array_builder.append_integer((*value).into())?;
    }

    Ok(Some(ColumnarValue::Array(
        array_builder.finish().into_array_ref(),
    )))
}
