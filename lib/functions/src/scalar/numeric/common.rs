use crate::scalar::ScalarSparqlOpArgs;
use datafusion::arrow::array::{Array, Datum};
use datafusion::arrow::error::ArrowError;
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::typed_value::{
    TypedValueArrayElementBuilder, TypedValueArrayParts, TypedValueEncoding,
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

    let lhs_slot = detect_slot(&lhs_parts);
    let rhs_slot = detect_slot(&rhs_parts);

    let slot = match (lhs_slot, rhs_slot) {
        (Some(lhs_slot), Some(rhs_slot)) if lhs_slot == rhs_slot => lhs_slot,
        _ => return Ok(None),
    };

    let result = match slot {
        HomogeneousSlot::Floats => op(lhs_parts.floats, rhs_parts.floats),
        HomogeneousSlot::Doubles => op(lhs_parts.doubles, rhs_parts.doubles),
        HomogeneousSlot::Integers => op(lhs_parts.integers, rhs_parts.integers),
        HomogeneousSlot::Ints => op(lhs_parts.ints, rhs_parts.ints),
    };

    let result = match result {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };

    let mut array_builder =
        TypedValueArrayElementBuilder::new(Arc::clone(&args.encoding));

    match slot {
        HomogeneousSlot::Integers => {
            let result = result
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .expect("expected Int64Array from arithmetic kernel");

            for value in result.values() {
                array_builder.append_integer((*value).into())?;
            }
        }
        HomogeneousSlot::Floats => {
            let result = result
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Float32Array>()
                .expect("expected Float32Array from arithmetic kernel");

            for value in result.values() {
                array_builder.append_float((*value).into())?;
            }
        }
        HomogeneousSlot::Doubles => {
            let result = result
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Float64Array>()
                .expect("expected Float64Array from arithmetic kernel");

            for value in result.values() {
                array_builder.append_double((*value).into())?;
            }
        }
        HomogeneousSlot::Ints => {
            let result = result
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .expect("expected Int32Array from arithmetic kernel");

            for value in result.values() {
                array_builder.append_int((*value).into())?;
            }
        }
    }

    Ok(Some(ColumnarValue::Array(
        array_builder.finish().into_array_ref(),
    )))
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum HomogeneousSlot {
    Integers,
    Floats,
    Doubles,
    Ints,
}

fn detect_slot(parts: &TypedValueArrayParts) -> Option<HomogeneousSlot> {
    let len = parts.array.len();

    if parts.integers.len() == len {
        Some(HomogeneousSlot::Integers)
    } else if parts.floats.len() == len {
        Some(HomogeneousSlot::Floats)
    } else if parts.doubles.len() == len {
        Some(HomogeneousSlot::Doubles)
    } else if parts.ints.len() == len {
        Some(HomogeneousSlot::Ints)
    } else {
        None
    }
}
