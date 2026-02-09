use crate::scalar::ScalarSparqlOpArgs;
use datafusion::arrow::array::{Array, BooleanArray, Datum};
use datafusion::arrow::error::ArrowError;
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::typed_value::{
    TypedValueArrayElementBuilder, TypedValueArrayParts, TypedValueEncoding,
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

    let lhs_slot = detect_slot(&lhs_parts);
    let rhs_slot = detect_slot(&rhs_parts);

    let slot = match (lhs_slot, rhs_slot) {
        (Some(lhs_slot), Some(rhs_slot)) if lhs_slot == rhs_slot => lhs_slot,
        _ => return Ok(None),
    };

    let result = match slot {
        HomogeneousSlot::Floats => cmp(lhs_parts.floats, rhs_parts.floats)
            .expect("Arrays have the same type and length"),
        HomogeneousSlot::Doubles => cmp(lhs_parts.doubles, rhs_parts.doubles)
            .expect("Arrays have the same type and length"),
        HomogeneousSlot::Booleans => cmp(lhs_parts.booleans, rhs_parts.booleans)
            .expect("Arrays have the same type and length"),
        HomogeneousSlot::Integers => cmp(lhs_parts.integers, rhs_parts.integers)
            .expect("Arrays have the same type and length"),
        HomogeneousSlot::Ints => cmp(lhs_parts.ints, rhs_parts.ints)
            .expect("Arrays have the same type and length"),
    };

    let mut array_builder =
        TypedValueArrayElementBuilder::new(Arc::clone(&args.encoding));
    for value in result.values() {
        array_builder.append_boolean(value.into())?;
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
    Booleans,
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
    } else if parts.booleans.len() == len {
        Some(HomogeneousSlot::Booleans)
    } else if parts.ints.len() == len {
        Some(HomogeneousSlot::Ints)
    } else {
        None
    }
}
