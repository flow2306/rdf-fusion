use crate::EncodingName;
use crate::encoding::TermEncoding;
use crate::object_id::{ObjectIdArray, ObjectIdScalar};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use rdf_fusion_model::DFResult;
use std::clone::Clone;
use std::hash::Hash;

/// The [ObjectIdEncoding] represents each distinct term in the database with a single unique id.
/// We call such an id *object id*. Here is an example of the encoding:
///
/// ```text
/// ?variable
///
///  ┌─────┐
///  │   1 │ ────►  <#MyEntity>
///  ├─────┤
///  │   2 │ ────►  120^^xsd:integer
///  ├─────┤
///  │ ... │
///  └─────┘
/// ```
///
/// # Object ID Mapping
///
/// The mapping implementation depends on the storage layer that is being used. For example, an
/// in-memory RDF store will use a different implementation as an on-disk RDF store. The
/// [ObjectIdMapping](crate::object_id::ObjectIdMapping) trait defines the contract.
///
/// # Strengths and Weaknesses
///
/// The object id encoding is very well suited for evaluating joins, as instead of joining
/// variable-length RDF terms, we can directly join the object ids. While we do not have recent
/// numbers for the performance gains, the [original pull request](https://github.com/tobixdev/rdf-fusion/pull/27)
/// quadrupled the performance of some queries (with relatively small datasets!).
///
/// However, this also introduces the necessity of decoding the object ids back to RDF terms. For
/// example, by converting it to the [PlainTermEncoding](crate::plain_term::PlainTermEncoding).
/// For queries that spend little time on join operations, the cost of decoding the object ids can
/// outweigh the benefits of using the object id encoding.
///
/// Furthermore, the encoding introduces the necessity of maintaining the
/// [ObjectIdMapping](crate::object_id::ObjectIdMapping), which can be non-trivial.
///
/// # Current Limitation
///
/// Currently, this id is fixed to being a 32-bit integer. However, we have an
/// [issue](https://github.com/tobixdev/rdf-fusion/issues/50) that tracks the progress on limiting
/// this limitation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectIdEncoding {
    /// The number of bytes in a single object id.
    object_id_size: u8,
}

impl ObjectIdEncoding {
    /// Creates a new [ObjectIdEncoding].
    pub fn new(object_id_size: u8) -> Self {
        Self { object_id_size }
    }

    /// Returns the size of the object id.
    pub fn object_id_size(&self) -> u8 {
        self.object_id_size
    }
}

impl TermEncoding for ObjectIdEncoding {
    type Array = ObjectIdArray;
    type Scalar = ObjectIdScalar;

    fn name(&self) -> EncodingName {
        EncodingName::PlainTerm
    }

    fn data_type(&self) -> DataType {
        DataType::UInt32
    }

    fn try_new_array(&self, array: ArrayRef) -> DFResult<Self::Array> {
        ObjectIdArray::try_new(self.clone(), array)
    }

    fn try_new_scalar(&self, scalar: ScalarValue) -> DFResult<Self::Scalar> {
        ObjectIdScalar::try_new(self.clone(), scalar)
    }
}
