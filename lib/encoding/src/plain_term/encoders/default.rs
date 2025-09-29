use crate::encoding::TermEncoder;
use crate::plain_term::{PlainTermArrayElementBuilder, PlainTermEncoding};
use crate::{EncodingArray, TermEncoding};
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{TermRef, ThinResult};

#[derive(Debug)]
pub struct DefaultPlainTermEncoder;

impl TermEncoder<PlainTermEncoding> for DefaultPlainTermEncoder {
    type Term<'data> = TermRef<'data>;

    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<PlainTermEncoding as TermEncoding>::Array> {
        let mut value_builder = PlainTermArrayElementBuilder::default();
        for value in terms {
            match value {
                Ok(TermRef::NamedNode(value)) => value_builder.append_named_node(value),
                Ok(TermRef::BlankNode(value)) => value_builder.append_blank_node(value),
                Ok(TermRef::Literal(value)) => value_builder.append_literal(value),
                Err(_) => value_builder.append_null(),
            }
        }
        Ok(value_builder.finish())
    }

    fn encode_term(
        term: ThinResult<Self::Term<'_>>,
    ) -> DFResult<<PlainTermEncoding as TermEncoding>::Scalar> {
        Self::encode_terms([term])?.try_as_scalar(0)
    }
}
