use codspeed_criterion_compat::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::logical_expr::AggregateUDF;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
use rdf_fusion_encoding::typed_value::{
    TypedValueArrayElementBuilder, TypedValueEncoding,
};
use rdf_fusion_encoding::{EncodingArray, RdfFusionEncodings, TermEncoding};
use rdf_fusion_extensions::functions::{
    BuiltinName, FunctionName, RdfFusionFunctionRegistry,
};
use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
use rdf_fusion_model::{Float, Integer};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
enum AggregateScenario {
    AllInt,
    AllFloat,
}

impl AggregateScenario {
    fn create_args(&self, encodings: &RdfFusionEncodings) -> Vec<ArrayRef> {
        let mut builder =
            TypedValueArrayElementBuilder::new(Arc::clone(encodings.typed_value()));
        match self {
            AggregateScenario::AllInt => {
                for i in 0..8192 {
                    builder.append_integer(Integer::from(i)).unwrap();
                }
            }
            AggregateScenario::AllFloat => {
                for i in 0..8192 {
                    builder.append_float(Float::from(i as i16)).unwrap();
                }
            }
        }

        vec![builder.finish().into_array_ref()]
    }
}

fn bench_aggregates(c: &mut Criterion) {
    let encodings = RdfFusionEncodings::new(
        Arc::clone(&PLAIN_TERM_ENCODING),
        Arc::new(TypedValueEncoding::default()),
        None,
        Arc::clone(&SORTABLE_TERM_ENCODING),
    );

    let registry = DefaultRdfFusionFunctionRegistry::new(encodings.clone());

    let runs: HashMap<BuiltinName, Vec<AggregateScenario>> = HashMap::from([
        (
            BuiltinName::Sum,
            vec![AggregateScenario::AllInt, AggregateScenario::AllFloat],
        ),
        (
            BuiltinName::Avg,
            vec![AggregateScenario::AllInt, AggregateScenario::AllFloat],
        ),
    ]);

    for (my_built_in, scenarios) in runs {
        let implementation = registry.udaf(&FunctionName::Builtin(my_built_in)).unwrap();
        for scenario in scenarios {
            bench_aggregate_function(c, &encodings, &implementation, scenario);
        }
    }
}

fn bench_aggregate_function(
    c: &mut Criterion,
    encodings: &RdfFusionEncodings,
    function: &AggregateUDF,
    scenario: AggregateScenario,
) {
    let args = scenario.create_args(encodings);

    let input_field = Arc::new(Field::new(
        "input",
        encodings.typed_value().data_type().clone(),
        true,
    ));

    let return_field = Arc::new(Field::new(
        "result",
        encodings.typed_value().data_type().clone(),
        true,
    ));

    let schema = Schema::new(vec![(*input_field).clone()]);

    let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("input", 0));

    let name = format!("{}_{scenario:?}", function.name());

    c.bench_function(&name, |b| {
        b.iter(|| {
            let accumulator_args = AccumulatorArgs {
                return_field: return_field.clone(),
                schema: &schema,
                ignore_nulls: false,
                order_bys: &[],
                is_reversed: false,
                name: function.name(),
                is_distinct: false,
                exprs: &[expr.clone()],
                expr_fields: &[input_field.clone()],
            };

            let mut acc = function.accumulator(accumulator_args).unwrap();

            acc.update_batch(&args).unwrap();
            let _ = acc.evaluate().unwrap();
        });
    });
}

criterion_group!(aggregates, bench_aggregates);
criterion_main!(aggregates);
