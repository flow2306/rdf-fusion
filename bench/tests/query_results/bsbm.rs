//! This file contains tests for an adapte version of the BSBM explore use case. The adaption have
//! been made such that the queries produce stable results.
//!
//! The results have been compared against GraphDB 11.0.1. The following curl command can be used to
//! get the results of GraphDB. Ideally, one can pipe the result of the request into a file or
//! directly to the clipboard using `| xclip -selection clipboard` (or similar). You can also
//! download the JSON result from the GraphDB UI.
//!
//! ```bash
//! curl -X POST \
//!     "http://<graphdb_url>/repositories/bsbm" \
//!     -H "Content-Type: application/sparql-query" \
//!     -H "Accept: application/sparql-results+json" \
//!     --data-binary '<query>'
//! ```
//!
//! Then, even though we pretty-print our results, there will be some differences (e.g., spacing,
//! order of keys). You can use a tool that semantically compares JSON files to "quickly" check
//! the results of a new test (e.g., [JSON Compare](https://jsoncompare.org/)). `CONSTRUCT` queries
//! have been compared manually.

use crate::query_results::{run_graph_result_query, run_select_query};
use insta::assert_snapshot;
use rdf_fusion_bench::benchmarks::Benchmark;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, ExploreUseCase, NumProducts};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::PathBuf;

#[tokio::test]
pub async fn bsbm_1000_test_results() {
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), 1);
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(NumProducts::N1_000, None).unwrap();
    let benchmark_name = benchmark.name();
    let ctx = benchmarking_context
        .create_benchmark_context(benchmark_name)
        .unwrap();

    let store = benchmark.prepare_store(&ctx).await.unwrap();

    //
    // Explore
    //

    assert_snapshot!(
        "Explore Q1",
        run_select_query(&store, include_str!("./queries/explore-q1.sparql")).await
    );
    assert_snapshot!(
        "Explore Q2 (empty optional)",
        run_select_query(
            &store,
            include_str!("./queries/explore-q2-empty-optional.sparql")
        )
        .await
    );
    assert_snapshot!(
        "Explore Q2 (non-empty optional)",
        run_select_query(
            &store,
            include_str!("./queries/explore-q2-non-empty-optional.sparql")
        )
        .await
    );
    assert_snapshot!(
        "Explore Q3",
        run_select_query(&store, include_str!("./queries/explore-q3.sparql")).await
    );
    assert_snapshot!(
        "Explore Q4",
        run_select_query(&store, include_str!("./queries/explore-q4.sparql")).await
    );
    assert_snapshot!(
        "Explore Q5",
        run_select_query(&store, include_str!("./queries/explore-q5.sparql")).await
    );
    assert_snapshot!(
        "Explore Q7",
        run_select_query(&store, include_str!("./queries/explore-q7.sparql")).await
    );
    assert_snapshot!(
        "Explore Q8",
        run_select_query(&store, include_str!("./queries/explore-q8.sparql")).await
    );
    assert_snapshot!(
        "Explore Q9",
        run_graph_result_query(&store, include_str!("./queries/explore-q9.sparql")).await
    );
    assert_snapshot!(
        "Explore Q10",
        run_select_query(&store, include_str!("./queries/explore-q10.sparql")).await
    );
    assert_snapshot!(
        "Explore Q11",
        run_select_query(&store, include_str!("./queries/explore-q11.sparql")).await
    );
    assert_snapshot!(
        "Explore Q12",
        run_graph_result_query(&store, include_str!("./queries/explore-q12.sparql"))
            .await
    );

    //
    // Business Intelligence
    //

    assert_snapshot!(
        "Business Intelligence Q1",
        run_select_query(&store, include_str!("./queries/bi-q1.sparql")).await
    );
    assert_snapshot!(
        "Business Intelligence Q2",
        run_select_query(&store, include_str!("./queries/bi-q2.sparql")).await
    );
    assert_snapshot!(
        "Business Intelligence Q3",
        run_select_query(&store, include_str!("./queries/bi-q3.sparql")).await
    );
    assert_snapshot!(
        "Business Intelligence Q4",
        run_select_query(&store, include_str!("./queries/bi-q4.sparql")).await
    );
    assert_snapshot!(
        "Business Intelligence Q5",
        run_select_query(&store, include_str!("./queries/bi-q5.sparql")).await
    );
    assert_snapshot!(
        "Business Intelligence Q6",
        run_select_query(&store, include_str!("./queries/bi-q6.sparql")).await
    );
    assert_snapshot!(
        "Business Intelligence Q7",
        run_select_query(&store, include_str!("./queries/bi-q7.sparql")).await
    );
    assert_snapshot!(
        "Business Intelligence Q8",
        run_select_query(&store, include_str!("./queries/bi-q8.sparql")).await
    );
}
