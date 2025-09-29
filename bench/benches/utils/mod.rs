use anyhow::Context;
use futures::StreamExt;
use rdf_fusion::execution::results::QueryResults;
use tokio::runtime::{Builder, Runtime};

pub mod verbose;

pub async fn consume_results(result: QueryResults) -> anyhow::Result<usize> {
    match result {
        QueryResults::Solutions(solutions) => {
            let mut inner = solutions
                .into_record_batch_stream()
                .context("Failed to convert solutions to record batch stream")?;

            let mut count = 0;
            while let Some(sol) = inner.next().await {
                count += sol.context("Error while getting record batch.")?.num_rows();
            }
            Ok(count)
        }
        QueryResults::Graph(mut triples) => {
            let mut count = 0;
            while let Some(sol) = triples.next().await {
                sol.context("Error while getting triple.")?;
                count += 1;
            }
            Ok(count)
        }
        _ => panic!("Unexpected QueryResults"),
    }
}

pub fn create_runtime(target_partitions: usize) -> Runtime {
    let force_single_thred =
        std::env::var("RDF_FUSION_SINGLE_THREAD_BENCH") == Ok("true".to_string());
    if target_partitions == 1 || force_single_thred {
        Builder::new_current_thread().enable_all().build().unwrap()
    } else {
        Builder::new_multi_thread()
            .worker_threads(target_partitions)
            .enable_all()
            .build()
            .unwrap()
    }
}
