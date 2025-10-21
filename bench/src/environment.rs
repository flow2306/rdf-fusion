use crate::BenchmarkingOptions;
use crate::benchmarks::BenchmarkName;
use crate::prepare::{PrepRequirement, prepare_run_closure, prepare_run_command};
use crate::prepare::{ensure_file_download, prepare_file_download};
use anyhow::bail;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::SessionConfig;
use rdf_fusion::store::Store;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Represents a context used to execute benchmarks.
pub struct RdfFusionBenchContext {
    /// General options for the benchmarks.
    options: BenchmarkingOptions,
    /// The path to the data dir.
    data_dir: Mutex<PathBuf>,
    /// The path to the results dir.
    results_dir: Mutex<PathBuf>,
}

impl RdfFusionBenchContext {
    /// Creates a new [RdfFusionBenchContext].
    pub fn new(
        options: BenchmarkingOptions,
        data_dir: PathBuf,
        results_dir: PathBuf,
    ) -> Self {
        Self {
            options,
            data_dir: Mutex::new(data_dir),
            results_dir: Mutex::new(results_dir),
        }
    }

    /// Creates a new [RdfFusionBenchContext] used in the criterion benchmarks.
    pub fn new_for_criterion(data_dir: PathBuf, target_partitions: usize) -> Self {
        Self {
            options: BenchmarkingOptions {
                verbose_results: false,
                target_partitions: Some(target_partitions),
                memory_size: None,
            },
            data_dir: Mutex::new(data_dir),
            results_dir: Mutex::new(PathBuf::from("/temp")),
        }
    }

    /// Returns the [BenchmarkingOptions] for this context.
    pub fn options(&self) -> &BenchmarkingOptions {
        &self.options
    }

    /// Resolves a relative path `file` against the data directory.
    pub fn join_data_dir(&self, file: &Path) -> anyhow::Result<PathBuf> {
        if !file.is_relative() {
            bail!("Only relative paths can be resolved.")
        }

        Ok(self.data_dir.lock().unwrap().join(file))
    }

    pub fn create_store(&self) -> Store {
        let mut config = SessionConfig::new()
            .with_batch_size(8192)
            .with_target_partitions(self.options.target_partitions.unwrap_or(1));

        let options = config.options_mut();
        options.optimizer.enable_dynamic_filter_pushdown = true;

        let runtime_enc = match self.options.memory_size {
            None => Arc::new(RuntimeEnv::default()),
            Some(memory_size) => RuntimeEnvBuilder::new()
                .with_memory_limit(memory_size * 1024 * 1024, 1f64)
                .build_arc()
                .expect("Only setting memory limit"),
        };
        Store::new_with_datafusion_config(config, runtime_enc)
    }

    /// Creates a new folder in the results directory and uses it until [Self::pop_dir] is
    /// called.
    ///
    /// This can be used to create folder hierarchies to separate the results of different
    /// benchmarks.
    #[allow(clippy::create_dir)]
    #[allow(clippy::unwrap_used, reason = "Mutex poisoning")]
    pub fn push_dir(
        &self,
        data_dir_name: &str,
        results_dir_name: &str,
    ) -> anyhow::Result<()> {
        let mut data_dir = self.data_dir.lock().unwrap();
        let mut results_dir = self.results_dir.lock().unwrap();

        data_dir.push(data_dir_name);
        results_dir.push(results_dir_name);

        Ok(())
    }

    /// Pops the last directory from the stack.
    pub fn pop_dir(&self) {
        let mut data_dir = self.data_dir.lock().unwrap();
        let mut results_dir = self.results_dir.lock().unwrap();

        data_dir.pop();
        results_dir.pop();
    }

    /// Creates a new bencher and modifies the context for this benchmark.
    pub fn create_benchmark_context(
        &self,
        benchmark_name: BenchmarkName,
    ) -> anyhow::Result<BenchmarkContext<'_>> {
        self.push_dir(
            &benchmark_name.data_dir_name(),
            &benchmark_name.results_dir_name(),
        )?;
        Ok(BenchmarkContext {
            context: self,
            benchmark_name,
        })
    }
}

/// A benchmarker that can be used to execute benchmarks.
///
/// It holds a reference to the current context to store its results.
pub struct BenchmarkContext<'ctx> {
    /// Reference to the benchmarking context.
    context: &'ctx RdfFusionBenchContext,
    /// Name of the benchmark that is being executed.
    benchmark_name: BenchmarkName,
}

impl<'ctx> BenchmarkContext<'ctx> {
    /// Returns a reference to the benchmarking context.
    pub fn parent(&self) -> &RdfFusionBenchContext {
        self.context
    }

    /// Returns the name of the benchmark that is being executed.
    pub fn benchmark_name(&self) -> BenchmarkName {
        self.benchmark_name
    }

    /// Returns the path to the results directory of this benchmark.
    pub fn data_dir(&self) -> PathBuf {
        self.context.data_dir.lock().unwrap().clone()
    }

    /// Returns the path to the results directory of this benchmark.
    pub fn results_dir(&self) -> PathBuf {
        self.context.results_dir.lock().unwrap().clone()
    }

    /// Prepares the context such that `requirement` is fulfilled.
    pub async fn prepare_requirement(
        &self,
        requirement: PrepRequirement,
    ) -> anyhow::Result<()> {
        match requirement {
            PrepRequirement::FileDownload {
                url,
                file_name,
                action,
            } => prepare_file_download(self, url, file_name, action).await,
            PrepRequirement::RunClosure { execute, .. } => {
                prepare_run_closure(self, &execute)
            }
            PrepRequirement::RunCommand {
                workdir,
                program,
                args,
                ..
            } => {
                let workdir = self.context.join_data_dir(&workdir)?;
                prepare_run_command(&workdir, &program, &args)
            }
        }
    }

    /// Ensures that the `requirement` is fulfilled in this context.
    pub fn ensure_requirement(&self, requirement: PrepRequirement) -> anyhow::Result<()> {
        match requirement {
            PrepRequirement::FileDownload { file_name, .. } => {
                ensure_file_download(self, file_name.as_path())
            }
            PrepRequirement::RunClosure {
                check_requirement, ..
            }
            | PrepRequirement::RunCommand {
                check_requirement, ..
            } => check_requirement(self),
        }
    }
}

/// Pops the results directory from the context when the bencher is dropped.
impl Drop for BenchmarkContext<'_> {
    fn drop(&mut self) {
        self.context.pop_dir();
    }
}
