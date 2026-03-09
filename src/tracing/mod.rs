//! Internal code for tracing/logging.
//!
//! This module is used to configure both tracing and logging.
//! Tracing and logging can be configured by the user, by default
//! they are disabled.
//!
//! Each tracing backend has to implement the `TracerBuilder` trait, which
//! requires a `build` function that is used to build the telemetry layer.

use std::time::Duration;

use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::Registry;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;

pub(crate) mod jaeger_tracing;
pub(crate) mod otlp_tracing;

pub(crate) use jaeger_tracing::JaegerConfig;
pub(crate) use otlp_tracing::OtlpTracingConfig;

use crate::errors::PythonException;
use crate::errors::tracked_err;
use crate::pyo3_extensions::PyConfigClass;

/// Base class for tracing/logging configuration.
///
/// There defines what to do with traces and logs emitted by Bytewax.
///
/// Use a specific subclass of this to configure where you want the
/// traces to go.
#[pyclass(module = "bytewax.tracing", subclass)]
pub(crate) struct TracingConfig;

#[pymethods]
impl TracingConfig {
    #[new]
    const fn new() -> Self {
        Self {}
    }
}

/// Trait that all the tracing config should implement.
/// This function should return a configured `SdkTracerProvider` for the backend.
pub(crate) trait TracerBuilder {
    fn build(&self) -> PyResult<SdkTracerProvider>;
}

impl PyConfigClass<Box<dyn TracerBuilder + Send>> for Py<TracingConfig> {
    #[allow(clippy::option_if_let_else)]
    fn downcast(&self, py: Python) -> PyResult<Box<dyn TracerBuilder + Send>> {
        if let Ok(otlp_conf) = self.extract::<OtlpTracingConfig>(py) {
            Ok(Box::new(otlp_conf))
        } else if let Ok(jaeger_conf) = self.extract::<JaegerConfig>(py) {
            Ok(Box::new(jaeger_conf))
        } else {
            let pytype = self.bind(py).get_type();
            Err(tracked_err::<PyTypeError>(&format!(
                "Unknown tracing_config type: {pytype}"
            )))
        }
    }
}

/// Utility class used to handle tracing.
///
/// It keeps a tokio runtime that is alive as long as the struct itself.
///
/// This should only be built via `setup_tracing`.
#[pyclass]
struct BytewaxTracer {
    rt: Option<tokio::runtime::Runtime>,
}

impl Drop for BytewaxTracer {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            // Use a timeout to prevent hanging in forked child processes
            // where tokio worker threads no longer exist after fork().
            rt.shutdown_timeout(Duration::from_millis(100));
        }
    }
}

#[allow(clippy::option_if_let_else)]
fn get_log_level(level: Option<String>) -> PyResult<LevelFilter> {
    if let Some(level) = level {
        match level.to_lowercase().as_str() {
            "trace" => Ok(LevelFilter::TRACE),
            "debug" => Ok(LevelFilter::DEBUG),
            "info" => Ok(LevelFilter::INFO),
            "warn" => Ok(LevelFilter::WARN),
            "error" => Ok(LevelFilter::ERROR),
            level => Err(tracked_err::<PyValueError>(&format!(
                "Unknown log level: {level}"
            ))),
        }
    } else {
        Ok(LevelFilter::ERROR)
    }
}

/// Synchronous setup for logging only (no tracing backend).
/// No tokio runtime needed — zero extra threads.
fn setup_logging_only(log_level: LevelFilter) -> PyResult<()> {
    let logs = tracing_subscriber::fmt::Layer::default()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_names(true)
        .with_filter(Targets::new().with_target("bytewax", log_level));
    tracing::subscriber::set_global_default(Registry::default().with(logs))
        .raise::<PyRuntimeError>("error setting global default tracer")
}

/// Async setup for tracing with a backend (OTLP/Jaeger).
/// Requires a tokio runtime for `install_batch(Tokio)` background export.
#[allow(clippy::unused_async)]
async fn setup_with_tracer(
    log_level: LevelFilter,
    tracer: Box<dyn TracerBuilder + Send>,
) -> PyResult<()> {
    let logs = tracing_subscriber::fmt::Layer::default()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_names(true)
        .with_filter(Targets::new().with_target("bytewax", log_level));
    let provider = tracer.build().reraise("error building tracer")?;
    let otel_tracer = provider.tracer("bytewax");
    let telemetry = tracing_opentelemetry::layer()
        .with_tracer(otel_tracer)
        .with_filter(Targets::new().with_target("bytewax", LevelFilter::TRACE));
    tracing::subscriber::set_global_default(Registry::default().with(logs).with(telemetry))
        .raise::<PyRuntimeError>("error setting global default tracer")
}

impl BytewaxTracer {
    /// Call this with a `TracingConfig` subclass to configure tracing.
    /// Returns a guard that you have to keep in scope for the
    /// whole execution of the code you want to trace.
    pub(crate) fn setup(
        &self,
        tracer: Option<Box<dyn TracerBuilder + Send>>,
        log_level: Option<String>,
    ) -> PyResult<()> {
        let log_level = get_log_level(log_level)?;

        if let Some(tracer) = tracer {
            let rt = self
                .rt
                .as_ref()
                .ok_or_else(|| tracked_err::<PyRuntimeError>("tracing runtime was shut down"))?;
            rt.block_on(rt.spawn(setup_with_tracer(log_level, tracer)))
                .map_err(|err| {
                    tracked_err::<PyRuntimeError>(&format!("error setting up tracing: {err}"))
                })?
        } else {
            setup_logging_only(log_level)
        }
    }
}

/// Setup Bytewax's internal tracing and logging.
///
/// By default it starts a tracer that logs all `ERROR`-level messages
/// to stdout.
///
/// Note: To make this work, you have to keep a reference of the
/// returned object.
///
/// % Skip this doctest because it requires starting the webserver.
///
/// ```python
/// from bytewax.tracing import setup_tracing
///
/// tracer = setup_tracing()
/// ```
///
/// :arg `tracing_config`: The specific backend you want to use.
///
/// :type `tracing_config`: bytewax.tracing.TracingConfig
///
/// :arg `log_level`: String of the log level. One of `"ERROR"`,
///     `"WARN"`, `"INFO"`, `"DEBUG"`, `"TRACE"`. Defaults to
///     `"ERROR"`.
///
/// :type `log_level`: str
#[pyfunction]
#[pyo3(signature = (tracing_config=None, log_level=None))]
fn setup_tracing(
    py: Python<'_>,
    tracing_config: Option<Py<TracingConfig>>,
    log_level: Option<String>,
) -> PyResult<Bound<'_, BytewaxTracer>> {
    let builder = tracing_config.map(|conf| conf.downcast(py)).transpose()?;

    // Only create a tokio runtime when a tracing backend is configured.
    // Logging-only mode is fully synchronous — zero extra threads.
    let rt = if builder.is_some() {
        Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .raise::<PyRuntimeError>("error building tokio runtime")?,
        )
    } else {
        None
    };

    let tracer = Bound::new(py, BytewaxTracer { rt })?;
    tracer.borrow().setup(builder, log_level)?;
    Ok(tracer)
}

pub(crate) fn register(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TracingConfig>()?;
    m.add_class::<JaegerConfig>()?;
    m.add_class::<OtlpTracingConfig>()?;
    m.add_class::<BytewaxTracer>()?;
    m.add_function(wrap_pyfunction!(setup_tracing, m)?)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pyo3::exceptions::PyValueError;

    #[test]
    fn get_log_level_valid_levels() {
        assert_eq!(
            get_log_level(Some("TRACE".to_string())).unwrap(),
            LevelFilter::TRACE
        );
        assert_eq!(
            get_log_level(Some("debug".to_string())).unwrap(),
            LevelFilter::DEBUG
        );
        assert_eq!(
            get_log_level(Some("Info".to_string())).unwrap(),
            LevelFilter::INFO
        );
        assert_eq!(
            get_log_level(Some("WARN".to_string())).unwrap(),
            LevelFilter::WARN
        );
        assert_eq!(
            get_log_level(Some("error".to_string())).unwrap(),
            LevelFilter::ERROR
        );
    }

    #[test]
    fn get_log_level_none_defaults_to_error() {
        assert_eq!(get_log_level(None).unwrap(), LevelFilter::ERROR);
    }

    #[test]
    fn get_log_level_invalid_returns_error() {
        pyo3::Python::initialize();
        Python::attach(|py| {
            let result = get_log_level(Some("bogus".to_string()));
            assert!(result.is_err());
            assert!(result.unwrap_err().is_instance_of::<PyValueError>(py));
        });
    }
}
