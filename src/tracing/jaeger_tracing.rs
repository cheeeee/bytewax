use opentelemetry_otlp::SpanExporter;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::Sampler;
use opentelemetry_sdk::trace::SdkTracerProvider;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::TracerBuilder;
use super::TracingConfig;
use crate::errors::PythonException;

/// **Deprecated**: Use `OtlpTracingConfig` instead.
///
/// Configure tracing to send traces to a Jaeger instance via OTLP.
///
/// This class is kept for backward compatibility but now uses the OTLP
/// gRPC protocol internally. Jaeger v1.35+ supports OTLP natively.
///
/// :arg `service_name`: Identifies this dataflow in Jaeger.
///
/// :type `service_name`: str
///
/// :arg endpoint: OTLP gRPC endpoint. Defaults to
///     `"http://127.0.0.1:4317"`.
///
/// :type endpoint: str
///
/// :arg `sampling_ratio`: Fraction of traces to send between `0.0` and
///     `1.0`.
///
/// :type `sampling_ratio`: float
#[pyclass(module="bytewax.tracing", extends=TracingConfig, from_py_object)]
#[derive(Clone)]
pub(crate) struct JaegerConfig {
    #[pyo3(get)]
    service_name: String,
    #[pyo3(get)]
    endpoint: Option<String>,
    #[pyo3(get)]
    pub(crate) sampling_ratio: f64,
}

#[pymethods]
impl JaegerConfig {
    #[new]
    #[pyo3(signature=(service_name, endpoint=None, sampling_ratio=1.0))]
    fn new(
        service_name: String,
        endpoint: Option<String>,
        sampling_ratio: f64,
    ) -> PyResult<(Self, TracingConfig)> {
        Python::attach(|py| {
            let warnings = py.import("warnings")?;
            warnings.call_method1(
                "warn",
                (
                    "JaegerConfig is deprecated. Use OtlpTracingConfig instead. \
                     JaegerConfig now uses OTLP gRPC protocol internally and requires \
                     Jaeger v1.35+ with OTLP receiver enabled.",
                    py.get_type::<pyo3::exceptions::PyDeprecationWarning>(),
                    2i32, // stacklevel
                ),
            )?;
            PyResult::Ok(())
        })?;

        let self_ = Self {
            service_name,
            endpoint,
            sampling_ratio,
        };
        let super_ = TracingConfig::new();
        Ok((self_, super_))
    }
}

impl TracerBuilder for JaegerConfig {
    fn build(&self) -> PyResult<SdkTracerProvider> {
        // Set W3C TraceContext propagator
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        // Build the OTLP span exporter (Jaeger supports OTLP natively since v1.35)
        let endpoint = self.endpoint.as_deref().unwrap_or("http://127.0.0.1:4317");

        let exporter = SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint)
            .build()
            .raise::<PyRuntimeError>("error building OTLP exporter for Jaeger")?;

        // Build the resource
        let resource = Resource::builder()
            .with_service_name(self.service_name.clone())
            .build();

        // Build the tracer provider
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_sampler(Sampler::TraceIdRatioBased(self.sampling_ratio))
            .with_resource(resource)
            .build();

        Ok(provider)
    }
}
