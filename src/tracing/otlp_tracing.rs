use opentelemetry_otlp::SpanExporter;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::Sampler;
use opentelemetry_sdk::trace::SdkTracerProvider;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::errors::PythonException;

use super::TracerBuilder;
use super::TracingConfig;

/// Send traces to the OpenTelemetry collector.
///
/// See [OpenTelemetry collector
/// docs](https://opentelemetry.io/docs/collector/) for more info.
///
/// Only supports GRPC protocol, so make sure to enable it on your
/// OTEL configuration.
///
/// This is the recommended approach since it allows the maximum
/// flexibility in what to do with all the data bytewax can generate.
///
/// :arg `service_name`: Identifies this dataflow in OTLP.
///
/// :type `service_name`: str
///
/// :arg url: Connection info. Defaults to `"grpc:://127.0.0.1:4317"`.
///
/// :type url: str
///
/// :arg `sampling_ratio`: Fraction of traces to send between `0.0` and
///     `1.0`.
///
/// :type `sampling_ratio`: float
#[pyclass(module="bytewax.tracing", extends=TracingConfig, from_py_object)]
#[derive(Clone)]
pub(crate) struct OtlpTracingConfig {
    #[pyo3(get)]
    pub(crate) service_name: String,
    #[pyo3(get)]
    pub(crate) url: Option<String>,
    #[pyo3(get)]
    pub(crate) sampling_ratio: f64,
}

#[pymethods]
impl OtlpTracingConfig {
    #[new]
    #[pyo3(signature=(service_name, url=None, sampling_ratio=1.0))]
    const fn new(
        service_name: String,
        url: Option<String>,
        sampling_ratio: f64,
    ) -> (Self, TracingConfig) {
        let self_ = Self {
            service_name,
            url,
            sampling_ratio,
        };
        let super_ = TracingConfig::new();
        (self_, super_)
    }
}

impl TracerBuilder for OtlpTracingConfig {
    #[allow(clippy::significant_drop_tightening)]
    fn build(&self) -> PyResult<SdkTracerProvider> {
        // Build the OTLP span exporter
        let mut exporter_builder = SpanExporter::builder().with_tonic();

        // Change the url if required
        if let Some(endpoint) = self.url.as_ref() {
            exporter_builder = exporter_builder.with_endpoint(endpoint);
        }

        let exporter = exporter_builder
            .build()
            .raise::<PyRuntimeError>("error building OTLP exporter")?;

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
