use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use opentelemetry::global;
use opentelemetry_sdk::metrics::{Aggregation, Instrument, SdkMeterProvider, Stream};
use prometheus::default_registry;
use pyo3::{PyErr, PyResult, exceptions::PyRuntimeError};

/// Whether metrics collection is active. Set to `true` by
/// [`initialize_metrics`] when the webserver is enabled. When `false`,
/// [`with_timer!`] skips timing entirely to avoid `Instant::now()`
/// overhead in the hot path.
pub(crate) static METRICS_ENABLED: AtomicBool = AtomicBool::new(false);

#[macro_export]
macro_rules! with_timer {
    ($histogram: expr, $labels: expr, $body: expr) => {{
        if $crate::metrics::METRICS_ENABLED.load(std::sync::atomic::Ordering::Relaxed) {
            let now = std::time::Instant::now();
            let res = $body;
            $histogram.record(now.elapsed().as_secs_f64(), &$labels);
            res
        } else {
            $body
        }
    }};
}

/// Initialize the global registry for Prometheus metrics,
/// and create a global `SdkMeterProvider`.
pub(crate) fn initialize_metrics() -> PyResult<()> {
    // Initialize the global default registry for prometheus metrics
    // as internally it's a lazy static.
    let registry = default_registry();
    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .with_namespace("bytewax")
        .build()
        .map_err(|err| PyErr::new::<PyRuntimeError, _>(err.to_string()))?;

    // Create a global SdkMeterProvider
    let provider = SdkMeterProvider::builder()
        .with_reader(exporter)
        .with_view(
            opentelemetry_sdk::metrics::new_view(
                Instrument::new().name("*duration*"), // Must match histogram name
                Stream::new().aggregation(Aggregation::ExplicitBucketHistogram {
                    boundaries: vec![
                        0.0, 0.0005, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0,
                        2.5, 5.0, 7.5, 10.0,
                    ],
                    record_min_max: true,
                }),
            )
            .map_err(|err| PyErr::new::<PyRuntimeError, _>(err.to_string()))?,
        )
        .build();
    global::set_meter_provider(provider);

    METRICS_ENABLED.store(true, Ordering::Relaxed);

    Ok(())
}
