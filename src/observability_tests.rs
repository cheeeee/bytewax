//! Process-isolated checks: both tracing and meter providers are global.

use std::io::{self, Write};
use std::process::Command;
use std::sync::{Arc, Mutex};

use opentelemetry::{global, sdk::metrics::MeterProvider, KeyValue};
use tracing::log::{LevelFilter, Log, Metadata, Record};
use tracing_subscriber::fmt::format::FmtSpan;

static LOGS: Mutex<Vec<String>> = Mutex::new(Vec::new());
struct Logger;
impl Log for Logger {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &Record<'_>) {
        LOGS.lock().unwrap().push(format!(
            "{} {} {}",
            record.level(),
            record.target(),
            record.args()
        ));
    }
    fn flush(&self) {}
}
static LOGGER: Logger = Logger;

#[derive(Clone)]
struct Buffer(Arc<Mutex<Vec<u8>>>);
impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn emit(optimized: bool) {
    let outer = if optimized {
        crate::debug_span_if_traced!(target: "bytewax::audit", "operator", step = "audit")
    } else {
        tracing::debug_span!(target: "bytewax::audit", "operator", step = "audit")
    };
    outer.in_scope(|| {
        let inner = if optimized {
            crate::trace_span_if_traced!(target: "bytewax::audit", "partition", key = "k")
        } else {
            tracing::trace_span!(target: "bytewax::audit", "partition", key = "k")
        };
        inner.in_scope(|| tracing::info!(target: "bytewax::audit", "body executed"));
    });
}

fn check_logging() {
    emit(true);
    tracing::log::set_logger(&LOGGER).unwrap();
    for level in [
        LevelFilter::Off,
        LevelFilter::Info,
        LevelFilter::Debug,
        LevelFilter::Trace,
        LevelFilter::Info,
    ] {
        tracing::log::set_max_level(level);
        emit(false);
        let expected = std::mem::take(&mut *LOGS.lock().unwrap());
        if level == LevelFilter::Trace {
            assert!(expected.iter().any(|line| line.contains("partition")));
        }
        emit(true);
        let actual = std::mem::take(&mut *LOGS.lock().unwrap());
        assert_eq!(actual, expected, "span-to-log output changed at {level}");
    }
}

fn check_subscriber(global_subscriber: bool, level: tracing::Level) {
    // Exercise calls before a subscriber exists, then register one later.
    emit(true);
    let buffer = Buffer(Arc::new(Mutex::new(Vec::new())));
    let writer = buffer.clone();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(level)
        .with_span_events(FmtSpan::NEW | FmtSpan::ENTER | FmtSpan::EXIT)
        .with_writer(move || writer.clone())
        .finish();
    let compare = move || {
        emit(false);
        let expected = std::mem::take(&mut *buffer.0.lock().unwrap());
        emit(true);
        let actual = std::mem::take(&mut *buffer.0.lock().unwrap());
        assert_eq!(actual, expected, "subscriber output changed");
        if level == tracing::Level::TRACE {
            assert!(String::from_utf8(actual).unwrap().contains("partition"));
        }
    };
    if global_subscriber {
        tracing::subscriber::set_global_default(subscriber).unwrap();
        std::thread::spawn(compare).join().unwrap();
    } else {
        tracing::subscriber::with_default(subscriber, compare);
    }
}

fn check_metrics(builtin: bool) {
    let registry = if builtin {
        crate::metrics::initialize_metrics().unwrap();
        prometheus::default_registry().clone()
    } else {
        let registry = prometheus::Registry::new();
        let exporter = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .build()
            .unwrap();
        global::set_meter_provider(MeterProvider::builder().with_reader(exporter).build());
        registry
    };
    let histogram = global::meter("audit")
        .f64_histogram("audit_duration")
        .init();
    let labels = [KeyValue::new("step", "audit")];
    assert_eq!(crate::with_timer!(histogram, labels, 42), 42);
    let failed: Result<(), &str> = crate::with_timer!(histogram, labels, Err("expected"));
    assert_eq!(failed, Err("expected"));
    let families = registry.gather();
    let family = families
        .iter()
        .find(|family| family.get_name().ends_with("audit_duration"))
        .expect("duration metric was not emitted");
    let metric = &family.get_metric()[0];
    assert_eq!(metric.get_histogram().get_sample_count(), 2);
    assert!(metric.get_histogram().get_sample_sum() >= 0.0);
    assert!(metric
        .get_label()
        .iter()
        .any(|label| { label.get_name() == "step" && label.get_value() == "audit" }));
}

#[test]
fn observability_contract() {
    const ENV: &str = "BYTEWAX_OBSERVABILITY_TEST_CASE";
    if let Ok(case) = std::env::var(ENV) {
        match case.as_str() {
            "log_only" => check_logging(),
            "global_trace" => check_subscriber(true, tracing::Level::TRACE),
            "global_error" => check_subscriber(true, tracing::Level::ERROR),
            "scoped_trace" => check_subscriber(false, tracing::Level::TRACE),
            "builtin_metrics" => check_metrics(true),
            "external_metrics" => check_metrics(false),
            "no_provider" => {
                let histogram = global::meter("audit")
                    .f64_histogram("audit_duration")
                    .init();
                assert_eq!(crate::with_timer!(histogram, [], 42), 42);
                assert!(prometheus::default_registry().gather().is_empty());
                emit(true);
            }
            _ => panic!("unknown case {case}"),
        }
        return;
    }
    let mut failed = Vec::new();
    for case in [
        "no_provider",
        "log_only",
        "global_trace",
        "global_error",
        "scoped_trace",
        "builtin_metrics",
        "external_metrics",
    ] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "observability_tests::observability_contract",
                "--nocapture",
            ])
            .env(ENV, case)
            .output()
            .unwrap();
        println!("{case}: {}", output.status);
        if !output.status.success() {
            eprintln!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            failed.push(case);
        }
    }
    assert!(failed.is_empty(), "failed cases: {failed:?}");
}
