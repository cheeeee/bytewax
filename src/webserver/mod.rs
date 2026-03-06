use axum::{
    Router,
    extract::Extension,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};

use prometheus::{TextEncoder, default_registry};
use pyo3::{exceptions::PyRuntimeError, exceptions::PyValueError, prelude::*};
use std::{net::SocketAddr, sync::Arc};

use crate::errors::PythonException;

struct State {
    dataflow_json: String,
}

pub(crate) async fn run_webserver(dataflow_json: String) -> PyResult<()> {
    let shared_state = Arc::new(State { dataflow_json });

    let app = Router::new()
        .route("/dataflow", get(get_dataflow))
        .route("/metrics", get(get_metrics))
        .layer(Extension(shared_state));

    let port: u16 = std::env::var("BYTEWAX_DATAFLOW_API_PORT")
        .ok()
        .map(|var| {
            var.parse::<u16>()
                .raise::<PyValueError>("unable to parse BYTEWAX_DATAFLOW_API_PORT as u16")
        })
        .transpose()?
        .unwrap_or(3030);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Starting Dataflow API server on {addr:?}");

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .map_err(|err| err.to_string())
        .raise_with::<PyRuntimeError>(|| format!("Unable to create local webserver at port {port}"))
}

async fn get_dataflow(Extension(state): Extension<Arc<State>>) -> impl IntoResponse {
    // We are building a custom response here, as the returned value
    // from our helper function is JSON formatted string.
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(state.dataflow_json.clone())
        .unwrap()
}

async fn get_metrics() -> Response {
    let py_metrics: String = match Python::attach(|py| -> PyResult<String> {
        let metrics_mod = PyModule::import(py, "bytewax._metrics")?;
        let metrics = metrics_mod
            .getattr("generate_python_metrics")?
            .call0()?
            .extract()?;
        Ok(metrics)
    }) {
        Ok(metrics) => metrics,
        Err(err) => {
            tracing::error!("Failed to generate Python metrics: {err}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to generate Python metrics: {err}"),
            )
                .into_response();
        }
    };
    let metric_families = default_registry().gather();
    let encoder = TextEncoder::new();
    let rust_metrics = match encoder.encode_to_string(&metric_families) {
        Ok(m) => m,
        Err(err) => {
            tracing::error!("Failed to encode Prometheus metrics: {err}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to encode metrics: {err}"),
            )
                .into_response();
        }
    };

    format!("{rust_metrics}{py_metrics}").into_response()
}
