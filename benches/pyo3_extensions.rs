//! Micro-benchmarks for TdPyAny wrapping overhead.
//!
//! Measures the per-operation cost of the Arc<SafePy<PyAny>> wrapping
//! used in bytewax's dataflow hot path. Each benchmark isolates one
//! operation to identify where overhead comes from.
//!
//! Run: cargo bench --bench pyo3_extensions

use std::hint::black_box;
use std::mem::ManuallyDrop;
use std::sync::Arc;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// Helpers — reproduce the wrapping layers from src/pyo3_extensions.rs
// without depending on pub(crate) types.
// ---------------------------------------------------------------------------

/// Mirrors `SafePy<T>` from src/pyo3_extensions.rs.
struct SafePy<T>(ManuallyDrop<Py<T>>);

impl<T> Drop for SafePy<T> {
    fn drop(&mut self) {
        #[cfg(Py_3_13)]
        if unsafe { pyo3::ffi::Py_IsFinalizing() } == 1 {
            return;
        }
        unsafe { ManuallyDrop::drop(&mut self.0) };
    }
}

impl<T> Clone for SafePy<T> {
    fn clone(&self) -> Self {
        Python::attach(|py| Self(ManuallyDrop::new(self.0.clone_ref(py))))
    }
}

impl<T> From<Py<T>> for SafePy<T> {
    fn from(obj: Py<T>) -> Self {
        Self(ManuallyDrop::new(obj))
    }
}

impl<T> SafePy<T> {
    fn into_inner(mut self) -> Py<T> {
        let inner = unsafe { ManuallyDrop::take(&mut self.0) };
        std::mem::forget(self);
        inner
    }
}

impl<T> std::ops::Deref for SafePy<T> {
    type Target = Py<T>;
    fn deref(&self) -> &Py<T> {
        &self.0
    }
}

/// Mirrors `TdPyAny` — current implementation with Arc.
#[derive(Clone)]
struct TdPyAnyArc(Arc<SafePy<PyAny>>);

/// Mirrors what TdPyAny would be without Arc — just SafePy.
#[derive(Clone)]
struct TdPyAnyDirect(SafePy<PyAny>);

// ---------------------------------------------------------------------------
// Benchmark helpers
// ---------------------------------------------------------------------------

fn init_python() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        Python::initialize();
    });
}

/// Create a fresh Python integer object for benchmarking.
fn make_pyobj() -> Py<PyAny> {
    init_python();
    Python::attach(|py| {
        42_i64
            .into_pyobject(py)
            .expect("into_pyobject")
            .into_any()
            .unbind()
    })
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_arc_wrap(c: &mut Criterion) {
    c.bench_function("tdpyany_arc_wrap", |b| {
        b.iter_batched(
            make_pyobj,
            |obj| black_box(TdPyAnyArc(Arc::new(SafePy::from(obj)))),
            BatchSize::SmallInput,
        );
    });
}

fn bench_direct_wrap(c: &mut Criterion) {
    c.bench_function("tdpyany_direct_wrap", |b| {
        b.iter_batched(
            make_pyobj,
            |obj| black_box(TdPyAnyDirect(SafePy::from(obj))),
            BatchSize::SmallInput,
        );
    });
}

fn bench_arc_unwrap_unique(c: &mut Criterion) {
    c.bench_function("tdpyany_arc_unwrap_unique", |b| {
        b.iter_batched(
            || TdPyAnyArc(Arc::new(SafePy::from(make_pyobj()))),
            |td| {
                let _: Py<PyAny> = match Arc::try_unwrap(td.0) {
                    Ok(safe) => safe.into_inner(),
                    Err(arc) => Python::attach(|py| (*arc).clone_ref(py)),
                };
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_arc_unwrap_shared(c: &mut Criterion) {
    c.bench_function("tdpyany_arc_unwrap_shared", |b| {
        b.iter_batched(
            || {
                let td = TdPyAnyArc(Arc::new(SafePy::from(make_pyobj())));
                let _hold = td.clone(); // keep a second reference
                (td, _hold)
            },
            |(td, _hold)| {
                let _: Py<PyAny> = match Arc::try_unwrap(td.0) {
                    Ok(safe) => safe.into_inner(),
                    Err(arc) => Python::attach(|py| (*arc).clone_ref(py)),
                };
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_direct_unwrap(c: &mut Criterion) {
    c.bench_function("tdpyany_direct_unwrap", |b| {
        b.iter_batched(
            || TdPyAnyDirect(SafePy::from(make_pyobj())),
            |td| {
                let _: Py<PyAny> = black_box(td.0.into_inner());
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_arc_clone(c: &mut Criterion) {
    let td = TdPyAnyArc(Arc::new(SafePy::from(make_pyobj())));
    c.bench_function("tdpyany_arc_clone", |b| {
        b.iter(|| black_box(td.clone()));
    });
}

fn bench_safepy_clone(c: &mut Criterion) {
    let safe = SafePy::from(make_pyobj());
    c.bench_function("safepy_clone", |b| {
        b.iter(|| black_box(safe.clone()));
    });
}

fn bench_baseline_py_clone(c: &mut Criterion) {
    let obj = make_pyobj();
    c.bench_function("baseline_py_clone", |b| {
        b.iter(|| {
            // py-clone feature: clone() acquires GIL internally
            Python::attach(|py| black_box(obj.clone_ref(py)))
        });
    });
}

fn bench_arc_roundtrip(c: &mut Criterion) {
    c.bench_function("roundtrip_arc", |b| {
        b.iter_batched(
            make_pyobj,
            |obj| {
                // wrap → clone → unwrap (simulates hot path with Arc)
                let td = TdPyAnyArc(Arc::new(SafePy::from(obj)));
                let td2 = td.clone();
                let _: Py<PyAny> = match Arc::try_unwrap(td2.0) {
                    Ok(safe) => safe.into_inner(),
                    Err(arc) => Python::attach(|py| (*arc).clone_ref(py)),
                };
                drop(td);
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_direct_roundtrip(c: &mut Criterion) {
    c.bench_function("roundtrip_direct", |b| {
        b.iter_batched(
            make_pyobj,
            |obj| {
                // wrap → clone → unwrap (simulates hot path without Arc)
                let td = TdPyAnyDirect(SafePy::from(obj));
                let td2 = td.clone();
                let _: Py<PyAny> = black_box(td2.0.into_inner());
                drop(td);
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    benches,
    // Wrapping: Py<PyAny> → TdPyAny
    bench_arc_wrap,
    bench_direct_wrap,
    // Unwrapping: TdPyAny → Py<PyAny>
    bench_arc_unwrap_unique,
    bench_arc_unwrap_shared,
    bench_direct_unwrap,
    // Cloning
    bench_arc_clone,
    bench_safepy_clone,
    bench_baseline_py_clone,
    // Full roundtrip (wrap → clone → unwrap)
    bench_arc_roundtrip,
    bench_direct_roundtrip,
);
criterion_main!(benches);
