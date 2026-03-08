"""Logging and tracing configuration.

.. deprecated::
    :py:obj:`JaegerConfig` is deprecated. Use :py:obj:`OtlpTracingConfig`
    instead. ``JaegerConfig`` now uses OTLP gRPC protocol internally
    and requires Jaeger v1.35+ with OTLP receiver enabled (default
    endpoint: ``http://127.0.0.1:4317``).
"""

from bytewax._bytewax import (
    JaegerConfig,
    OtlpTracingConfig,
    TracingConfig,
    setup_tracing,
)

__all__ = [
    "JaegerConfig",
    "OtlpTracingConfig",
    "TracingConfig",
    "setup_tracing",
]
