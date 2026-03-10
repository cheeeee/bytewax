import warnings

from bytewax.tracing import JaegerConfig, OtlpTracingConfig, setup_tracing
from pytest import raises


def test_setup_tracing_invalid_log_level():
    with raises(ValueError, match="Unknown log level"):
        setup_tracing(log_level="BOGUS")


def test_setup_tracing_empty_string_log_level():
    with raises(ValueError, match="Unknown log level"):
        setup_tracing(log_level="")


def test_otlp_config_defaults():
    config = OtlpTracingConfig(service_name="test")
    assert config.service_name == "test"
    assert config.url is None
    assert config.sampling_ratio == 1.0


def test_otlp_config_custom_url():
    config = OtlpTracingConfig(service_name="test", url="grpc://custom:4317")
    assert config.url == "grpc://custom:4317"


def test_otlp_config_sampling_ratio():
    config = OtlpTracingConfig(service_name="test", sampling_ratio=0.5)
    assert config.sampling_ratio == 0.5


def test_jaeger_config_defaults():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        config = JaegerConfig(service_name="test")
    assert config.service_name == "test"
    assert config.endpoint is None
    assert config.sampling_ratio == 1.0


def test_jaeger_config_custom_endpoint():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        config = JaegerConfig(service_name="test", endpoint="http://custom:4317")
    assert config.endpoint == "http://custom:4317"


def test_jaeger_config_sampling_ratio():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        config = JaegerConfig(service_name="test", sampling_ratio=0.5)
    assert config.sampling_ratio == 0.5


def test_jaeger_config_deprecation_warning():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        JaegerConfig(service_name="test")
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "deprecated" in str(w[0].message).lower()
        assert "OtlpTracingConfig" in str(w[0].message)
