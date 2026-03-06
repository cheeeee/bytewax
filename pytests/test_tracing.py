from bytewax.tracing import setup_tracing
from pytest import raises


def test_setup_tracing_invalid_log_level():
    with raises(ValueError, match="Unknown log level"):
        setup_tracing(log_level="BOGUS")


def test_setup_tracing_empty_string_log_level():
    with raises(ValueError, match="Unknown log level"):
        setup_tracing(log_level="")
