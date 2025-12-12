"""Tests for synchronous APIs."""

import signal
import time
from unittest.mock import Mock, patch
import pytest
from rembus.sync import receive_signal, node


def test_receive_signal():
    """Test that receive_signal calls close on the handle."""
    mock_handle = Mock()
    mock_handle.close = Mock()

    receive_signal(mock_handle)

    mock_handle.close.assert_called_once()


def test_receive_signal_with_node():
    """Test receive_signal with an actual node instance."""
    rb = node()

    # Spy on the close method
    original_close = rb.close
    rb.close = Mock(side_effect=original_close)

    try:
        receive_signal(rb)

        rb.close.assert_called_once()
    except Exception:
        if rb._runner is not None:
            rb.close()
        raise


def test_sigint_handler():
    """Test that SIGINT handler is properly registered."""
    with patch('signal.signal') as mock_signal:
        rb = node()
        # Verify signal.signal was called with SIGINT
        calls = mock_signal.call_args_list
        sigint_call = None
        for call in calls:
            if call[0][0] == signal.SIGINT:
                sigint_call = call
                break
        assert sigint_call is not None, "SIGINT handler not registered"
        # Get the handler function
        handler = sigint_call[0][1]
        rb.close = Mock()
        handler(signal.SIGINT, None)
        rb.close.assert_called_once()
        if rb._runner is not None:
            rb._runner.shutdown()
            rb._runner = None

        rb.router.db.close()


def test_node_context_manager():
    """Test that node works as a context manager and closes properly."""
    with node() as rb:
        assert rb.isrepl()
        assert rb._runner is not None

    # After exiting context, runner should be None
    assert rb._runner is None


def test_node_context_manager_with_exception():
    """Test that node closes even when exception occurs."""
    rb = None
    try:
        with node() as rb:
            assert rb._runner is not None
            raise ValueError("Test exception")
    except ValueError:
        pass

    # Should still be closed
    assert rb is not None
    assert rb._runner is None


@pytest.mark.parametrize("method_name,args", [
    ("isopen", []),
    ("isrepl", []),
    ("rid", []),
])
def test_node_properties_and_methods(method_name, args):
    """Test various node properties and methods work."""
    rb = node()
    try:
        attr = getattr(rb, method_name)

        if callable(attr):
            result = attr(*args)
        else:
            result = attr

        assert result is not None or result is False
    finally:
        rb.close()


def test_node_basic_operations():
    """Test basic node operations."""
    rb = node()
    try:
        assert rb.rid is not None
        assert rb.isrepl() is True
        assert str(rb) == rb.rid
        assert repr(rb) == rb.rid

        assert rb.router is not None
        assert rb.uid is not None
    finally:
        rb.close()
