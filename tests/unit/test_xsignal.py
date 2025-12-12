import asyncio
from unittest.mock import Mock, patch, AsyncMock
import pytest
from rembus.component import receive_signal


class TestReceiveSignal:
    """Tests for the receive_signal function."""

    @pytest.mark.asyncio
    async def test_receive_signal_calls_shutdown(self):
        """Test that receive_signal schedules the shutdown coroutine."""
        mock_handle = Mock()
        mock_handle.shutdown = AsyncMock()

        loop = asyncio.get_running_loop()

        receive_signal(mock_handle, loop)

        # Give the event loop a moment to schedule the coroutine
        await asyncio.sleep(0.01)

        mock_handle.shutdown.assert_called_once()

    def test_receive_signal_with_mock_loop(self):
        """Test receive_signal with a mocked loop."""
        mock_handle = Mock()
        mock_handle.shutdown = AsyncMock()
        mock_loop = Mock()

        # Patch asyncio.run_coroutine_threadsafe to close the coroutine
        def mock_run_coroutine_threadsafe(coro, loop):
            # Close the coroutine to avoid warning
            coro.close()
            return Mock()

        with patch(
            "asyncio.run_coroutine_threadsafe",
            side_effect=mock_run_coroutine_threadsafe,
        ) as mock_run:
            receive_signal(mock_handle, mock_loop)

            mock_run.assert_called_once()

            args, kwargs = mock_run.call_args
            assert args[1] == mock_loop

    @pytest.mark.asyncio
    async def test_receive_signal_threadsafe_execution(self):
        """
        Test that receive_signal properly schedules coroutine in thread-safe
        manner.
        """
        mock_handle = Mock()
        mock_handle.shutdown = AsyncMock(return_value=None)

        loop = asyncio.get_running_loop()

        with patch("asyncio.run_coroutine_threadsafe") as mock_run:
            mock_future = Mock()
            mock_run.return_value = mock_future

            receive_signal(mock_handle, loop)

            mock_run.assert_called_once()
            args, kwargs = mock_run.call_args
            assert args[1] == loop

    @pytest.mark.asyncio
    async def test_receive_signal_shutdown_exception(self):
        """
        Test that receive_signal handles exceptions in shutdown gracefully.
        """
        from types import SimpleNamespace

        exception_raised = asyncio.Event()

        # Create an async function that raises an exception
        async def failing_shutdown():
            exception_raised.set()
            raise Exception("Shutdown error")

        # Use SimpleNamespace to avoid any Mock interference
        mock_handle = SimpleNamespace(shutdown=failing_shutdown)

        loop = asyncio.get_running_loop()

        # Call receive_signal - exception should be contained
        receive_signal(mock_handle, loop)

        # Wait for the exception to be raised
        await asyncio.wait_for(exception_raised.wait(), timeout=0.1)
