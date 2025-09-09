"""Synchronous APIs."""
import asyncio
import atexit
import logging
import threading
from types import TracebackType
from typing import Any, Callable, Coroutine, Optional, Type, List
from rembus import (
    component,
    CBOR,
    SIG_RSA,
)

logger = logging.getLogger(__name__)


class AsyncLoopRunner:
    """:meta private:"""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._start_loop, daemon=True)
        self._thread.start()
        atexit.register(self.shutdown)

    def _start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def run(self, coro: Coroutine[Any, Any, Any]) -> Any:
        """Run a coroutine in the event loop and return its result."""
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result()

    def shutdown(self):
        """Shutdown the event loop and wait for the thread to finish."""
        if not self.loop.is_closed():
            try:
                self.loop.call_soon_threadsafe(self.loop.stop)
            except RuntimeError:
                return

            # Wait briefly for the thread to exit
            self._thread.join(timeout=1.0)

            # Clean up the loop
            if not self.loop.is_closed():
                self.loop.run_until_complete(self.loop.shutdown_asyncgens())
                self.loop.close()


class node:  # pylint: disable=invalid-name
    """The synchronous Rembus twin."""

    def __init__(
        self,
        url: str | List[str] | None = None,
        name: str | None = None,
        port: int | None = None,
        secure: bool = False,
        enc: int = CBOR
    ):
        self._runner = AsyncLoopRunner()
        self._rb = self._runner.run(component(url, name, port, secure, enc))

    @property
    def router(self):
        """The router object associated with this node."""
        return self._rb.router

    @property
    def uid(self):
        """The RbUrl object for this node."""
        return self._rb.uid

    @property
    def rid(self):
        """The unique Rembus id for this node."""
        return self._rb.rid

    def isopen(self) -> bool:
        """Check if the connection is open."""
        return self._rb.isopen()

    def isrepl(self) -> bool:
        """Check if the connection is a REPL (Read-Eval-Print Loop)."""
        return self._rb.isrepl()

    def inject(self, ctx: Any):
        """Initialize the context object."""
        return self._rb.inject(ctx)

    def register(self, rid: str, pin: str, scheme: int = SIG_RSA):
        """Register a component with the given rid."""
        return self._runner.run(self._rb.register(rid, pin, scheme))

    def unregister(self):
        """Unregister the component."""
        return self._runner.run(self._rb.unregister())

    def direct(self, target: str, topic: str, *args: Any):
        """Send a direct RPC request to the target component."""
        return self._runner.run(self._rb.direct(target, topic, *args))

    def rpc(self, topic: str, *args: Any):
        """Send a RPC request."""
        return self._runner.run(self._rb.rpc(topic, *args))

    def publish(self, topic: str, *args: Any, **kwargs):
        """Publish a message to the specified topic."""
        return self._runner.run(self._rb.publish(topic, *args, **kwargs))

    def put(self, topic: str, *args: Any, **kwargs):
        """Publish a message to the specified topic."""
        return self._runner.run(
            self._rb.publish(self.rid + '/' + topic, *args, **kwargs)
        )

    def subscribe(self,
                  fn: Callable[..., Any],
                  retroactive: bool = False,
                  topic: Optional[str] = None):
        """
        Subscribe the function to the corresponding topic.
        """
        return self._runner.run(self._rb.subscribe(fn, retroactive, topic))

    def unsubscribe(self, fn: Callable[..., Any]):
        """
        Unsubscribe the function from the corresponding topic.
        """
        return self._runner.run(self._rb.unsubscribe(fn))

    def expose(self, fn: Callable[..., Any]):
        """
        Expose the function as a remote procedure call(RPC) handler.
        """
        return self._runner.run(self._rb.expose(fn))

    def unexpose(self, fn: Callable[..., Any]):
        """
        Unexpose the function as a remote procedure call(RPC) handler.
        """
        return self._runner.run(self._rb.unexpose(fn))

    def reactive(self):
        """
        Set the component to receive published messages on subscribed topics.
        """
        return self._runner.run(self._rb.reactive())

    def unreactive(self):
        """
        Set the component to stop receiving published
        messages on subscribed topics.
        """
        return self._runner.run(self._rb.unreactive())

    def wait(self, timeout: float | None = None):
        """
        Start the twin event loop that wait for rembus messages.
        """
        return self._runner.run(self._rb.wait(timeout))

    def close(self):
        """Close the connection and clean up resources."""
        self._runner.run(self._rb.close())

    def __enter__(self):
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]) -> Optional[bool]:
        self.close()


def register(rid: str, pin: str, scheme: int = SIG_RSA, enc: int = CBOR):
    """Provisions the component with rid identifier."""
    rb = node("ws:", enc=enc)
    try:
        rb.register(rid, pin, scheme)
    except Exception as e:
        logger.error("[%s] register: %s", rid, e)
        raise
    finally:
        rb.close()
