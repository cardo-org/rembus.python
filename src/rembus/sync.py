import asyncio
import atexit
import logging
from types import TracebackType
from typing import Any, Callable, Coroutine, Optional, Type
from rembus import (
    component,
    SIG_ECDSA,
    SIG_RSA
)
import threading

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
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result()
    
    def shutdown(self):
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            self._thread.join()


class node:
    def __init__(
            self,
            url:str|None=None,
            name:str|None=None,
            port:int|None=None,
            secure:bool=False
        ):
        self._runner = AsyncLoopRunner()
        self._rb = self._runner.run(component(url, name, port, secure))

    @property
    def router(self):
        return self._rb.router

    @property
    def uid(self):
        return self._rb.uid

    @property
    def rid(self):
        return self._rb.rid

    def isopen(self) -> bool:
        """Check if the connection is open."""
        return self._rb.isopen()
    
    def isrepl(self) -> bool:
        """Check if the connection is a REPL (Read-Eval-Print Loop)."""
        return self._rb.isrepl()
    
    def inject(self, ctx:Any):
        """Initialize the context object."""
        return self._rb.inject(ctx)

    def register(self, cid:str, pin:str, scheme:int=SIG_RSA):
        return self._runner.run(self._rb.register(cid, pin, scheme))

    def unregister(self):
        return self._runner.run(self._rb.unregister())

    def direct(self, target:str, topic:str, *args:tuple[Any]):
        return self._runner.run(self._rb.direct(target, topic, *args))

    def rpc(self, topic:str, *args:tuple[Any]):
        return self._runner.run(self._rb.rpc(topic, *args))

    def publish(self, topic:str, *args:tuple[Any], **kwargs):
        return self._runner.run(self._rb.publish(topic, *args, **kwargs))

    def subscribe(self, fn:Callable[..., Any], retroactive:bool=False):
        return self._runner.run(self._rb.subscribe(fn, retroactive))

    def unsubscribe(self, fn:Callable[..., Any]):
        return self._runner.run(self._rb.unsubscribe(fn))
    
    def expose(self, fn:Callable[..., Any]):
        return self._runner.run(self._rb.expose(fn))

    def unexpose(self, fn:Callable[..., Any]):
        return self._runner.run(self._rb.unexpose(fn))
    
    def reactive(self):
        return self._runner.run(self._rb.reactive())
        
    def unreactive(self):
        return self._runner.run(self._rb.unreactive())

    def wait(self, timeout:float|None=None):
        return self._runner.run(self._rb.wait(timeout))

    def close(self):
        self._runner.run(self._rb.close())
    
    def __enter__(self):
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]) -> Optional[bool]:
        self.close()

def register(cid:str, pin:str, scheme:int=SIG_RSA):
    rb = node("ws:")
    try:
        rb.register(cid, pin, scheme)
    except Exception as e:
        logger.error(f"[{cid}] register: {e}")
        raise
    finally:    
        rb.close()
