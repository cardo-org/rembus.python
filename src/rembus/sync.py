import asyncio
import atexit
from types import TracebackType
from typing import Any, Callable, Coroutine, Optional, Type
from rembus import component
import threading

_loop_runner = None

def get_loop_runner():
    global _loop_runner
    if _loop_runner is None:
        _loop_runner = AsyncLoopRunner()
    return _loop_runner

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
    def __init__(self, name:str|None=None):
        self._runner = AsyncLoopRunner()
        self._rb = self._runner.run(component(name))

    def isopen(self) -> bool:
        """Check if the connection is open."""
        return self._rb.isopen()
    
    def register(self, cid:str, pin:str, tenant:str|None=None):
        return self._runner.run(self._rb.register(cid, pin, tenant))

    def unregister(self):
        return self._runner.run(self._rb.unregister())

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
    
    def wait(self):
        return self._runner.run(self._rb.wait())

    def shutdown(self):
        return self._runner.run(self._rb.shutdown())
    
    def close(self):
        try:
            self.shutdown()
        except Exception:
            pass
    
    def __enter__(self):
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]) -> Optional[bool]:
        self.close()

def register(cid:str, pin:str, tenant:str|None=None):
    rb = node()
    rb.register(cid, pin, tenant)
    rb.close()
