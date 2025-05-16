import asyncio
import atexit
from types import TracebackType
from typing import Any, Callable, Coroutine, Optional, Type
from rembus.twin import component, logger, _components
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
        cmps = list(_components.values())
        for twin in cmps:
            logger.debug(f"runner shutting down {twin.uid.rid()}")
            #self.run(twin.router.shutdown())
            twin.inbox.put_nowait("shutdown")

        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self.loop.stop)
            self._thread.join()


class node:
    ### def __init__(self, name:str="ws:"):
    def __init__(self, url:str|None = None, name:str='broker', port:int|None= None):
        self._runner = AsyncLoopRunner()
        self._rb = self._runner.run(component(url, name, port))

    @property
    def router(self):
        return self._rb.router

    def isopen(self) -> bool:
        """Check if the connection is open."""
        return self._rb.isopen()
    
    def inject(self, ctx:Any):
        """Initialize the context object."""
        return self._rb.inject(ctx)

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

###    def shutdown(self):
###        logger.debug(f"shutting down {self._rb.uid.rid()}")
###        return self._runner.run(self._rb.shutdown())
    
    def close(self):
        #try:
        self._runner.run(self._rb.close())
        #except Exception:
        #    pass
    
    def __enter__(self):
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType]) -> Optional[bool]:
        self.close()

def register(cid:str, pin:str, tenant:str|None=None):
    rb = node("ws:")
    rb.register(cid, pin, tenant)
    rb.close()
