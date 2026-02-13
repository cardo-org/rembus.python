"""
The core module of the Rembus library that includes implementations for the
RbURL and Supervised concept.
"""

from __future__ import annotations
import asyncio
import base64
import logging
import os
from typing import Any, Optional
from urllib.parse import urlparse
import uuid
import rembus.protocol as rp

__all__ = [
    "randname",
    "bytes_to_b64",
    "domain",
    "FutureResponse",
    "RbURL",
    "Supervised",
    "response_data",
]

logger = logging.getLogger(__name__)


def randname() -> str:
    """Return a random name for a component."""
    return str(uuid.uuid4())


def bytes_to_b64(val: bytes, enc: int):
    """
    Encode ``val`` as Base64 if ``enc`` equals ``rp.JSON``.

    Returns a UTF-8 string containing the Base64 representation when
    JSON encoding is requested, otherwise returns ``val`` unchanged.
    """
    if enc == rp.JSON:
        return base64.b64encode(val).decode("utf-8")
    return val


def domain(s: str) -> str:
    """
    Return the domain portion of a string.

    If the input string does not contain a domain, the root domain
    "." is returned.
    """
    dot_index = s.find(".")
    if dot_index != -1:
        return s[dot_index + 1 :]

    return "."


class FutureResponse:
    """
    Encapsulate a future response for a Rembus message request.
    """

    def __init__(self, task: asyncio.Task | None, data: Any = None):
        self.future = asyncio.get_running_loop().create_future()
        self.task = task
        self.data = data


class RbURL:
    """
    A class to parse and manage Rembus URLs.
    It supports the `repl` scheme, the standard `ws`/`wss` and
    `mqtt`/`mqtts` schemes.

    The URL `repl` scheme defines a broker.
    """

    def __init__(self, url: str | None = None) -> None:
        default_url = os.getenv("REMBUS_BASE_URL", "ws://127.0.0.1:8000")
        baseurl = urlparse(default_url)
        u = urlparse(url)

        if u.scheme == "repl":
            self.protocol = u.scheme
            self.hostname = ""
            self.port = 0
            self.hasname = False
            self.id = "repl"
        else:
            if isinstance(u.path, str) and u.path and u.path != "__noname__":
                self.hasname = True
                self.id = u.path[1:] if u.path.startswith("/") else u.path
            else:
                self.hasname = False
                self.id = randname()

            if u.scheme:
                self.protocol = u.scheme
            else:
                self.protocol = baseurl.scheme

            if u.hostname:
                self.hostname = u.hostname
            else:
                self.hostname = baseurl.hostname

            if u.port:
                self.port = u.port
            else:
                self.port = baseurl.port

    def __repr__(self):
        return f"{self.protocol}://{self.hostname}:{self.port}/{self.id}"

    def isbroker(self):
        """Check if the url defines a broker."""
        return self.protocol == "repl"

    @property
    def netlink(self):
        """Return the remote connection url endpoint."""
        return f"{self.protocol}://{self.hostname}:{self.port}"

    @property
    def twkey(self):
        """Return a twin unique string identifier."""
        return self.id if self.id == "repl" else f"{self.id}@{self.netlink}"


async def shutdown_message(process: Supervised) -> None:
    """
    Request graceful shutdown of a supervised process by sending
    a "shutdown" message to its inbox.
    """
    logger.debug("[%s] sending shutdown message", process)
    await process.inbox.put("shutdown")


class Supervised:
    """
    A superclass that provides task supervision and auto-restarting for
    a designated task.
    Subclasses must implement the `_task_impl` coroutine.
    """

    downstream: Supervised | None
    """
    The supervised router process next in the chain of Supervised messages
    handlers.

    The downstream direction terminates at the router returned by
    :func:`~rembus.router.top_router`.
    """

    upstream: Supervised | None
    """
    The supervised router process previous in the chain of Supervised messages
    handlers.

    The upstream direction terminates at the router returned by
    :func:`~rembus.router.bottom_router`. It is the router process directly
    bound to the :class:`~rembus.twin.Twin` entity.
    """

    def __init__(self):
        self.upstream = None
        self.downstream = None
        self._task: Optional[asyncio.Task[None]] = None
        self._supervisor_task: Optional[asyncio.Task[None]] = None
        self.inbox: asyncio.Queue[Any] = asyncio.Queue()
        self._should_run = True  # Flag to control supervisor loop

    async def _shutdown(self) -> None:
        """Override in subclasses for custom shutdown logic."""

    async def _task_impl(self) -> None:
        """Override in subclasses for supervised task impl."""

    async def _supervisor(self) -> None:
        """
        Supervises the _task_impl, restarting if it exits
        unexpectedly or due to an exception.
        """
        while self._should_run:
            logger.debug("[%s] starting supervised task", self)
            self._task = asyncio.create_task(self._task_impl())
            try:
                await self._task
            except asyncio.CancelledError:
                logger.debug("[%s] task cancelled, exiting", self)
                self._should_run = False  # Ensure supervisor also stops
                break
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.error("[%s] error: %s (restarting)", self, e)
                logging.exception("traceback for task error:")
                if self._should_run:
                    await asyncio.sleep(0.5)

    async def start(self) -> None:
        """Starts the supervisor task."""
        self._should_run = True
        self._supervisor_task = asyncio.create_task(self._supervisor())

    async def shutdown(self) -> None:
        """Gracefully stops the supervised worker and its supervisor."""
        logger.debug(
            "[%s] shutting down (should_run: %s)", self, self._should_run
        )
        if self._should_run:
            self._should_run = False

            await self._shutdown()

            if self._task and not self._task.done():
                await shutdown_message(self)
                await self._task

            if self._supervisor_task and not self._supervisor_task.done():
                self._supervisor_task.cancel()
                try:
                    await self._supervisor_task
                except asyncio.CancelledError:
                    pass
            logger.debug("[%s] shutdown complete", self)


def response_data(msg: rp.ResMsg):
    """Return the response data or raise an exception on error."""
    sts = msg.status
    if sts == rp.STS_OK:
        return rp.tag2df(msg.data)
    elif sts == rp.STS_CHALLENGE:
        return msg.data
    else:
        raise rp.RembusError(sts, msg.data)
