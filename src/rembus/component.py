from typing import List
from contextlib import asynccontextmanager
from rembus.core import Twin, RbURL, init_router, add_plugin
from rembus.settings import DEFAULT_BROKER, DEFAULT_PORT
from rembus.protocol import CBOR
from rembus.keyspace import KeySpaceRouter


async def _component(
    url: str | List[str] | None = None,
    name: str | None = None,
    port: int | None = None,
    secure: bool = False,
    policy: str = "first_up",
    schema: str | None = None,
    enc: int = CBOR,
    keyspace: bool = True,
) -> Twin:
    """Return a Rembus component."""
    isserver = (url is None) or (port is not None)

    if isserver and port is None:
        port = DEFAULT_PORT

    if isinstance(url, str):
        uid = RbURL(url)

    else:
        uid = RbURL("repl://")

    default_name = DEFAULT_BROKER
    if uid.hasname:
        default_name = uid.id

    router_name = name if name else default_name
    router = await init_router(
        router_name, policy, uid, port, secure, isserver, schema
    )
    handle = await router.init_twin(uid, enc, isserver)
    if isinstance(url, list):
        for netlink in url:
            await router.init_twin(RbURL(netlink), enc, isserver)

    if keyspace:
        kspace = KeySpaceRouter()
        await add_plugin(handle, kspace)

    return handle


async def component(
    url: str | List[str] | None = None,
    name: str | None = None,
    port: int | None = None,
    secure: bool = False,
    policy: str = "first_up",
    schema: str | None = None,
    enc: int = CBOR,
    keyspace: bool = True,
) -> Twin:
    handle = await _component(
        url, name, port, secure, policy, schema, enc, keyspace
    )
    return handle


@asynccontextmanager
async def connect(
    url: str | List[str] | None = None,
    name: str | None = None,
    port: int | None = None,
    secure: bool = False,
    policy: str = "first_up",
    schema: str | None = None,
    enc: int = CBOR,
    keyspace: bool = True,
):
    """Initialize a component context."""
    handle = await _component(
        url, name, port, secure, policy, schema, enc, keyspace
    )
    try:
        yield handle
    finally:
        await handle.close()
