from typing import List
from contextlib import asynccontextmanager
from rembus.core import RbURL
from rembus.twin import Twin, init_twin
from rembus.router import init_router, add_plugin
from rembus.settings import DEFAULT_BROKER, DEFAULT_PORT
from rembus.protocol import CBOR
from rembus.keyspace import KeySpaceRouter


def anonym(host=None, port=None):
    """Return an url representing an anonymous component."""
    url = RbURL()
    if host:
        url.hostname = host
    if port:
        url.port = port

    return url


async def _component(
    url: RbURL | str | List[str] | None = None,
    name: str | None = None,
    port: int | None = None,
    secure: bool = False,
    policy: str = "first_up",
    schema: str | None = None,
    enc: int = CBOR,
    keyspace: bool = True,
    mqtt: str | None = None,
) -> Twin:
    """Return a Rembus component."""
    isserver = (url is None) or (port is not None)

    if isserver and port is None:
        port = DEFAULT_PORT

    if isinstance(url, str):
        uid = RbURL(url)
    elif isinstance(url, RbURL):
        uid = url
    else:
        uid = RbURL("repl://")

    default_name = DEFAULT_BROKER
    if uid.hasname:
        default_name = uid.id

    router_name = name if name else default_name
    router = await init_router(
        router_name, policy, uid, port, secure, isserver, schema
    )
    handle = await init_twin(router, uid, enc, isserver)
    if isinstance(url, list):
        for netlink in url:
            await init_twin(router, RbURL(netlink), enc, isserver)

    if keyspace:
        kspace = KeySpaceRouter()
        await add_plugin(handle, kspace)

    if mqtt:
        mqtt_twin = await init_twin(router, RbURL(mqtt), enc, False)
        await kspace.subscribe_handler(mqtt_twin, "**")

    return handle


async def component(
    url: RbURL | str | List[str] | None = None,
    name: str | None = None,
    port: int | None = None,
    secure: bool = False,
    policy: str = "first_up",
    schema: str | None = None,
    enc: int = CBOR,
    keyspace: bool = True,
    mqtt: str | None = None,
) -> Twin:
    """Initialize a rembus component."""
    handle = await _component(
        url, name, port, secure, policy, schema, enc, keyspace, mqtt
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
