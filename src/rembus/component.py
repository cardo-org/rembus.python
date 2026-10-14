"""Rembus component entry point."""

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
    schema=None,
    enc: int = CBOR,
    keyspace: bool = True,
    mqtt: str | None = None,
    relay: bool = True,
    authenticated: bool = False,
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
    if (
        authenticated
        and url is not None
        and not isserver
        and not uid.hasname
    ):
        raise ValueError("anonymous components not allowed")

    router = await init_router(
        router_name,
        policy,
        uid,
        port,
        secure,
        isserver,
        schema,
        relay,
        authenticated,
    )
    handle = await init_twin(router, uid, enc, isserver)
    if isinstance(url, list):
        for netlink in url:
            await init_twin(router, RbURL(netlink), enc, isserver)

    if keyspace and isserver:
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
    schema=None,
    enc: int = CBOR,
    keyspace: bool = True,
    mqtt: str | None = None,
    authenticated: bool = False,
) -> Twin:
    """Initialize a rembus component.

    If `authenticated` is true only authenticated components are allowed
    to connect and anonymous components are not allowed.
    """
    handle = await _component(
        url,
        name,
        port,
        secure,
        policy,
        schema,
        enc,
        keyspace,
        mqtt,
        authenticated=authenticated,
    )
    return handle


async def server(
    name: str | None = None,
    port: int | None = None,
    secure: bool = False,
    schema: str | None = None,
    enc: int = CBOR,
    keyspace: bool = True,
    authenticated: bool = False,
) -> Twin:
    """Initialize a rembus server.

    A server accepts connections but, unlike a broker, does not relay RPC
    requests between connected components: a request is resolved only by
    the methods exposed by the server itself.
    """
    return await _component(
        None,
        name,
        port,
        secure,
        "first_up",
        schema,
        enc,
        keyspace,
        relay=False,
        authenticated=authenticated,
    )


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
    authenticated: bool = False,
):
    """Initialize a component context."""
    handle = await _component(
        url,
        name,
        port,
        secure,
        policy,
        schema,
        enc,
        keyspace,
        authenticated=authenticated,
    )
    try:
        yield handle
    finally:
        await handle.close()
