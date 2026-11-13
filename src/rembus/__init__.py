import os
from importlib.metadata import version

__version__ = version("rembus")

from .core import RbURL  # noqa: F401
from .component import anonym, component, connect, server
from .settings import (  # noqa: F401
    nowbucket,
    rembus_dir,
    DEFAULT_BROKER,
    TENANTS_FILE,
)
from .protocol import (  # noqa: F401
    RembusError,
    CBOR,
    JSON,
    LastReceived,
    Now,
    QOS0,
    QOS1,
    QOS2,
    SIG_ECDSA,
    SIG_RSA,
)
from .router import add_plugin
from .sync import node, register
from .keyspace import KeySpaceRouter
from .db import dbconnect
from .tenant import (
    add_cid,
    add_cid_opaque,
    add_tenant,
    add_tenant_opaque,
    remove_cid,
    remove_tenant,
    set_tenant_mode,
)

if os.environ.get("REMBUS_KEEP_PROXY", "0") == "0":
    os.environ.pop("http_proxy", None)
    os.environ.pop("HTTP_PROXY", None)
    os.environ.pop("https_proxy", None)
    os.environ.pop("HTTPS_PROXY", None)

__all__ = [
    "add_plugin",
    "add_tenant",
    "add_tenant_opaque",
    "add_cid",
    "add_cid_opaque",
    "remove_cid",
    "remove_tenant",
    "set_tenant_mode",
    "anonym",
    "nowbucket",
    "connect",
    "dbconnect",
    "component",
    "node",
    "register",
    "server",
    "rembus_dir",
    "KeySpaceRouter",
    "RbURL",
]
