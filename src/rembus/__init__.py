from importlib.metadata import version

__version__ = version("rembus")

# import logging
from .core import add_plugin, RbURL  # noqa: F401
from .component import anonym, component, connect
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

from .sync import node, register
from .keyspace import KeySpaceRouter

__all__ = [
    "add_plugin",
    "anonym",
    "connect",
    "component",
    "node",
    "register",
    "rembus_dir",
    "KeySpaceRouter",
    "RbURL",
]
