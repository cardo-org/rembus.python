from importlib.metadata import version, PackageNotFoundError

__version__ = version("rembus")

# import logging
from .core import component, add_plugin, RbURL  # noqa: F401
from .settings import (  # noqa: F401
    rembus_dir,
    DEFAULT_BROKER,
    TENANTS_FILE,
)
from .protocol import (  # noqa: F401
    CBOR,
    JSON,
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
    "component",
    "node",
    "register",
    "rembus_dir",
    "KeySpaceRouter",
]
