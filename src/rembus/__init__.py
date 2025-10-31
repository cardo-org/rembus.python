from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("rembus")
except PackageNotFoundError:
    __version__ = "0.0.0"

# import logging
from .core import component, RbURL  # noqa: F401
from .settings import (  # noqa: F401
    rembus_dir,
    DEFAULT_BROKER,
    TENANTS_FILE
)
from .protocol import (  # noqa: F401
    CBOR,
    JSON,
    QOS0,
    QOS1,
    QOS2,
    SIG_ECDSA,
    SIG_RSA
)
from .sync import node, register

__all__ = [
    'component',
    'node',
    'register',
]
