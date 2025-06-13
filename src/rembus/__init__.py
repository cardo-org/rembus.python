__version__ = "0.1.7"

import logging
from .core import component, RbURL  # noqa: F401
from .settings import (  # noqa: F401
    rembus_dir,
    DEFAULT_BROKER,
    TENANTS_FILE
)
from .protocol import (  # noqa: F401
    QOSLevel,
    CBOR,
    JSON,
    SIG_ECDSA,
    SIG_RSA
)
from .sync import node, register

__all__ = [
    'component',
    'node',
    'register',
]

logging.basicConfig(
    format="[%(levelname)s][%(name)s] %(message)s\r", level=logging.DEBUG)
logger = logging.getLogger("rembus")
