__version__ = "0.1.7"

import logging
from .core import component, RbURL  # noqa: F401
from .settings import (  # noqa: F401
    rembus_dir,
    DEFAULT_BROKER,
    TENANTS_FILE
)
from .protocol import (  # noqa: F401
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


# logging.basicConfig(level=logging.WARNING)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rembus")
