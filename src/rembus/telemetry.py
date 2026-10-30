"""Opt-in telemetry of pub/sub traffic, enabled by ``REMBUS_TELEMETRY``.

The flag is evaluated once at import time. Call sites guard every
telemetry call with ``if telemetry.ENABLED:`` so that, when disabled,
the cost is a single module attribute lookup and no argument is built.
"""

import logging
import os

logger = logging.getLogger("rembus.telemetry")

ENABLED: bool = os.environ.get("REMBUS_TELEMETRY", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

if ENABLED and logger.level == logging.NOTSET:
    logger.setLevel(logging.INFO)
    if not logger.handlers and not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)


def _status(twin) -> str:
    try:
        return "connected" if twin.isopen() else "disconnected"
    except Exception:  # pylint: disable=broad-exception-caught
        return "unknown"


def published(router, msg, subscribers) -> None:
    """Log a published message and the subscribed twins with their status."""
    subs = ", ".join(
        f"{t}[{_status(t)}{'' if t.isreactive else ',not-reactive'}]"
        for t in subscribers
    )
    logger.info(
        "[%s] publish topic=%s id=%s from=%s subscribers=[%s]",
        router,
        msg.topic,
        getattr(msg, "id", None),
        msg.twin,
        subs,
    )


def sent(router, msg, twin, delivered: bool, reason: str = "") -> None:
    """Log the outcome of the delivery of a message to a subscriber."""
    logger.info(
        "[%s] %s topic=%s to=%s%s",
        router,
        "send" if delivered else "skip",
        msg.topic,
        twin,
        f" ({reason})" if reason else "",
    )


def received(twin, msg) -> None:
    """Log a pub/sub message received by a twin."""
    logger.info(
        "[%s] recv topic=%s id=%s status=%s",
        twin,
        msg.topic,
        getattr(msg, "id", None),
        _status(twin),
    )
