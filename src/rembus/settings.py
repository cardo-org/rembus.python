"""Settings for Rembus components."""

import json
import logging
import os
import time
from platformdirs import user_config_dir

logger = logging.getLogger(__name__)

DEFAULT_BROKER = "broker"
DEFAULT_PORT = 8000
TENANTS_FILE = "tenants.json"


def nowbucket(width: int = 900):
    """The start of current bucket expressed in seconds from epoch."""
    return (int(time.time()) // 900) * 900


class Config:
    """Configuration values to modify the behavior of Rembus."""

    def __init__(self, name: str):
        cfg = {}
        try:
            fn = os.path.join(rembus_dir(), name, "settings.json")
            if os.path.isfile(fn):
                with open(fn, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
        except json.decoder.JSONDecodeError as e:
            raise (RuntimeError(f"{fn}: {e}")) from e

        def_timeout = float(os.environ.get("REMBUS_TIMEOUT", 10))
        def_ack_timeout = float(os.environ.get("REMBUS_ACK_TIMEOUT", 2))
        def_ws_ping_interval = float(
            os.environ.get("REMBUS_WS_PING_INTERVAL", 30)
        )
        def_send_retries = int(os.environ.get("REMBUS_SEND_RETRIES", 10))
        self.request_timeout = cfg.get("request_timeout", def_timeout)
        self.ws_ping_interval = cfg.get(
            "ws_ping_interval", def_ws_ping_interval
        )
        self.start_anyway = cfg.get("start_anyway", False)

        # Max numbers of QOS1 and QOS2 Pub/Sub retrasmissions
        self.send_retries = cfg.get("send_retries", def_send_retries)

        # Pub/Sub message timeout
        self.ack_timeout = cfg.get("ack_timeout", def_ack_timeout)

        self.db_attach = db_attach(name)


def db_attach(router_id):
    """Return the DuckDB ATTACH directive."""
    data_dir = os.path.join(rembus_dir(), router_id)
    if "DUCKLAKE_URL" in os.environ:
        db_name = os.environ["DUCKLAKE_URL"]
    else:
        db_name = f"ducklake:{data_dir}.ducklake"

    if "DUCKDB_IGNORE_DATA_PATH" not in os.environ:
        logger.debug("ATTACH '%s' AS rl (DATA_PATH '%s')", db_name, data_dir)
        return f"ATTACH '{db_name}' AS rl (DATA_PATH '{data_dir}')"
    else:
        return f"ATTACH '{db_name}' AS rl"


def rembus_dir():
    """The root directory for all rembus components."""
    rdir = os.getenv("REMBUS_DIR")
    if not rdir:
        rdir = user_config_dir("rembus", "Rembus")

    if not os.path.isdir(rdir):
        os.makedirs(rdir, exist_ok=True)

    return rdir


def broker_dir(router_id):
    """The directory for rembus broker settings and secrets."""
    return os.path.join(rembus_dir(), router_id)


def keys_dir(router_id: str):
    """The directory for rembus public keys."""
    return os.path.join(broker_dir(router_id), "keys")


def keystore_dir():
    """The directory for rembus keystore."""
    return os.environ.get(
        "REMBUS_KEYSTORE", os.path.join(rembus_dir(), "keystore")
    )


def rembus_ca():
    """The CA bundle for rembus located at default location."""
    cadir = os.path.join(rembus_dir(), "ca")
    if os.path.isdir(cadir):
        files = os.listdir(cadir)
        if len(files) == 1:
            return os.path.join(cadir, files[0])

    raise RuntimeError("CA bundle not found")


def key_base(broker_name: str, cid: str) -> str:
    """The directory for the public keys."""
    return os.path.join(rembus_dir(), broker_name, "keys", cid)


def key_file(broker_name: str, cid: str):
    """The public key file for the given component."""
    basename = key_base(broker_name, cid)

    for keyformat in ["pem", "der"]:
        for keytype in ["rsa", "ecdsa"]:
            fn = f"{basename}.{keytype}.{keyformat}"
            logger.debug("looking for %s", fn)
            if os.path.isfile(fn):
                return fn

    raise FileNotFoundError(f"key file not found: {basename}")
