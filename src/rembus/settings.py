"""Settings for Rembus components."""
import json
import logging
import os
from platformdirs import user_config_dir

logger = logging.getLogger(__name__)

DEFAULT_BROKER = "broker"
TENANTS_FILE = "tenants.json"


class Config:
    """Configuration values to modify the behavior of Rembus."""

    def __init__(self, name: str):
        cfg = {}
        try:
            fn = os.path.join(rembus_dir(), name, "settings.json")
            if os.path.isfile(fn):
                with open(fn, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
        except json.decoder.JSONDecodeError as e:
            raise (RuntimeError(f"{fn}: {e}")) from e

        self.request_timeout = cfg.get("request_timeout", 5)
        self.ws_ping_interval = cfg.get("ws_ping_interval", None)
        self.start_anyway = cfg.get("start_anyway", False)
        self.send_retries = cfg.get("send_retries", 3)


def rembus_dir():
    """The root directory for all rembus components."""
    rdir = os.getenv("REMBUS_DIR")
    if not rdir:
        return user_config_dir("rembus", "Rembus")

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


def load_tenants(router):
    """Load the tenants settings."""
    fn = os.path.join(broker_dir(router.id), TENANTS_FILE)
    cfg = {}
    if os.path.isfile(fn):
        with open(fn, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    return cfg
