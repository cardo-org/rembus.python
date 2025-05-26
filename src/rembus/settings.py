import json
import logging
import os
from platformdirs import user_config_dir
from .protocol import SIG_RSA, SIG_ECDSA

logger = logging.getLogger(__name__)

DEFAULT_BROKER = "broker"
TENANTS_FILE = "tenants.json"

class Config:
    def __init__(self, name: str):
        cfg = {}
        try:
            fn = os.path.join(rembus_dir(), name, "settings.json")
            with open(fn, "r") as f:
                cfg = json.loads(fn)
        except FileNotFoundError:
            pass
        except json.decoder.JSONDecodeError as e:
            raise(RuntimeError(f"{fn}: {e}"))

        self.request_timeout = cfg.get("request_timeout", 1)
        self.ws_ping_interval = cfg.get("ws_ping_interval", None)

def rembus_dir():
    """The root directory for all rembus components."""
    rdir = os.getenv("REMBUS_DIR")
    if not rdir:
        return user_config_dir("rembus", "Rembus")
    else:
        return rdir

def broker_dir(router_id):
    """The directory for rembus broker settings and secrets."""
    return os.path.join(rembus_dir(), router_id)

def keys_dir(router_id:str):
    """The directory for rembus public keys."""
    return os.path.join(broker_dir(router_id), "keys")

def keystore_dir():
    return os.environ.get("REMBUS_KEYSTORE", os.path.join(rembus_dir(), "keystore"))

def rembus_ca():
    dir = os.path.join(rembus_dir(), "ca")
    if os.path.isdir(dir):
        files = os.listdir(dir)
        if len(files) == 1:
            return os.path.join(dir, files[0])

    raise Exception("CA bundle not found")

def key_base(broker_name:str, cid:str) -> str:
    return os.path.join(rembus_dir(), broker_name, "keys", cid)

def key_file(broker_name:str, cid:str):
    basename = key_base(broker_name, cid)
    
    for format in ["pem", "der"]:
        for type in ["rsa", "ecdsa"]:
            fn = f"{basename}.{type}.{format}"
            logger.debug(f"looking for %s", fn)
            if os.path.isfile(fn):
                return fn
    
    raise FileNotFoundError(f"key file not found: {basename}")

def isregistered(router_id, cid:str):
    try:
        key_file(router_id, cid)
        return True
    except FileNotFoundError:
        return False

def save_pubkey(router_id:str, cid:str, pubkey:bytes, type:int):
    name = key_base(router_id, cid)
    format = "der"
    # check if pubkey start with -----BEGIN chars
    if pubkey[0:10] == bytes([0x2d, 0x2d, 0x2d, 0x2d, 0x2d, 0x42, 0x45, 0x47, 0x49, 0x4e]):
        format = "pem"

    if type == SIG_RSA:
        typestr = "rsa"
    else:
        typestr = "ecdsa"
    
    fn = f"{name}.{typestr}.{format}"
    with open(fn, "wb") as f:
        f.write(pubkey)

def remove_pubkey(router, cid:str):
    fn = key_file(router.id, cid)
    os.remove(fn)

def load_tenants(router):
    """Load the tenants settings."""
    fn = os.path.join(broker_dir(router.id), TENANTS_FILE)
    cfg = {}
    if os.path.isfile(fn):
        with open(fn, "r") as f:
            cfg = json.load(f)
    return cfg
