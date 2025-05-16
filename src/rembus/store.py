import json
import os
import pandas as pd
from platformdirs import user_config_dir
from .protocol import SIG_RSA, SIG_ECDSA

TENANTS_FILE = "tenants.json"
TENANT_COMPONENT = "tenant_component.json"

def rembus_dir():
    """The directory for rembus settings and secrets."""
    rdir = os.getenv("REMBUS_DIR")
    if not rdir:
        # appears in path only for Windows machine
        app_author = "Rembus"
        return user_config_dir("rembus", app_author)
    else:
        return rdir

def broker_dir(router_id):
    """The directory for rembus broker settings and secrets."""
    return os.path.join(rembus_dir(), router_id)

def keys_dir(router_id:str):
    """The directory for rembus public keys."""
    return os.path.join(broker_dir(router_id), "keys")

def fullname(basename:str) -> str|None:
    """The component secret file full path or None if not found."""
    for format in ["pem", "der"]:
        for type in ["rsa", "ecdsa"]:
            fn = "$basename.$type.$format"
            if os.path.isfile(fn):
                return fn
    
    return basename if os.path.isfile(basename) else None

def key_base(broker_name:str, cid:str) -> str:
    return os.path.join(rembus_dir(), broker_name, "keys", cid)

def key_file(broker_name:str, cid:str):
    basename = key_base(broker_name, cid)
    return fullname(basename)
def isregistered(router_id, cid:str):
    return key_file(router_id, cid) != None

def save_pubkey(router_id:str, cid:str, pubkey:bytes, type:int):
    name = key_base(router_id, cid)
    format = "der"
    # check if pubkey start with -----BEGIN chars
    if pubkey[1:10] == [0x2d, 0x2d, 0x2d, 0x2d, 0x2d, 0x42, 0x45, 0x47, 0x49, 0x4e]:
        format = "pem"

    if type == SIG_RSA:
        typestr = "rsa"
    else:
        typestr = "ecdsa"
    
    fn = f"{name}.{typestr}.{format}"
    with open(fn, "wb") as f:
        f.write(pubkey)

def load_tenants(router):
    """Load the tenants settings."""
    fn = os.path.join(broker_dir(router.id), TENANTS_FILE)
    if os.path.isfile(fn): 
        df =  pd.read_json(fn)
        df['pin'] = df['pin'].astype(str)
        if 'tenant' not in df.columns:
            df['tenant'] = router.id
    else:
        df = pd.DataFrame(columns=["pin", "tenant"])

    return df

def load_tenant_component(router):
    """Return the dataframe that maps tenants with components."""
    fn = os.path.join(broker_dir(router.id), TENANT_COMPONENT)
    df = pd.DataFrame(columns=['tenant', 'component'])
    if os.path.isfile(fn):
        d = pd.read_json(fn)
        if not d.empty and 'tenant' in d.columns and 'component' in d.columns:
            df = d

    return df

def save_tenant_component(router_id, df):
    fn = os.path.join(broker_dir(router_id), TENANT_COMPONENT)
    with open(fn, "w") as f:
        f.write(df.to_json(orient="records"))
