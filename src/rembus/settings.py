import json
import os
import tempfile

class Config:
    def __init__(self, name: str):
        cfg = {}
        try:
            homedir = os.environ.get("HOME", tempfile.gettempdir())
            rembus_dir = os.environ.get(
                "REMBUS_DIR", 
                os.path.join(homedir, ".config", "rembus", name)
            )
            fn = os.path.join(rembus_dir, "settings.json")
            with open(fn, "r") as f:
                cfg = json.loads(fn)
        except FileNotFoundError:
            pass
        except json.decoder.JSONDecodeError as e:
            print(f"error decoding config file: {e}")

        self.request_timeout = cfg.get("request_timeout", 1)
        self.ws_ping_interval = cfg.get("ws_ping_interval", 30)
