import json
import os
import time
from pathlib import Path
import rembus


def test_connect_anyway():
    name = "test_node"
    cfg = {
        "start_anyway": True
    }
    cmp_dir = os.path.join(rembus.rembus_dir(), name)
    Path(cmp_dir).mkdir(parents=True, exist_ok=True)
    fn = os.path.join(cmp_dir, "settings.json")

    with open(fn, "w", encoding="utf-8") as f:
        f.write(json.dumps(cfg))

    rb = rembus.node(f"ws://127.0.0.1:8999/{name}")

    time.sleep(1)
    rb.close()
