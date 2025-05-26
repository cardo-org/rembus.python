import asyncio
import json
import logging
import os
import pytest
import rembus
import rembus.protocol as rp
import rembus.twin as core
import rembus.settings as rs
import stat
import time

def start_server(port):
    server = rembus.node(port=port)
    time.sleep(1)
    return server

async def test_register_error():
    cid = "myname"
    logging.info(f"REMBUS_DIR: {rembus.rembus_dir()}")
    server = start_server(port=8000)

    rembus.register(cid, "11223344")

    key_dir = core.keys_dir(server.router.id)
    curr_mode = os.stat(key_dir).st_mode
    os.chmod(key_dir, curr_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))

    myname = rembus.node(cid)
    try:
        myname.unregister()
        assert False
    except rp.RembusError as e:
        assert True
    finally:
        # restore permissions
        os.chmod(key_dir, curr_mode)

    myname.unregister()
    myname.close()
    server.close()

def test_settings_error():
    fn = os.path.join(rs.rembus_dir(), rs.DEFAULT_BROKER, "settings.json")

    with open(fn, "w") as f:
        f.write('this is not a valid json')

    with pytest.raises(RuntimeError):
        rs.Config(rs.DEFAULT_BROKER)

    os.remove(fn)