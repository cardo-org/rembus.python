"""Test reconnecting to a rembus broker and builtins methods."""
import time
import rembus

import rembus.settings


def test_reconnect():
    """Test reconnecting to a rembus broker."""
    server = rembus.node(port=8000)
    rb = rembus.node("ws:")
    rid = rb.rpc("rid")
    assert rid == "broker"
    server.close()
    time.sleep(1)
    server = rembus.node(port=8000)
    while True:
        time.sleep(1)
        if rb.isopen():
            break

    rid = rb.rpc("rid")
    assert rid == "broker"
    rb.close()
    server.close()


def test_builtins():
    """Test the built-in methods of a rembus node."""
    server = rembus.node(port=8010)
    rb1 = rembus.node("ws://:8010/test_mycomponent")
    rb2 = rembus.node("ws://:8010/test_another")
    rid = rb1.rpc("rid")
    assert rid == rembus.settings.DEFAULT_BROKER
    version = rb1.rpc("version")
    assert version == rembus.__version__
    uptime = rb1.rpc("uptime")
    assert isinstance(uptime, str)
    server.close()
    rb1.close()
    rb2.close()
