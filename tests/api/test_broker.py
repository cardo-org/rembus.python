"""Test the rembus broker feature."""

import logging
import rembus
import rembus.protocol as rp


def myservice(x, y):
    return x + y


def test_broker():
    """Test reconnecting to a rembus broker."""
    x = 1
    y = 2
    server = rembus.node(port=8801)
    srv = rembus.node("ws://:8801/srv")
    srv.expose(myservice)

    cli = rembus.node("ws://:8801/cli")
    z = cli.rpc("myservice", x, y)
    assert z == x + y

    srv.unexpose(myservice)

    # logs the WARNING: admin error: expected cmd property
    srv.exec(
        srv._rb._send_message,
        lambda id: rp.AdminMsg(id=id, topic="topic", data={}),
    )

    srv.close()
    cli.close()
    server.close()


def test_default_broker():
    rb = rembus.node()
    rb.close()
