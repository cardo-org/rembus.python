"""Test the rembus broker feature."""

import logging
import os
import rembus
import rembus.protocol as rp


def myservice(x, y):
    return x + y


def broker_service(x, y):
    return x * y


def test_broker():
    """Test reconnecting to a rembus broker."""
    x = 1
    y = 2

    server = rembus.node(port=8801)
    server.expose(broker_service)

    srv_name = "srv"
    srv = rembus.node(f"ws://:8801/{srv_name}")
    srv.expose(myservice)

    router = server.router
    twin = router.get_twin(f"ws://:8801/{srv_name}")
    assert twin.uid.id == srv_name

    cli = rembus.node("ws://:8801/cli")
    z = cli.rpc("myservice", x, y)
    assert z == x + y

    z = cli.rpc("broker_service", x, y)
    assert z == x * y

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
