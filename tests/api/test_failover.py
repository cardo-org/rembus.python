"""Test cases for the node API given a list of URLs (ordered failover)."""

import logging
import time
import pytest
import rembus
import rembus.protocol as rp


def myservice(x, y):
    """A simple service that adds two numbers."""
    return x + y


def slowservice():
    """Trigger a timeout"""
    time.sleep(0.6)
    return "ok"


def test_rpc(server):
    """Test the RPC related methods."""
    x = 2
    y = 3
    rb = rembus.node(["client1", "client2"])

    server.router.shared = None
    server.expose(myservice)
    server.expose(slowservice)

    result = rb.rpc("myservice", x, y)
    assert result == x + y

    rb._rb.router.config.request_timeout = 0.5
    with pytest.raises(rp.RembusTimeout):
        rb.rpc("slowservice")

    with pytest.raises(rp.RembusError):
        rb.rpc("myservice", x)

    server.unexpose(myservice)
    rb.close()

    with pytest.raises(rp.RembusConnectionClosed):
        rb.rpc("myservice", x, y)


def test_direct(server):
    """Test the direct method."""
    x = 2
    y = 3
    rb = rembus.node(["client1", "client2"])
    server.expose(myservice)

    result = rb.direct(rembus.settings.DEFAULT_BROKER, "myservice", x, y)
    assert result == x + y

    server.unexpose(myservice)
    rb.close()


def mytopic():
    """A simple pubsub handler that logs a message."""
    logging.info("mytopic called")


def puttopic():
    """A simple pubsub method that logs a message."""
    logging.info("puttopic called")


def test_publish(server):
    """Test the publish method."""
    ctx = {}

    server.subscribe(mytopic)
    server.subscribe(puttopic, topic="cmp.net/mytopic")
    server.inject(ctx)

    rb = rembus.node(["pub1", "pub2"])
    assert rb.uid.id == "pub1"
    assert rb.isbroker() is False
    assert rb.isopen()
    assert isinstance(rb.router, rembus.router.Router)

    rb.publish("mytopic")
    rb.publish("mytopic", "log_warning")
    rb.put("mytopic")

    rb.close()
    with pytest.raises(rp.RembusConnectionClosed):
        rb.publish("mytopic")
    server.unsubscribe(mytopic)
    server.wait(0.1)


@pytest.mark.asyncio
async def test_ordered_failover():
    """A list of URLs is an ordered failover with one active connection."""
    import asyncio  # pylint: disable=import-outside-toplevel

    async def who():
        return "second"

    srv = await rembus.component(port=8107)
    await srv.expose(who, "who")
    # The primary endpoint is down: the second URL is used.
    cli = await rembus.component(
        ["ws://127.0.0.1:8108/cli.net", "ws://127.0.0.1:8107/cli.net"]
    )
    assert cli.uid.id == "cli.net"
    assert await cli.rpc("who") == "second"
    assert len(srv.router.id_twin) == 2  # repl + the single client twin
    await asyncio.sleep(0.1)
    await cli.close()
    await srv.close()
