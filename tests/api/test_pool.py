"""Test cases for the node synchronous API."""
import logging
import time
import pytest
import rembus
import rembus.protocol as rp


def myservice(x, y):
    """A simple service that adds two numbers."""
    return x+y


def slowservice():
    """Trigger a timeout"""
    time.sleep(0.6)
    return "ok"


def test_rpc(server):
    """Test the RPC related methods."""
    x = 2
    y = 3
    rb = rembus.node(["client1", "client2"])
    server.expose(myservice)
    server.expose(slowservice)

    result = rb.rpc("myservice", x, y)
    assert result == x+y

    rb._rb.router.config.request_timeout = 0.5
    with pytest.raises(rp.RembusTimeout):
        rb.rpc("slowservice")

    with pytest.raises(rp.RembusError):
        rb.rpc("myservice", x)

    server.unexpose(myservice)
    rb.close()

    with pytest.raises(rp.RembusConnectionClosed):
        rb.rpc("myservice", x, y)


async def test_direct(server):
    """Test the direct method."""
    x = 2
    y = 3
    rb = rembus.node(["client1", "client2"])
    server.expose(myservice)

    result = rb.direct(rembus.settings.DEFAULT_BROKER, "myservice", x, y)
    assert result == x+y

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
    assert repr(rb.uid) == "repl://:0/repl"
    assert rb.isrepl() is True
    assert rb.isopen()

    # implemented but without meaning in this rembus version
    rb.reactive()
    rb.unreactive()
    assert isinstance(rb.router, rembus.core.Router)
    assert rembus.core.domain(rb.rid) == "."

    rb.publish("mytopic")
    rb.publish("mytopic", "log_warning")
    rb.put("mytopic")

    rb.close()
    with pytest.raises(rp.RembusConnectionClosed):
        rb.publish("mytopic")
    server.unsubscribe(mytopic)
    server.wait(0.1)
