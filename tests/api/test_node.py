"""Test cases for the node synchronous API."""
import logging
import pytest
import rembus
import rembus.protocol as rp
import rembus.settings


def myservice(x, y):
    """A simple service that adds two numbers."""
    return x+y


def test_rpc(server):
    """Test the RPC related methods."""
    x = 2
    y = 3
    rb = rembus.node("client")
    server.expose(myservice)

    result = rb.rpc("myservice", x, y)
    assert result == x+y

    server.unexpose(myservice)

    rb.close()

    with pytest.raises(rp.RembusConnectionClosed):
        rb.rpc("myservice", x, y)


def test_direct(server):
    """Test the direct method."""
    x = 2
    y = 3
    rb = rembus.node("client")
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


def test_runtime_context():
    """Test the node with context."""
    with rembus.node("mynode") as rb:
        rb.rpc("rid")


def test_publish(server):
    """Test the publish method."""
    ctx = {}
    server.subscribe(mytopic)
    server.subscribe(puttopic, topic="cmp.net/mytopic")

    server.inject(ctx)

    rb = rembus.node("cmp.net")
    assert repr(rb.uid) == "ws://127.0.0.1:8000/cmp.net"
    assert rb.isrepl() is False
    assert rb.isopen()

    # implemented but without meaning in this rembus version
    rb.reactive()
    rb.unreactive()

    assert isinstance(rb.router, rembus.core.Router)
    assert rembus.core.domain(rb.rid) == "net"

    rb.publish("mytopic")
    rb.publish("mytopic", "log_warning")
    rb.put("mytopic")
    rb.close()

    with pytest.raises(rp.RembusConnectionClosed):
        rb.publish("mytopic")

    server.unsubscribe(mytopic)
    # server.wait(0.1)
