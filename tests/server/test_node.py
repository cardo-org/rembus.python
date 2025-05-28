import logging
import pytest
import rembus
import rembus.protocol as rp

import rembus.settings


def myservice(x, y):
    return x+y


def test_loop_shutdown():
    asyncloop = rembus.sync.AsyncLoopRunner()
    assert asyncloop.loop.is_running()
    asyncloop.shutdown()


def test_rpc():
    x = 2
    y = 3
    server = rembus.node(port=8002)
    rb = rembus.node("ws://:8002")
    server.expose(myservice)

    result = rb.rpc("myservice", x, y)
    assert result == x+y

    server.unexpose(myservice)

    rb.close()

    with pytest.raises(rp.RembusConnectionClosed):
        rb.rpc("myservice", x, y)

    server.close()


async def test_direct():
    x = 2
    y = 3
    server = rembus.node(port=8004)

    rb = rembus.node("ws://:8004")
    server.expose(myservice)

    result = rb.direct(rembus.settings.DEFAULT_BROKER, "myservice", x, y)
    assert result == x+y

    server.unexpose(myservice)

    rb.close()
    server.close()


def mytopic():
    logging.info("mytopic called")


def test_publish():
    ctx = {}
    server = rembus.node(port=8006)
    server.subscribe(mytopic)
    server.inject(ctx)

    rb = rembus.node("ws://:8006/cmp.net")
    assert rb.isrepl() is False
    assert rb.isopen()

    # implemented but without meaning in this rembus version
    rb.reactive()
    rb.unreactive()

    assert isinstance(rb.router, rembus.core.Router)
    assert rembus.core.domain(rb.rid) == "net"

    rb.publish("mytopic")

    rb.publish("mytopic", "log_warning")

    rb.close()

    with pytest.raises(rp.RembusConnectionClosed):
        rb.publish("mytopic")

    server.unsubscribe(mytopic)

    server.wait(0.1)
    server.close()


def test_runtime_context():
    server = rembus.node(port=8000)
    with rembus.node("mynode") as rb:
        rb.rpc("rid")
    server.close()
