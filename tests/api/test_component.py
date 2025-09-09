"""Test cases for the rembus API methods."""
import asyncio
import os
import time
import logging
import signal
import pytest
import rembus
import rembus.protocol as rp
import rembus.settings


async def shutdown(rb):
    """Shutdown the rembus component `rb` after a short delay.
    This is used to test the wait functionality.
    """
    await asyncio.sleep(0.1)
    await rb.close()


def start_server(port):
    """Start a rembus server on the given port.
    This is a helper function to create a server for testing purposes:
    it initializes a server and waits for it to be ready.
    """
    server = rembus.node(port=port)
    time.sleep(1)
    return server


async def test_wait():
    """Test the wait functionality of the rembus component."""
    server = start_server(port=8001)
    rb = await rembus.component("ws://:8001")
    rid = await rb.rpc("rid")
    assert rid == "broker"
    # schedule the shutdown
    asyncio.create_task(shutdown(rb))
    await rb.wait()
    await rb.close()
    server.close()


async def myservice(x, y):
    """A simple service that adds two numbers."""
    return x+y


async def test_rpc():
    """Test the RPC method of the rembus component."""
    x = 2
    y = 3
    server = start_server(port=8002)
    rb = await rembus.component("ws://:8002")
    server.expose(myservice)
    result = await rb.rpc("myservice", x, y)
    assert result == x+y
    server.unexpose(myservice)
    await rb.close()
    with pytest.raises(rp.RembusConnectionClosed):
        await rb.rpc("myservice", x, y)
    server.close()


async def myservice_ctx(_, __, x, y):
    """
    A service that adds two numbers,
    expecting a context and a rembus handle.
    """
    return x+y


async def test_rpc_ctx():
    """Test the RPC method of the rembus component with an injected context."""
    x = 2
    y = 3
    server = start_server(port=8003)
    ctx = {}
    rb = await rembus.component("ws://:8003")
    server.expose(myservice_ctx)
    server.inject(ctx)
    result = await rb.rpc("myservice_ctx", x, y)
    assert result == x+y
    await rb.close()
    server.close()


async def test_direct():
    """Test the direct method of the rembus component."""
    x = 2
    y = 3
    server = start_server(port=8004)
    rb = await rembus.component("ws://:8004")
    server.expose(myservice)
    result = await rb.direct(rembus.settings.DEFAULT_BROKER, "myservice", x, y)
    assert result == x+y
    server.unexpose(myservice)
    await rb.close()
    server.close()


async def test_unreactive():
    """Test the unreactive method of the rembus component."""
    server = start_server(port=8005)
    rb = await rembus.component("ws://:8005")
    await rb.unreactive()
    await rb.close()
    server.close()


def mytopic():
    """A simple pubsub method that logs a message."""
    logging.info("mytopic called")


def puttopic():
    """A simple pubsub method that logs a message."""
    logging.info("puttopic called")


async def test_publish():
    """Test the publish method of the rembus component."""
    server = start_server(port=8006)
    server.subscribe(puttopic, topic="cmp.net/mytopic")
    server.subscribe(mytopic)

    rb = await rembus.component("ws://:8006/cmp.net")
    assert rb.isrepl() is False
    assert isinstance(rb.router, rembus.core.Router)
    assert repr(
        server.router) == "broker: {'cmp.net@ws://127.0.0.1:8000': cmp.net}"
    assert repr(rb.uid) == "ws://127.0.0.1:8006/cmp.net"
    assert rembus.core.domain(rb.rid) == "net"
    await rb.publish("mytopic")
    await rb.publish("mytopic", "log_warning")

    await rb.put("mytopic")

    await rb.close()
    with pytest.raises(rp.RembusConnectionClosed):
        await rb.publish("mytopic")
    server.close()


async def test_cancel_supervisor_task():
    """Test shutdown in case of task cancellation"""
    server = await rembus.component(port=8000)

    server_wait_task = asyncio.create_task(
        server.wait(), name="server_wait_task")
    await asyncio.sleep(1)
    server_wait_task.cancel()
    await server_wait_task
