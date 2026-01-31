"""Test cases for the rembus API methods."""

import asyncio
import logging
import pytest
import rembus
import rembus.protocol as rp
import rembus.settings


async def shutdown(cli):
    """Shutdown the rembus component `cli` after a short delay.
    This is used to test the wait functionality.
    """
    await asyncio.sleep(0.1)
    await cli.close()


async def start_server(port):
    """Start a rembus server on the given port.
    This is a helper function to create a server for testing purposes:
    it initializes a server and waits for it to be ready.
    """
    server = await rembus.component(port=port)
    await asyncio.sleep(1)
    return server


@pytest.mark.asyncio
async def test_wait():
    """Test the wait functionality of the rembus component."""
    server = await start_server(port=8001)
    server.register_shutdown()

    async with rembus.connect("ws://:8001") as cli:
        assert cli.broker_dir == rembus.settings.broker_dir(
            rembus.DEFAULT_BROKER
        )

        rid = await cli.rpc("rid")
        assert rid == "broker"
        # schedule the shutdown
        asyncio.create_task(shutdown(cli))
        await cli.wait()

    await server.close()


@pytest.mark.asyncio
async def test_builtin_with_ctx():
    """Test the rid builtin with a ctx."""
    server = await start_server(port=8001)
    server.register_shutdown()
    ctx = {}
    server.inject(ctx)

    async with rembus.connect("ws://:8001") as cli:
        rid = await cli.rpc("rid")
        assert rid == "broker"
        # schedule the shutdown
        asyncio.create_task(shutdown(cli))
        await cli.wait()

    await server.close()


async def myservice(x, y):
    """A simple service that adds two numbers."""
    return x + y


@pytest.mark.asyncio
async def test_rpc():
    """Test the RPC method of the rembus component."""
    x = 2
    y = 3
    server = await start_server(port=8002)
    cli = await rembus.component("ws://:8002")
    await server.expose(myservice)
    result = await cli.rpc("myservice", x, y)
    assert result == x + y
    await server.unexpose(myservice)
    await cli.close()
    with pytest.raises(rp.RembusConnectionClosed):
        await cli.rpc("myservice", x, y)
    await server.close()


async def myservice_ctx(x, y, ctx, node):
    """
    A service that adds two numbers,
    expecting a context and a rembus handle.
    """
    return x + y


@pytest.mark.asyncio
async def test_rpc_ctx():
    """Test the RPC method of the rembus component with an injected context."""
    x = 2
    y = 3
    server = await start_server(port=8003)
    ctx = {}
    cli = await rembus.component("ws://:8003")
    await server.expose(myservice_ctx)
    server.inject(ctx)
    result = await cli.rpc("myservice_ctx", x, y)
    assert result == x + y
    await cli.close()
    await server.close()


@pytest.mark.asyncio
async def test_direct():
    """Test the direct method of the rembus component."""
    x = 2
    y = 3
    server = await start_server(port=8004)
    cli = await rembus.component("ws://:8004")
    await server.expose(myservice)
    result = await cli.direct(rembus.settings.DEFAULT_BROKER, "myservice", x, y)
    assert result == x + y
    await server.unexpose(myservice)
    await cli.close()
    await server.close()


@pytest.mark.asyncio
async def test_unreactive():
    """Test the unreactive method of the rembus component."""
    server = await start_server(port=8006)
    cli = await rembus.component("ws://:8006")
    await cli.unreactive()
    await cli.close()
    await server.close()


def mytopic():
    """A simple pubsub method that logs a message."""
    logging.info("mytopic called")


def never_called(ctx, cli):
    logging.error("never_called: unexpected invocation")
    ctx[cli.rid] = True


def puttopic():
    """A simple pubsub method that logs a message."""
    logging.info("puttopic called")


@pytest.mark.asyncio
async def test_publish():
    """Test the publish method of the rembus component."""
    server = await start_server(port=8006)
    await server.subscribe(puttopic, topic="cmp.net/mytopic")
    await server.subscribe(mytopic)

    sub = await rembus.component("ws://:8006/sub.net")
    await sub.subscribe(mytopic)
    await sub.reactive()

    ctx = {}
    cli = await rembus.component("ws://:8006/cmp.net")
    cli.inject(ctx)

    assert cli.isrepl() is False
    assert isinstance(cli.router, rembus.router.Router)
    assert "cmp.net@ws://127.0.0.1:8000" in repr(server.router)
    assert repr(cli.uid) == "ws://127.0.0.1:8006/cmp.net"
    assert rembus.core.domain(cli.rid) == "net"

    # subscription to itself
    await cli.subscribe(never_called, topic="mytopic")

    await cli.publish("mytopic")
    await cli.publish("mytopic", "log_warning")

    await cli.put("mytopic")

    await cli.close()
    await sub.close()

    with pytest.raises(rp.RembusConnectionClosed):
        await cli.publish("mytopic")
    await server.close()
    assert len(ctx) == 0


@pytest.mark.asyncio
async def test_publish_slot():
    """Test the slot option of the publish api."""
    server = await start_server(port=8007)
    await server.subscribe(puttopic, topic="cmp.net/mytopic")
    await server.subscribe(mytopic)

    cli = await rembus.component("ws://:8007/cmp.net")
    assert cli.isrepl() is False
    assert isinstance(cli.router, rembus.router.Router)
    assert repr(server.router) == "broker: {'cmp.net@ws://127.0.0.1:8000'}"
    assert repr(cli.uid) == "ws://127.0.0.1:8007/cmp.net"
    assert rembus.core.domain(cli.rid) == "net"
    await cli.publish("mytopic", slot=rembus.nowbucket())
    await cli.publish("mytopic", slot=1234, qos=rp.QOS2)
    await cli.publish("mytopic", "log_warning", slot=1234, qos=rp.QOS1)

    await cli.close()
    await server.close()


@pytest.mark.asyncio
async def test_cancel_server_task():
    """Test shutdown in case of task cancellation"""
    server = await rembus.component(port=8000)
    await asyncio.sleep(0)

    # yield point
    server._task.cancel()

    # close the db
    server.db.close()
