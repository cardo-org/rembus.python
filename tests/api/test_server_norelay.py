"""Test that a server does not relay RPC requests between components."""

import asyncio

import pytest
import rembus
import rembus.protocol as rp


async def _run(srv, port):
    exposer = caller = None
    try:
        await srv.expose(lambda: "from_server", "local")
        exposer = await rembus.component(f"ws://:{port}/exposer")
        await exposer.expose(lambda: "from_exposer", "remote")
        caller = await rembus.component(f"ws://:{port}/caller")
        assert await caller.rpc("local") == "from_server"
        return caller
    except Exception:
        for node in (caller, exposer):
            if node:
                await node.close()
        raise


async def test_server_does_not_relay():
    srv = await rembus.server(port=8992)
    caller = exposer = None
    try:
        caller = await _run(srv, 8992)
        with pytest.raises(rp.RembusError):
            await caller.rpc("remote")
    finally:
        if caller:
            await caller.close()
        await srv.close()


async def test_server_rejects_direct():
    srv = await rembus.server(port=8994)
    caller = None
    try:
        caller = await _run(srv, 8994)
        with pytest.raises(rp.RembusError, match="direct not permitted"):
            await caller.direct("exposer", "remote")
    finally:
        if caller:
            await caller.close()
        await srv.close()


async def test_broker_relays():
    srv = await rembus.component(port=8993)
    caller = None
    try:
        caller = await _run(srv, 8993)
        assert await caller.rpc("remote") == "from_exposer"
    finally:
        if caller:
            await caller.close()
        await srv.close()


async def _pubsub(srv, port):
    got_server, got_sub = [], []
    sub = pub = None
    try:
        await srv.subscribe(lambda x: got_server.append(x), topic="news")
        sub = await rembus.component(f"ws://:{port}/sub")
        await sub.subscribe(lambda x: got_sub.append(x), topic="news")
        await sub.reactive()
        pub = await rembus.component(f"ws://:{port}/pub")
        await pub.publish("news", 1)
        await asyncio.sleep(0.5)
    finally:
        for node in (pub, sub):
            if node:
                await node.close()
        await srv.close()
    return got_server, got_sub


async def test_server_does_not_broadcast():
    got_server, got_sub = await _pubsub(await rembus.server(port=8995), 8995)
    assert got_server == [1]
    assert not got_sub


async def test_broker_broadcasts():
    got_server, got_sub = await _pubsub(await rembus.component(port=8996), 8996)
    assert got_server == [1]
    assert got_sub == [1]
