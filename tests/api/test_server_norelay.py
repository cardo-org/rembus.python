"""Test that a server does not relay RPC requests between components."""

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
