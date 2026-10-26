"""A broker publishes the arguments of a successful RPC to the topic
subscribers, excluding the requestor."""

import asyncio
import pytest
import rembus

RECEIVED = []


async def double(x):
    """RPC handler."""
    return x * 2


async def record(x):
    """Subscriber handler."""
    RECEIVED.append(x)


@pytest.mark.asyncio
async def test_rpc_result_broadcast():
    """Subscribers receive the RPC data; the requestor does not."""
    RECEIVED.clear()
    port = 8106
    server = await rembus.component(port=port)
    exposer = await rembus.component(f"ws://:{port}/exp.net")
    await exposer.expose(double, "double")
    sub = await rembus.component(f"ws://:{port}/sub.net")
    await sub.subscribe(record, topic="double")
    await sub.reactive()
    cli = await rembus.component(f"ws://:{port}/cli.net")
    await cli.subscribe(record, topic="double")
    await cli.reactive()

    assert await cli.rpc("double", 3) == 6
    await asyncio.sleep(0.5)
    assert RECEIVED == [3]

    for c in (cli, sub, exposer, server):
        await c.close()
