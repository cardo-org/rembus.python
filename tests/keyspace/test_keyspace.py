import asyncio
import logging
import pytest
import rembus


def consume(ctx, rb, topic, data):
    logging.info("[%s] recv: %s", topic, data)
    ctx[rb.rid] = data


async def create_sub(name, topic, ctx):
    sub = await rembus.component(name)
    await sub.subscribe(consume, topic=topic)
    sub.inject(ctx)
    await sub.reactive()
    return sub


@pytest.mark.anyio
async def test_add_plugin():
    ctx = {}
    server = await rembus.component(port=8000)

    pub = await rembus.component("pub")

    kspace = rembus.KeySpaceRouter()

    rembus.add_plugin(server, kspace)
    sub1 = await create_sub("sub1", "k/*/y", ctx)
    sub2 = await create_sub("sub2", "k/*/y", ctx)

    await pub.publish("k/e/y", "space")
    await pub.publish("k/@e/y", "space")

    await sub2.unsubscribe("k/*/y")
    await sub1.close()

    await asyncio.sleep(0.1)

    # force a keyspace cleanup because sub1 twin is gone.
    await pub.publish("k/e/y", "space")

    await pub.close()
    await sub2.close()
    await server.close()

    assert len(ctx) == 2
