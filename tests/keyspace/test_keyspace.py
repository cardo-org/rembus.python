import asyncio
import logging
import pytest
import rembus


# def consume(ctx, rb, topic, data):
def consume(topic, data, ctx, node):
    logging.info("[%s] test_keyspace recv: %s", topic, data)
    ctx[node.rid] = data


def broker_consume(topic, data):
    logging.info("[%s] broker recv: %s", topic, data)


async def create_sub(name, topic, ctx):
    sub = await rembus.component(name)
    await sub.subscribe(consume, topic=topic)
    sub.inject(ctx)
    await sub.reactive()
    return sub


@pytest.mark.anyio
async def test_add_plugin():
    space_topic = "k/*/y"
    ctx = {}
    server = await rembus.component(port=8000)

    # register a space topic to the server
    await server.subscribe(broker_consume, topic=space_topic)

    pub = await rembus.component("pub")

    sub1 = await create_sub("sub1", space_topic, ctx)
    sub2 = await create_sub("sub2", space_topic, ctx)

    await pub.publish("k/e/y", "space")
    await pub.publish("k/@e/y", "space")

    await sub2.unsubscribe(space_topic)
    await sub1.close()

    await asyncio.sleep(0.1)

    # force a keyspace cleanup because sub1 twin is gone.
    await pub.publish("k/e/y", "space")

    await pub.close()
    await sub2.close()
    await server.close()

    assert len(ctx) == 2


@pytest.mark.anyio
async def test_remove_close_from_keyspace():
    space_topic = "k/*/y"
    ctx = {}
    server = await rembus.component(port=8000)

    pub = await rembus.component("pub")

    sub1 = await create_sub("sub1", space_topic, ctx)
    sub2 = await create_sub("sub2", space_topic, ctx)

    await pub.publish("k/e/y", "space")

    await asyncio.sleep(0.1)
    await sub1.close()

    # sub1 twin is gone, deliver only to sub2.
    await asyncio.sleep(0.1)
    await pub.publish("k/e/y", "space_1999")

    await asyncio.sleep(0.1)
    await pub.close()
    await sub2.close()
    await server.close()

    assert len(ctx) == 2
    assert ctx["sub1"] == "space"
    assert ctx["sub2"] == "space_1999"
